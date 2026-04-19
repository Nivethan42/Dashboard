"""Compute signal state for each trading strategy and write data/latest.json.

All signals follow the same rule:
  * Use QQQ (or SPY) opens, prior data only (t-1 and earlier).
  * Trades execute at today's open in the matching leveraged ETF.
"""

from __future__ import annotations

import json
import math
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd
import yfinance as yf


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUTPUT_FILE = DATA_DIR / "latest.json"
HISTORY_FILE = DATA_DIR / "history.json"

# Enough history for 200-day SMAs plus a few years of EMA warm-up.
LOOKBACK_DAYS = 900


def download_opens(ticker: str) -> pd.Series:
    """Return a Series of daily opens for `ticker`, indexed by date (ascending)."""
    last_err: Exception | None = None
    for attempt in range(4):
        try:
            df = yf.download(
                ticker,
                period=f"{LOOKBACK_DAYS}d",
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            if df is not None and not df.empty:
                opens = df["Open"]
                if isinstance(opens, pd.DataFrame):
                    opens = opens.iloc[:, 0]
                opens = opens.dropna().astype(float)
                opens.name = ticker
                return opens
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Failed to download {ticker}: {last_err}")


# --- Indicator helpers ---------------------------------------------------

def sma(series: pd.Series, n: int, t_offset: int = 1) -> float:
    """SMA over the N opens ending at t-t_offset (inclusive)."""
    window = series.iloc[-t_offset - n + 1 : len(series) - t_offset + 1]
    if len(window) < n:
        return float("nan")
    return float(window.mean())


def sma_series(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(window=n).mean()


def ema_series(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()


def open_ratio(series: pd.Series, lead: int, lag: int) -> float:
    """Open[t-lead] / Open[t-lag] - 1."""
    if len(series) < lag + 1:
        return float("nan")
    return float(series.iloc[-lead] / series.iloc[-lag] - 1)


def realized_vol(series: pd.Series, n: int) -> float:
    """stdev of daily open-to-open returns over the last n returns (t-1..t-n)."""
    returns = series.pct_change().dropna()
    window = returns.iloc[-n:]
    if len(window) < n:
        return float("nan")
    return float(window.std(ddof=1))


# --- Indicator bundles per strategy --------------------------------------

def qqq_indicators(opens: pd.Series) -> dict:
    sma7 = sma_series(opens, 7)
    sma50 = sma_series(opens, 50)
    sma63 = sma_series(opens, 63)
    sma126 = sma_series(opens, 126)
    sma150 = sma_series(opens, 150)
    sma200 = sma_series(opens, 200)
    ema100 = ema_series(opens, 100)

    # Shift by 1 so everything references t-1 (prior data only).
    s7 = sma7.iloc[-1]
    s7_3 = sma7.iloc[-4]
    e100 = ema100.iloc[-1]
    e100_3 = ema100.iloc[-4]

    ind = {
        "asof_open_date": opens.index[-1].strftime("%Y-%m-%d"),
        "last_open": float(opens.iloc[-1]),
        "SSLP7_3": float(s7 / s7_3 - 1) if s7_3 else float("nan"),
        "ESLP100_3": float(e100 / e100_3 - 1) if e100_3 else float("nan"),
        "MOM100": open_ratio(opens, 1, 101),
        "MOM150": open_ratio(opens, 1, 151),
        "MOM180": open_ratio(opens, 1, 181),
        "ROC5": open_ratio(opens, 1, 6),
        "SR50_150": float(sma50.iloc[-1] / sma150.iloc[-1]) if sma150.iloc[-1] else float("nan"),
        "SR63_126": float(sma63.iloc[-1] / sma126.iloc[-1]) if sma126.iloc[-1] else float("nan"),
        "SR150_200": float(sma150.iloc[-1] / sma200.iloc[-1]) if sma200.iloc[-1] else float("nan"),
        "RV5": realized_vol(opens, 5),
        "RV7": realized_vol(opens, 7),
    }
    return ind


def spy_indicators(opens: pd.Series) -> dict:
    sma5 = sma_series(opens, 5)
    sma20 = sma_series(opens, 20)
    sma100 = sma_series(opens, 100)

    returns = opens.pct_change().dropna()
    rv20 = returns.iloc[-20:].std(ddof=1)
    rv100 = returns.iloc[-100:].std(ddof=1)

    ind = {
        "asof_open_date": opens.index[-1].strftime("%Y-%m-%d"),
        "last_open": float(opens.iloc[-1]),
        "MOM90": open_ratio(opens, 1, 91),
        "MOM100": open_ratio(opens, 1, 101),
        "ABVMA100": float(opens.iloc[-1] - sma100.iloc[-1]),  # positive => above
        "ABVMA100_true": bool(opens.iloc[-1] > sma100.iloc[-1]),
        "SLP5_1": float(sma5.iloc[-1] / sma5.iloc[-2] - 1) if sma5.iloc[-2] else float("nan"),
        "SLP20_1": float(sma20.iloc[-1] / sma20.iloc[-2] - 1) if sma20.iloc[-2] else float("nan"),
        "SLP20_3": float(sma20.iloc[-1] / sma20.iloc[-4] - 1) if sma20.iloc[-4] else float("nan"),
        "VR20_100": float(rv20 / rv100) if rv100 else float("nan"),
    }
    return ind


# --- Strategy evaluation -------------------------------------------------

@dataclass
class Condition:
    label: str
    formula: str
    value: float | None
    passed: bool
    group: str = "filter"  # filter | trigger


@dataclass
class StrategyResult:
    id: str
    name: str
    etf: str
    account: str
    source: str
    buy_signal: bool
    conditions: list[Condition] = field(default_factory=list)
    trigger_passed: bool = True
    filters_passed: bool = True
    notes: str = ""


def _cond(label: str, formula: str, value: float | None, passed: bool, group: str = "filter") -> Condition:
    return Condition(label=label, formula=formula, value=value, passed=passed, group=group)


def evaluate_tqqq_tfsa(ind: dict) -> StrategyResult:
    r = StrategyResult(
        id="tqqq_tfsa",
        name="TQQQ · TFSA — Multi-trigger + Quad trend",
        etf="TQQQ",
        account="TFSA",
        source="QQQ",
    )
    triggers = [
        _cond("SSLP7_3 < -0.02", "SMA7 / SMA7[t-3] − 1", ind["SSLP7_3"], ind["SSLP7_3"] < -0.02, "trigger"),
        _cond("ESLP100_3 < -0.006", "EMA100 / EMA100[t-3] − 1", ind["ESLP100_3"], ind["ESLP100_3"] < -0.006, "trigger"),
        _cond("MOM100 > -0.0225", "Open[t-1] / Open[t-101] − 1", ind["MOM100"], ind["MOM100"] > -0.0225, "trigger"),
    ]
    filters = [
        _cond("SR150_200 < 1.07", "SMA150 / SMA200", ind["SR150_200"], ind["SR150_200"] < 1.07),
        _cond("SR63_126 < 1.06", "SMA63 / SMA126", ind["SR63_126"], ind["SR63_126"] < 1.06),
        _cond("MOM180 > -0.12", "Open[t-1] / Open[t-181] − 1", ind["MOM180"], ind["MOM180"] > -0.12),
        _cond("SR50_150 < 1.08", "SMA50 / SMA150", ind["SR50_150"], ind["SR50_150"] < 1.08),
    ]
    r.trigger_passed = any(c.passed for c in triggers)
    r.filters_passed = all(c.passed for c in filters)
    r.buy_signal = r.trigger_passed and r.filters_passed
    r.conditions = triggers + filters
    return r


def evaluate_tqqq_rrsp(ind: dict) -> StrategyResult:
    r = StrategyResult(
        id="tqqq_rrsp",
        name="TQQQ · RRSP — Reset / momentum + volatility",
        etf="TQQQ",
        account="RRSP",
        source="QQQ",
    )
    triggers = [
        _cond("SSLP7_3 < -0.02025", "SMA7 / SMA7[t-3] − 1", ind["SSLP7_3"], ind["SSLP7_3"] < -0.02025, "trigger"),
        _cond("MOM150 > -0.01825", "Open[t-1] / Open[t-151] − 1", ind["MOM150"], ind["MOM150"] > -0.01825, "trigger"),
    ]
    vol_filters = [
        _cond("RV5 < 0.0310", "stdev open-open returns (5d)", ind["RV5"], ind["RV5"] < 0.0310),
        _cond("RV7 < 0.0350", "stdev open-open returns (7d)", ind["RV7"], ind["RV7"] < 0.0350),
    ]
    trend_pair = [
        _cond("SR50_150 < 1.081", "SMA50 / SMA150", ind["SR50_150"], ind["SR50_150"] < 1.081, "filter"),
        _cond("ROC5 < 0.007", "Open[t-1] / Open[t-6] − 1", ind["ROC5"], ind["ROC5"] < 0.007, "filter"),
    ]
    trend_ok = any(c.passed for c in trend_pair)
    r.trigger_passed = any(c.passed for c in triggers)
    r.filters_passed = all(c.passed for c in vol_filters) and trend_ok
    r.buy_signal = r.trigger_passed and r.filters_passed
    r.conditions = triggers + vol_filters + trend_pair
    r.notes = "Trend pair is an OR: at least one of SR50_150 / ROC5 must pass."
    return r


def _spxl_conditions(ind: dict) -> tuple[list[Condition], Condition]:
    score_conds = [
        _cond("MOM90 > 0", "SPY Open[t-1] / Open[t-91] − 1", ind["MOM90"], ind["MOM90"] > 0, "trigger"),
        _cond("MOM100 > 0", "SPY Open[t-1] / Open[t-101] − 1", ind["MOM100"], ind["MOM100"] > 0, "trigger"),
        _cond("ABVMA100", "SPY Open[t-1] > SMA100", ind["ABVMA100"], ind["ABVMA100_true"], "trigger"),
        _cond("SLP5_1 > 0", "(SMA5[t-1] / SMA5[t-2]) − 1", ind["SLP5_1"], ind["SLP5_1"] > 0, "trigger"),
        _cond("SLP20_1 > 0", "(SMA20[t-1] / SMA20[t-2]) − 1", ind["SLP20_1"], ind["SLP20_1"] > 0, "trigger"),
        _cond("SLP20_3 > 0", "(SMA20[t-1] / SMA20[t-4]) − 1", ind["SLP20_3"], ind["SLP20_3"] > 0, "trigger"),
    ]
    vr_cond = _cond("VR20/100 < 1.4", "20d vol / 100d vol", ind["VR20_100"], ind["VR20_100"] < 1.4)
    return score_conds, vr_cond


def evaluate_spxl_tfsa(ind: dict) -> StrategyResult:
    r = StrategyResult(
        id="spxl_tfsa",
        name="SPXL · TFSA — Score ≥ 3 + volatility ratio",
        etf="SPXL",
        account="TFSA",
        source="SPY",
    )
    score_conds, vr_cond = _spxl_conditions(ind)
    score = sum(1 for c in score_conds if c.passed)
    r.trigger_passed = score >= 3
    r.filters_passed = vr_cond.passed
    r.buy_signal = r.trigger_passed and r.filters_passed
    r.conditions = score_conds + [vr_cond]
    r.notes = f"Score = {score}/6. Buy when score ≥ 3 AND VR20/100 < 1.4. Sell when score ≤ 1 OR VR20/100 ≥ 1.4."
    return r


def evaluate_spxl_rrsp(ind: dict) -> StrategyResult:
    # Placeholder: mirrors SPXL-TFSA until the RRSP-specific rule is supplied.
    r = evaluate_spxl_tfsa(ind)
    r.id = "spxl_rrsp"
    r.name = "SPXL · RRSP — Placeholder (matches TFSA)"
    r.account = "RRSP"
    r.notes = (r.notes + " ").strip() + " RRSP rule not yet supplied — using SPXL-TFSA rule as placeholder."
    return r


# --- Serialization -------------------------------------------------------

def _nan_to_none(v):
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def strategy_to_dict(r: StrategyResult) -> dict:
    return {
        "id": r.id,
        "name": r.name,
        "etf": r.etf,
        "account": r.account,
        "source": r.source,
        "buy_signal": r.buy_signal,
        "trigger_passed": r.trigger_passed,
        "filters_passed": r.filters_passed,
        "notes": r.notes,
        "conditions": [
            {
                "label": c.label,
                "formula": c.formula,
                "value": _nan_to_none(c.value),
                "passed": c.passed,
                "group": c.group,
            }
            for c in r.conditions
        ],
    }


def indicators_to_dict(ind: dict) -> dict:
    return {k: _nan_to_none(v) for k, v in ind.items()}


def main() -> int:
    now = datetime.now(timezone.utc)

    try:
        qqq = download_opens("QQQ")
        spy = download_opens("SPY")
    except Exception as e:  # noqa: BLE001
        print(f"[error] download failed: {e}", file=sys.stderr)
        return 1

    qqq_ind = qqq_indicators(qqq)
    spy_ind = spy_indicators(spy)

    strategies = [
        evaluate_tqqq_tfsa(qqq_ind),
        evaluate_tqqq_rrsp(qqq_ind),
        evaluate_spxl_tfsa(spy_ind),
        evaluate_spxl_rrsp(spy_ind),
    ]

    payload = {
        "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "generated_at_display": now.strftime("%Y-%m-%d %H:%M UTC"),
        "sources": {
            "QQQ": indicators_to_dict(qqq_ind),
            "SPY": indicators_to_dict(spy_ind),
        },
        "strategies": [strategy_to_dict(s) for s in strategies],
    }

    DATA_DIR.mkdir(exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(payload, indent=2))

    # Append a compact history entry so we can chart indicator drift later.
    history_entry = {
        "generated_at_utc": payload["generated_at_utc"],
        "qqq_last_open": qqq_ind["last_open"],
        "spy_last_open": spy_ind["last_open"],
        "signals": {s.id: s.buy_signal for s in strategies},
    }
    try:
        history = json.loads(HISTORY_FILE.read_text()) if HISTORY_FILE.exists() else []
    except Exception:
        history = []
    history.append(history_entry)
    history = history[-365:]  # keep last ~year of entries
    HISTORY_FILE.write_text(json.dumps(history, indent=2))

    print(f"[ok] wrote {OUTPUT_FILE.relative_to(ROOT)}")
    for s in strategies:
        flag = "BUY" if s.buy_signal else "FLAT"
        print(f"  {s.id:12} {flag}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
