"""Compute signal state for each trading strategy and write data/latest.json.

Produces:
  * `data/latest.json`   — current indicator values, per-strategy conditions,
                           and the last ~1 year of realized trades per strategy.
  * `data/history.json`  — daily append of the current buy/flat state per
                           strategy (keeps the last ~400 entries so the UI's
                           streak counter has context across re-runs).

All signals use prior data only (t-1 and earlier). Trades execute at today's
open in the leveraged ETF (TQQQ for QQQ signals, SPXL for SPY signals).
"""

from __future__ import annotations

import json
import math
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUTPUT_FILE = DATA_DIR / "latest.json"
HISTORY_FILE = DATA_DIR / "history.json"

# Lookback must cover 200-day warm-up + >= 365 calendar days of backfill.
LOOKBACK_DAYS = 900
BACKFILL_DAYS = 400            # show trades from the last ~1 year (+ buffer)
TICKERS = ["QQQ", "SPY", "TQQQ", "SPXL"]


# ─── Data download ────────────────────────────────────────────────────────────

def download_opens(ticker: str) -> pd.Series:
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
                opens.index = pd.to_datetime(opens.index).tz_localize(None).normalize()
                return opens
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Failed to download {ticker}: {last_err}")


# ─── Indicator series ─────────────────────────────────────────────────────────

def qqq_indicators(opens: pd.Series) -> pd.DataFrame:
    """Build a DataFrame where row `t` holds the values used for day-t signals.

    Every indicator is shifted so that day t only sees data through t-1.
    """
    sma = {n: opens.rolling(n).mean() for n in (7, 50, 63, 126, 150, 200)}
    ema100 = opens.ewm(span=100, adjust=False).mean()
    ret = opens.pct_change()

    df = pd.DataFrame(index=opens.index)
    df["open"]       = opens
    df["SSLP7_3"]    = sma[7].shift(1)   / sma[7].shift(4)  - 1
    df["ESLP100_3"]  = ema100.shift(1)   / ema100.shift(4)  - 1
    df["MOM100"]     = opens.shift(1)    / opens.shift(101) - 1
    df["MOM150"]     = opens.shift(1)    / opens.shift(151) - 1
    df["MOM180"]     = opens.shift(1)    / opens.shift(181) - 1
    df["ROC5"]       = opens.shift(1)    / opens.shift(6)   - 1
    df["RV5"]        = ret.rolling(5).std(ddof=1).shift(1)
    df["RV7"]        = ret.rolling(7).std(ddof=1).shift(1)
    df["SR50_150"]   = sma[50].shift(1)  / sma[150].shift(1)
    df["SR63_126"]   = sma[63].shift(1)  / sma[126].shift(1)
    df["SR150_200"] = sma[150].shift(1) / sma[200].shift(1)
    return df


def spy_indicators(opens: pd.Series) -> pd.DataFrame:
    sma = {n: opens.rolling(n).mean() for n in (5, 20, 100)}
    ret = opens.pct_change()

    df = pd.DataFrame(index=opens.index)
    df["open"]         = opens
    df["MOM90"]        = opens.shift(1) / opens.shift(91)  - 1
    df["MOM100"]       = opens.shift(1) / opens.shift(101) - 1
    df["ABVMA100"]     = opens.shift(1) - sma[100].shift(1)
    df["ABVMA100_true"]= (opens.shift(1) > sma[100].shift(1)).astype(bool)
    df["SLP5_1"]       = sma[5].shift(1)  / sma[5].shift(2)  - 1
    df["SLP20_1"]      = sma[20].shift(1) / sma[20].shift(2) - 1
    df["SLP20_3"]      = sma[20].shift(1) / sma[20].shift(4) - 1
    df["VR20_100"]     = ret.rolling(20).std(ddof=1).shift(1) / ret.rolling(100).std(ddof=1).shift(1)
    return df


# ─── Strategy rule definitions ────────────────────────────────────────────────

# Each rule: (label, formula, value-column, pass-predicate on the column value,
#             group in {"trigger", "filter"})
# Strategy evaluates trigger_ok = (#triggers passing >= trigger_threshold) and
# filters_ok = all filters passing. BUY = trigger_ok & filters_ok.

def strat_tqqq_tfsa():
    return {
        "id": "tqqq_tfsa",
        "name": "TQQQ · TFSA — Multi-trigger + Quad trend",
        "etf": "TQQQ",
        "account": "TFSA",
        "source": "QQQ",
        "trigger_threshold": 1,
        "rules": [
            ("SSLP7_3 < -0.02",   "SMA7 / SMA7[t-3] − 1",   "SSLP7_3",  lambda v: v < -0.02,    "trigger"),
            ("ESLP100_3 < -0.006","EMA100 / EMA100[t-3] − 1","ESLP100_3",lambda v: v < -0.006,  "trigger"),
            ("MOM100 > -0.0225",  "Open[t-1] / Open[t-101] − 1","MOM100",lambda v: v > -0.0225, "trigger"),
            ("SR150_200 < 1.07",  "SMA150 / SMA200",        "SR150_200",lambda v: v < 1.07,    "filter"),
            ("SR63_126 < 1.06",   "SMA63 / SMA126",         "SR63_126", lambda v: v < 1.06,    "filter"),
            ("MOM180 > -0.12",    "Open[t-1] / Open[t-181] − 1","MOM180",lambda v: v > -0.12,   "filter"),
            ("SR50_150 < 1.08",   "SMA50 / SMA150",         "SR50_150", lambda v: v < 1.08,    "filter"),
        ],
        "notes": "",
    }


def strat_tqqq_rrsp():
    return {
        "id": "tqqq_rrsp",
        "name": "TQQQ · RRSP — Reset / momentum + volatility",
        "etf": "TQQQ",
        "account": "RRSP",
        "source": "QQQ",
        "trigger_threshold": 1,
        "rules": [
            ("SSLP7_3 < -0.02025", "SMA7 / SMA7[t-3] − 1",   "SSLP7_3",  lambda v: v < -0.02025, "trigger"),
            ("MOM150 > -0.01825",  "Open[t-1] / Open[t-151] − 1","MOM150",lambda v: v > -0.01825,"trigger"),
            ("RV5 < 0.0310",       "stdev open-open (5d)",    "RV5",      lambda v: v < 0.0310,  "filter"),
            ("RV7 < 0.0350",       "stdev open-open (7d)",    "RV7",      lambda v: v < 0.0350,  "filter"),
            # Trend-pair is OR; handled specially in evaluate_strategy.
            ("SR50_150 < 1.081",   "SMA50 / SMA150",          "SR50_150", lambda v: v < 1.081,   "filter_or"),
            ("ROC5 < 0.007",       "Open[t-1] / Open[t-6] − 1","ROC5",    lambda v: v < 0.007,   "filter_or"),
        ],
        "notes": "Last two filters are an OR pair (at least one must pass).",
    }


def _spxl_rules():
    return [
        ("MOM90 > 0",   "SPY Open[t-1] / Open[t-91] − 1",  "MOM90",        lambda v: v > 0,   "trigger"),
        ("MOM100 > 0",  "SPY Open[t-1] / Open[t-101] − 1", "MOM100",       lambda v: v > 0,   "trigger"),
        ("ABVMA100",    "SPY Open[t-1] > SMA100",          "ABVMA100_true",lambda v: bool(v),"trigger"),
        ("SLP5_1 > 0",  "(SMA5[t-1] / SMA5[t-2]) − 1",     "SLP5_1",       lambda v: v > 0,   "trigger"),
        ("SLP20_1 > 0", "(SMA20[t-1] / SMA20[t-2]) − 1",   "SLP20_1",      lambda v: v > 0,   "trigger"),
        ("SLP20_3 > 0", "(SMA20[t-1] / SMA20[t-4]) − 1",   "SLP20_3",      lambda v: v > 0,   "trigger"),
        ("VR20/100 < 1.4","20d vol / 100d vol",            "VR20_100",     lambda v: v < 1.4, "filter"),
    ]


def strat_spxl_tfsa():
    return {
        "id": "spxl_tfsa",
        "name": "SPXL · TFSA — Score ≥ 3 + volatility ratio",
        "etf": "SPXL",
        "account": "TFSA",
        "source": "SPY",
        "trigger_threshold": 3,
        "rules": _spxl_rules(),
        "notes": "Buy when trigger score ≥ 3/6 AND VR20/100 < 1.4. Sell when score ≤ 1 OR VR20/100 ≥ 1.4.",
    }


def strat_spxl_rrsp():
    s = strat_spxl_tfsa()
    s["id"] = "spxl_rrsp"
    s["name"] = "SPXL · RRSP — Placeholder (matches TFSA)"
    s["account"] = "RRSP"
    s["notes"] = (s["notes"] + " ").strip() + " RRSP rule not yet supplied — using SPXL-TFSA rule as placeholder."
    return s


STRATEGIES = [strat_tqqq_tfsa, strat_tqqq_rrsp, strat_spxl_tfsa, strat_spxl_rrsp]


# ─── Evaluation ───────────────────────────────────────────────────────────────

def evaluate_strategy(spec: dict, ind_df: pd.DataFrame) -> tuple[pd.Series, pd.DataFrame]:
    """Evaluate a strategy across every day in ind_df.

    Returns:
      buy_series: bool series indexed by date.
      condition_df: per-rule pass/fail columns, with *_val columns for values.
    """
    rules = spec["rules"]
    trigger_threshold = spec["trigger_threshold"]

    cond_cols = {}
    val_cols  = {}
    for label, _formula, col, pred, _group in rules:
        values = ind_df[col]
        val_cols[label] = values
        if col == "ABVMA100_true":
            cond_cols[label] = values.fillna(False).astype(bool)
        else:
            cond_cols[label] = values.apply(lambda v: False if pd.isna(v) else bool(pred(v)))

    cond_df = pd.DataFrame(cond_cols)
    val_df  = pd.DataFrame(val_cols)

    triggers = [lbl for (lbl, _f, _c, _p, g) in rules if g == "trigger"]
    filters_and = [lbl for (lbl, _f, _c, _p, g) in rules if g == "filter"]
    filters_or  = [lbl for (lbl, _f, _c, _p, g) in rules if g == "filter_or"]

    trig_count  = cond_df[triggers].sum(axis=1) if triggers else pd.Series(0, index=cond_df.index)
    trig_ok     = trig_count >= trigger_threshold

    filt_and_ok = cond_df[filters_and].all(axis=1) if filters_and else pd.Series(True, index=cond_df.index)
    filt_or_ok  = cond_df[filters_or].any(axis=1)  if filters_or  else pd.Series(True, index=cond_df.index)

    buy = trig_ok & filt_and_ok & filt_or_ok
    return buy.fillna(False), cond_df, val_df


# ─── Trades ───────────────────────────────────────────────────────────────────

def signals_to_trades(buy: pd.Series, etf_opens: pd.Series) -> list[dict]:
    """Convert a bool buy series into a list of trade records.

    Rule: if signal flips False→True on day t, enter at etf_opens[t]. If it
    flips True→False, exit at etf_opens[t]. An open position at the end of
    the series is included with exit_* null.
    """
    trades = []
    in_pos = False
    entry_date = None
    entry_price = None

    buy = buy.dropna()
    prev = False
    for date, is_buy in buy.items():
        price = etf_opens.get(date)
        if (not prev) and is_buy:
            entry_date = date
            entry_price = float(price) if price is not None and not math.isnan(price) else None
            in_pos = True
        elif prev and (not is_buy) and in_pos:
            exit_price = float(price) if price is not None and not math.isnan(price) else None
            ret = None
            if entry_price and exit_price:
                ret = exit_price / entry_price - 1
            trades.append({
                "entry_date":  entry_date.strftime("%Y-%m-%d") if entry_date is not None else None,
                "entry_price": entry_price,
                "exit_date":   date.strftime("%Y-%m-%d"),
                "exit_price":  exit_price,
                "return_pct":  ret,
                "bars":        int((date - entry_date).days) if entry_date is not None else None,
                "open":        False,
            })
            in_pos = False
            entry_date = None
            entry_price = None
        prev = bool(is_buy)

    if in_pos and entry_date is not None:
        trades.append({
            "entry_date":  entry_date.strftime("%Y-%m-%d"),
            "entry_price": entry_price,
            "exit_date":   None,
            "exit_price":  None,
            "return_pct":  None,
            "bars":        None,
            "open":        True,
        })
    return trades


def trades_summary(trades: list[dict]) -> dict:
    closed = [t for t in trades if not t.get("open")]
    wins = [t for t in closed if (t.get("return_pct") or 0) > 0]
    losses = [t for t in closed if (t.get("return_pct") or 0) <= 0]
    total_return = 1.0
    for t in closed:
        if t.get("return_pct") is not None:
            total_return *= (1 + t["return_pct"])
    return {
        "trades_closed": len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": (len(wins) / len(closed)) if closed else None,
        "compounded_return_pct": (total_return - 1) if closed else None,
        "best_trade_pct": max((t["return_pct"] for t in closed if t.get("return_pct") is not None), default=None),
        "worst_trade_pct": min((t["return_pct"] for t in closed if t.get("return_pct") is not None), default=None),
    }


# ─── Serialization helpers ────────────────────────────────────────────────────

def _nan_to_none(v):
    if v is None: return None
    if isinstance(v, float) and math.isnan(v): return None
    if isinstance(v, (pd.Timestamp,)): return v.strftime("%Y-%m-%d")
    return v


def latest_conditions(spec: dict, ind_row: pd.Series, cond_row: pd.Series, val_row: pd.Series) -> list[dict]:
    out = []
    for label, formula, col, _pred, group in spec["rules"]:
        # Normalize trend_pair group label back to "filter" for the UI.
        ui_group = "filter" if group == "filter_or" else group
        val = val_row.get(label)
        passed = bool(cond_row.get(label, False))
        out.append({
            "label":   label,
            "formula": formula,
            "value":   _nan_to_none(float(val) if val is not None and not (isinstance(val, float) and math.isnan(val)) else None),
            "passed":  passed,
            "group":   ui_group,
        })
    return out


def indicator_snapshot(df: pd.DataFrame) -> dict:
    last = df.iloc[-1]
    snap = {"asof_open_date": df.index[-1].strftime("%Y-%m-%d"), "last_open": _nan_to_none(float(last["open"]))}
    for col in df.columns:
        if col == "open": continue
        v = last[col]
        if isinstance(v, (bool,)):
            snap[col] = bool(v)
        else:
            try:
                snap[col] = _nan_to_none(float(v))
            except (TypeError, ValueError):
                snap[col] = _nan_to_none(v)
    return snap


def main() -> int:
    now = datetime.now(timezone.utc)

    # Download everything up front.
    prices: dict[str, pd.Series] = {}
    for tkr in TICKERS:
        try:
            prices[tkr] = download_opens(tkr)
        except Exception as e:  # noqa: BLE001
            print(f"[error] {tkr}: {e}", file=sys.stderr)
            return 1

    # Indicator frames for QQQ-sourced and SPY-sourced strategies.
    qqq_df = qqq_indicators(prices["QQQ"])
    spy_df = spy_indicators(prices["SPY"])

    # Slice to a reasonable window so history.json and trades don't grow forever.
    cutoff = qqq_df.index[-1] - pd.Timedelta(days=BACKFILL_DAYS)

    strategies_out = []
    history_entries = []
    trades_by_id: dict[str, dict] = {}

    for build in STRATEGIES:
        spec = build()
        ind_df = qqq_df if spec["source"] == "QQQ" else spy_df
        etf_series = prices[spec["etf"]]

        buy, cond_df, val_df = evaluate_strategy(spec, ind_df)
        buy_window = buy.loc[buy.index >= cutoff]

        trades = signals_to_trades(buy_window, etf_series)
        summary = trades_summary(trades)

        latest_conds = latest_conditions(spec, ind_df.iloc[-1], cond_df.iloc[-1], val_df.iloc[-1])
        triggers = [c for c in latest_conds if c["group"] == "trigger"]
        trigger_passed = sum(1 for c in triggers if c["passed"]) >= spec["trigger_threshold"]
        filters_passed = all(c["passed"] for c in latest_conds if c["group"] == "filter")
        buy_today = bool(buy.iloc[-1])

        strategies_out.append({
            "id":               spec["id"],
            "name":             spec["name"],
            "etf":              spec["etf"],
            "account":          spec["account"],
            "source":           spec["source"],
            "buy_signal":       buy_today,
            "trigger_passed":   trigger_passed,
            "filters_passed":   filters_passed,
            "notes":            spec["notes"],
            "conditions":       latest_conds,
            "trades":           trades,
            "trades_summary":   summary,
        })

    # ── Append compact history entry ──
    history_entry = {
        "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "qqq_last_open": _nan_to_none(float(prices["QQQ"].iloc[-1])),
        "spy_last_open": _nan_to_none(float(prices["SPY"].iloc[-1])),
        "signals": {s["id"]: s["buy_signal"] for s in strategies_out},
    }
    try:
        history = json.loads(HISTORY_FILE.read_text()) if HISTORY_FILE.exists() else []
    except Exception:
        history = []
    if not isinstance(history, list):
        history = []
    history.append(history_entry)
    history = history[-500:]

    payload = {
        "generated_at_utc":     now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "generated_at_display": now.strftime("%Y-%m-%d %H:%M UTC"),
        "backfill_days":        BACKFILL_DAYS,
        "sources": {
            "QQQ": indicator_snapshot(qqq_df),
            "SPY": indicator_snapshot(spy_df),
        },
        "strategies": strategies_out,
    }

    DATA_DIR.mkdir(exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(payload, indent=2))
    HISTORY_FILE.write_text(json.dumps(history, indent=2))

    print(f"[ok] wrote {OUTPUT_FILE.relative_to(ROOT)}")
    for s in strategies_out:
        flag = "BUY " if s["buy_signal"] else "FLAT"
        n = s["trades_summary"]["trades_closed"]
        print(f"  {s['id']:12}  {flag}  trades_closed={n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
