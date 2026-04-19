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
from urllib.parse import quote as urlquote
from zoneinfo import ZoneInfo

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUTPUT_FILE = DATA_DIR / "latest.json"
HISTORY_FILE = DATA_DIR / "history.json"

# Lookback must cover 200-day warm-up + >= 365 calendar days of backfill.
LOOKBACK_DAYS = 900
BACKFILL_DAYS = 400            # show trades from the last ~1 year (+ buffer)
TICKERS = ["QQQ", "SPY", "TQQQ", "SPXL"]

ET = ZoneInfo("America/New_York")

# Browser-like headers — same idea as UrlFetchApp.fetch in Apps Script.
YAHOO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://finance.yahoo.com/",
    "Origin": "https://finance.yahoo.com",
}


# ─── Yahoo session (cookies + crumb, created once per run) ───────────────────

class _YahooSession:
    """Holds a requests.Session with Yahoo Finance cookies and a crumb token.

    Mirrors the implicit cookie behaviour of UrlFetchApp.fetch in the Apps Script:
    the browser/Google's HTTP client retains session cookies across calls; here
    we do the same with a persistent requests.Session.
    """

    def __init__(self) -> None:
        self.sess = requests.Session()
        self.crumb: str | None = None
        self._init()

    def _init(self) -> None:
        # Step 1 — hit the consent gate to plant the A1/B1 cookies (same origin
        # as a first visit to finance.yahoo.com would set them).
        try:
            self.sess.get(
                "https://fc.yahoo.com", headers=YAHOO_HEADERS,
                timeout=10, allow_redirects=True,
            )
        except Exception as e:
            print(f"  [yahoo] fc.yahoo.com warning: {e}", file=sys.stderr)

        # Step 2 — fetch the crumb that Yahoo requires on chart/v8 requests.
        for host in ("query1", "query2"):
            try:
                r = self.sess.get(
                    f"https://{host}.finance.yahoo.com/v1/test/getcrumb",
                    headers=YAHOO_HEADERS, timeout=10,
                )
                if r.status_code == 200 and r.text.strip():
                    self.crumb = r.text.strip()
                    print(f"  [yahoo] crumb OK via {host}", file=sys.stderr)
                    return
            except Exception as e:
                print(f"  [yahoo] crumb/{host} error: {e}", file=sys.stderr)
        print("  [yahoo] proceeding without crumb", file=sys.stderr)

    def get(self, url: str, **kwargs) -> requests.Response:
        if self.crumb:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}crumb={urlquote(self.crumb)}"
        return self.sess.get(url, headers=YAHOO_HEADERS, **kwargs)


_SESSION: _YahooSession | None = None


def _session() -> _YahooSession:
    global _SESSION
    if _SESSION is None:
        _SESSION = _YahooSession()
    return _SESSION


# ─── JSON parsing ─────────────────────────────────────────────────────────────

def _parse_chart(jo: dict, ticker: str, date_col: str = "open") -> dict[str, float]:
    """Return {date_str: open_price} from a v8/finance/chart JSON payload."""
    result = (jo.get("chart") or {}).get("result") or []
    if not result:
        raise ValueError(f"{ticker}: empty chart result")
    r = result[0]
    timestamps = r.get("timestamp") or []
    quote = ((r.get("indicators") or {}).get("quote") or [{}])[0]
    raw = quote.get(date_col) or []

    records: dict[str, float] = {}
    for ts, v in zip(timestamps, raw):
        if v is None or not math.isfinite(v) or v <= 0:
            continue
        records[datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")] = float(v)
    return records


def _chart_url(ticker: str, range_: str, interval: str, host: str = "query1") -> str:
    return (
        f"https://{host}.finance.yahoo.com/v8/finance/chart/"
        f"{urlquote(ticker)}?range={range_}&interval={interval}"
    )


# ─── Daily opens download (mirrors fetchYahooDaily_) ─────────────────────────

def download_opens(ticker: str) -> pd.Series:
    sess = _session()
    last_err: Exception | None = None

    for attempt, host in enumerate(("query1", "query2", "query1", "query2")):
        url = _chart_url(ticker, f"{LOOKBACK_DAYS}d", "1d", host)
        try:
            resp = sess.get(url, timeout=20)
            if resp.status_code != 200:
                raise ValueError(f"HTTP {resp.status_code}")
            records = _parse_chart(resp.json(), ticker)
            if not records:
                raise ValueError("no valid opens")
            series = pd.Series(records)
            series.index = pd.to_datetime(series.index)
            series = series.sort_index()
            series.name = ticker
            print(f"  [{ticker}] daily OK — {len(series)} bars via {host}", file=sys.stderr)
            return series
        except Exception as e:
            last_err = e
            print(f"  [{ticker}] attempt {attempt + 1} ({host}) failed: {e}", file=sys.stderr)
        time.sleep(2 ** attempt)   # 1s, 2s, 4s, 8s

    raise RuntimeError(f"Failed to download {ticker}: {last_err}")


# ─── Intraday 1m (mirrors fetchYahooIntraday1m_ + getTodayOpenFromYahooIntraday_)

def _fetch_intraday_first_opens(ticker: str) -> dict[str, float]:
    """Return {date_str: first_open_at_or_after_09:30_ET} for the past 7d.

    Mirrors getTodayOpenFromYahooIntraday_ from the Apps Script.
    """
    sess = _session()
    try:
        resp = sess.get(_chart_url(ticker, "7d", "1m"), timeout=20)
        if resp.status_code != 200:
            return {}
        result = (resp.json().get("chart") or {}).get("result") or []
        if not result:
            return {}
        r = result[0]
        timestamps = r.get("timestamp") or []
        quote = ((r.get("indicators") or {}).get("quote") or [{}])[0]
        raw_opens = quote.get("open") or []

        first_open: dict[str, float] = {}
        for ts, o in zip(timestamps, raw_opens):
            if o is None or not math.isfinite(o) or o <= 0:
                continue
            dt_et = datetime.fromtimestamp(ts, tz=ET)
            date_str = dt_et.strftime("%Y-%m-%d")
            hhmm = dt_et.strftime("%H:%M")
            if hhmm < "09:30":
                continue
            if date_str not in first_open:          # keep the 09:30 bar, not later ones
                first_open[date_str] = float(o)
        return first_open
    except Exception as e:
        print(f"  [{ticker}] intraday error: {e}", file=sys.stderr)
        return {}


def patch_today_open(opens: pd.Series, ticker: str) -> pd.Series:
    """Upsert today's 09:30 ET open into the series using intraday 1m data.

    Mirrors upsertTodayOpen_ + repairRecentDailyOpensFromIntraday_ in the
    Apps Script. If Yahoo's daily bar already has today's open, this is a no-op.
    """
    now_et = datetime.now(ET)
    today_str = now_et.strftime("%Y-%m-%d")

    # Only attempt after 09:30 ET on a weekday.
    if now_et.weekday() >= 5 or now_et.strftime("%H:%M") < "09:30":
        return opens

    intra = _fetch_intraday_first_opens(ticker)
    today_open = intra.get(today_str)
    if today_open is None:
        return opens

    today_idx = pd.Timestamp(today_str)
    if today_idx in opens.index:
        # Repair if daily bar has a bad/missing open (same as repairRecentDailyOpensFromIntraday_)
        opens = opens.copy()
        opens.loc[today_idx] = today_open
    else:
        new_row = pd.Series({today_idx: today_open}, name=opens.name)
        opens = pd.concat([opens, new_row]).sort_index()

    print(f"  [{ticker}] today open patched: {today_open}", file=sys.stderr)
    return opens


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

    # Download everything up front, then patch today's open from intraday 1m
    # (mirrors upsertTodayOpen_ + repairRecentDailyOpensFromIntraday_ in Apps Script).
    prices: dict[str, pd.Series] = {}
    for tkr in TICKERS:
        try:
            series = download_opens(tkr)
            series = patch_today_open(series, tkr)
            prices[tkr] = series
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
