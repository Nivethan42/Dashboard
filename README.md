# Leveraged ETF Strategy Dashboard

A static GitHub Pages dashboard that shows whether each of your four
leveraged-ETF strategies is in **BUY** or **FLAT** state right now, based on
QQQ / SPY opens pulled from Yahoo Finance every trading morning.

Strategies covered:

| # | Account | ETF  | Signal source | Summary |
|---|---------|------|---------------|---------|
| 1 | TFSA    | TQQQ | QQQ | `(SSLP7_3 < -0.02 OR ESLP100_3 < -0.006 OR MOM100 > -0.0225)` AND quad-trend filters |
| 2 | RRSP    | TQQQ | QQQ | `(SSLP7_3 < -0.02025 OR MOM150 > -0.01825)` AND RV5/RV7 AND `(SR50_150 < 1.081 OR ROC5 < 0.007)` |
| 3 | TFSA    | SPXL | SPY | Score ≥ 3 of 6 trend conditions AND `VR20/100 < 1.4` |
| 4 | RRSP    | SPXL | SPY | *Placeholder — edit `scripts/compute_signals.py:evaluate_spxl_rrsp` with the real RRSP rule.* |

All signals use prior data only (t-1 and earlier). Trades execute at today's
open in the leveraged ETF.

## Repository layout

```
index.html                 # dashboard page
assets/css/style.css       # responsive styling (light + dark)
assets/js/main.js          # renders data/latest.json into the page
data/latest.json           # current signal state (committed by the workflow)
data/history.json          # rolling ~1 year of run entries
scripts/compute_signals.py # computes every indicator + strategy
requirements.txt           # Python deps (yfinance, pandas)
.github/workflows/
  update-data.yml          # cron job: pulls yfinance, commits JSON
  deploy-pages.yml         # deploys the site on every push to main
```

## One-time setup

1. **Push to GitHub** (to `main`).
2. **Enable GitHub Pages**
   - Repo *Settings → Pages → Build and deployment → Source: `GitHub Actions`*.
3. **Allow Actions to write**
   - Repo *Settings → Actions → General → Workflow permissions*:
     set to *Read and write permissions*.
4. **Kick off the first run**
   - Repo *Actions → "Update dashboard data" → Run workflow*.
   - It will commit `data/latest.json`, which triggers the Pages deploy.

## How the data updates

`update-data.yml` runs on cron **9:30, 9:35, 9:40, 9:45 ET** on weekdays, and
also supports manual `workflow_dispatch`.

Because cron runs in UTC and ignores daylight saving time, the workflow is
scheduled in both EDT and EST windows (13:3x and 14:3x UTC). The Python script
is idempotent — if the data didn't change, no commit is made.

## Manual refresh

Two ways:

1. **Dashboard page** — click the **Refresh** button. This reloads
   `data/latest.json` without CDN caching. Useful after the workflow has just
   committed new data.
2. **Re-run the pipeline** — *Actions → Update dashboard data → Run workflow*.
   This re-pulls from Yahoo Finance.

## Running the script locally

```bash
pip install -r requirements.txt
python scripts/compute_signals.py
```

This writes `data/latest.json` and appends to `data/history.json`.

## Adjusting the SPXL-RRSP rule

`scripts/compute_signals.py` has an `evaluate_spxl_rrsp` function that
currently mirrors the TFSA rule. Replace it with the real RRSP conditions
(e.g. different score threshold, different VR cutoff) and the dashboard will
pick it up on the next run.

## Not financial advice

Signals shown here are generated mechanically from price data. Verify every
decision against your own plan.
