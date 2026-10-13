# Datasets

Both feeds are real, public, openly licensed CSV files. Nothing in this
repository contains personal, confidential or restricted data.

## 1. S&P 500 composite, monthly

| | |
|---|---|
| Source | <https://github.com/datasets/s-and-p-500> |
| File | `https://raw.githubusercontent.com/datasets/s-and-p-500/main/data/data.csv` |
| Upstream origin | Robert Shiller's long-run US stock market dataset (Yale) |
| Licence | Open Data Commons Public Domain Dedication and Licence (PDDL) 1.0 |
| Grain | one row per month, `1871-01` onward |
| `series_id` | `SP500_COMPOSITE` |

Columns consumed: `Date`, `SP500`, `Dividend`, `Earnings`,
`Consumer Price Index`, `Long Interest Rate`, `Real Price`, `Real Dividend`,
`Real Earnings`, `PE10`.

## 2. CBOE Volatility Index (VIX), daily

| | |
|---|---|
| Source | <https://github.com/datasets/finance-vix> |
| File | `https://raw.githubusercontent.com/datasets/finance-vix/main/data/vix-daily.csv` |
| Upstream origin | Cboe Global Markets published VIX history |
| Licence | Open Data Commons Public Domain Dedication and Licence (PDDL) 1.0 |
| Grain | one row per trading day, `1990-01-02` onward |
| `series_id` | `CBOE_VIX` |

Columns consumed: `DATE`, `OPEN`, `HIGH`, `LOW`, `CLOSE`.

## Why these two

They exercise the problems a data pipeline has to solve, rather than just
being large:

- **Different shapes.** One is monthly with nine measures, the other daily with
  four. Both are normalized into a single long-format fact table, which is the
  reason `pipeline/sources.py` is a declarative spec instead of two scripts.
- **Different date granularity.** `YYYY-MM` is normalized to the first of the
  month so one `DATE` partition column serves both.
- **Genuine upstream messiness.** The S&P feed pads not-yet-reported
  fundamentals with `0.0` rather than leaving them blank. See below.
- **A natural incremental key.** Both are append-mostly time series with
  occasional revision of recent values, which is exactly the case that makes a
  deterministic key and an upsert necessary.

## The `0.0` problem

In the S&P feed, recent months carry `0.0` for columns that have not been
reported yet:

```
2026-08-01,7711.32,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
2026-09-01,7669.41,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
```

A 0.0 Consumer Price Index or a 0.00% long-term interest rate is not an
observation; it is a placeholder. Loading it would corrupt every average and
every month-over-month change that touched those rows.

Each source therefore declares `zero_means_missing` (see
`pipeline/sources.py`), and `parse_metric_value` treats a sentinel zero as
missing rather than as data. The effect is visible in the coverage report: the
index level runs to 2026-09, while CPI and the fundamentals stop at 2023,
which is where the upstream feed genuinely stops reporting them.

This is a judgement call, and it is deliberately declared per source and per
metric rather than applied globally — a zero *is* meaningful for some measures,
so suppressing every zero everywhere would be the wrong fix.

## Acquiring the data

```bash
make fetch            # or: python -m pipeline.cli fetch
```

Files land in `data/raw/`, which is gitignored: the data is reproducible from
the URLs above and does not belong in version control.

If the machine has no outbound network access, download both CSVs by hand, save
them as `data/raw/sp500_monthly.csv` and `data/raw/vix_daily.csv`, and use
`--offline`:

```bash
python -m pipeline.cli run-local --offline
```

Small committed extracts of the VIX feed live in `tests/fixtures/` so the test
suite runs with no network at all.
