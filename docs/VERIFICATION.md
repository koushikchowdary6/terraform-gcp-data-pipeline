# Verification record

Captured on 2026-10-13 on Linux x86_64, Python 3.13.16, DuckDB 1.5.6.

Everything below is output from commands that actually ran. Where a check could
not run in this environment, it says so explicitly rather than being omitted.

## Summary

| Check | Tool | Result |
|---|---|---|
| Python unit + integration tests | pytest 9.1.1 | **114 passed** |
| Python lint | ruff 0.17.0 | **clean** |
| Python formatting | ruff format | **clean** (22 files) |
| Terraform formatting | OpenTofu 1.8.5 `fmt -check` | **clean** |
| Terraform lint | tflint 0.53.0 | **no findings** |
| Terraform security scan | checkov 3.3.29 | **12 passed, 0 failed, 4 accepted** |
| Local pipeline, real data | DuckDB | **53,513 observations loaded** |
| Idempotency, incremental rerun | DuckDB | **0 rows inserted, count unchanged** |
| Idempotency, full re-process | DuckDB | **0 rows inserted, count unchanged** |
| `terraform validate` | Terraform CLI | **not run locally** — see below |
| GCP deployment | — | **not performed** — no credentials, no approval |
| BigQuery SQL execution | — | **not executed** — requires a GCP project |

### Why `terraform validate` did not run locally

`terraform validate` needs the `hashicorp/google` provider schema, which means
`terraform init` must download the provider. In this environment both
`releases.hashicorp.com` and `registry.opentofu.org` are blocked by an outbound
proxy, so the provider could not be fetched:

```
Error: Failed to resolve provider packages
Could not resolve provider hashicorp/google: could not connect to
registry.opentofu.org: failed to request discovery document: Forbidden
```

What *was* verified locally, and what it does and does not prove:

- `tofu fmt -check -recursive` passed. `fmt` parses every `.tf` file, so this
  confirms the HCL is **syntactically valid** and canonically formatted.
- `tflint` passed with its bundled Terraform ruleset, which checks structure,
  deprecated syntax and unused declarations.
- `checkov` parsed the configuration and evaluated 16 GCP security policies.

None of that proves the resource arguments match the provider's schema. A typo
in an attribute name would still be caught only by `terraform validate`, which
runs in the `terraform` GitHub Actions workflow. **Check that workflow's status
before treating the configuration as schema-valid.**

OpenTofu was used because it is a drop-in fork of Terraform that reads the same
HCL; the Terraform binary itself could not be downloaded here.

## Test suite

```
$ python -m pytest
........................................................................ [ 63%]
..........................................                               [100%]
114 passed in 2.30s
```

Coverage by area:

| File | Focus |
|---|---|
| `test_schema.py` | deterministic keys (incl. a pinned digest), date and numeric parsing, range and sentinel rules, header validation |
| `test_ingest.py` | clean ingestion, malformed cells with reasons, duplicates, empty and header-only files, missing columns, Parquet round-trip |
| `test_incremental.py` | watermark persistence, monotonic advance, corrupt-state handling, lookback window, floor filtering |
| `test_warehouse.py` | staging load, insert/update/no-op upsert paths, revision handling, grain uniqueness, monthly model and window functions |
| `test_quality.py` | all checks pass on clean data; each defect is detected individually; enforcement and reporting |
| `test_cli.py` | end-to-end CLI runs, exit codes, watermark advance, idempotent reruns, config guard before any cloud call |

## Lint and static analysis

```
$ python -m ruff check .
All checks passed!

$ python -m ruff format --check .
22 files already formatted

$ tofu fmt -check -recursive -diff terraform/     # OpenTofu 1.8.5
(no output, exit 0)

$ tflint --chdir=terraform                        # v0.53.0
(no output, exit 0)

$ checkov --directory terraform --framework terraform --compact --quiet
terraform scan results:
Passed checks: 12, Failed checks: 0, Skipped checks: 4
```

The 4 skips are accepted findings, each carrying an inline
`checkov:skip=<id>:<reason>` comment in `terraform/main.tf`:

| Check | Resource | Why it is accepted |
|---|---|---|
| `CKV_GCP_62` | `google_storage_bucket.raw` | Access logging needs a second bucket and bills per log object. The data is public and writes are attributable via `batch_id` in the object path. |
| `CKV_GCP_81` | `google_bigquery_dataset.analytics` | Data is already encrypted at rest with Google-managed keys. CMEK adds a KMS key to provision, rotate and pay for, unjustified for public market data. |
| `CKV_GCP_80` | both BigQuery tables | Same reasoning as above. |

Before the skips were added, these were genuine findings reported as 4 failures.
They are recorded rather than silently suppressed so a reviewer can disagree.

## Local pipeline run on real data

First load of both feeds (1,869 monthly S&P rows + 9,278 daily VIX rows):

```
=== Run summary ===
staged rows           : 53513
fact rows before/after: 0 / 53513
rows inserted         : 53513
monthly mart rows     : 18165
  sp500_monthly    read=1869   observations=16401  rejects=0  missing=420  dup_dropped=0  skipped_by_watermark=0     max_date=2026-09-01
  vix_daily        read=9278   observations=37112  rejects=0  missing=0    dup_dropped=0  skipped_by_watermark=0     max_date=2026-09-22

=== Watermarks advanced ===
  sp500_monthly    -> 2026-09-01
  vix_daily        -> 2026-09-22
```

All eight data-quality checks passed:

```
CHECK                             STATUS  OBSERVED  DETAIL
all_staged_rows_loaded            PASS           0  staged observation_keys missing from the fact table
business_grain_is_unique          PASS           0  (series_id, metric_name, observation_date) combinations appearing more than once
fact_table_not_empty              PASS       53513  rows in analytics.fct_market_observation
no_future_observation_dates       PASS           0  rows dated after today
no_null_metric_values             PASS           0  rows with a NULL metric_value
observation_key_is_unique         PASS           0  observation_key values appearing more than once
staged_values_match_fact          PASS           0  staged keys whose fact value differs from the staged value
vix_close_within_plausible_range  PASS           0  CBOE_VIX close values outside (0, 500]
```

Coverage, which shows the sentinel-zero rule working — the index level runs to
2026-09 while the fundamentals stop in 2023, where the upstream feed stops
reporting them:

```
  CBOE_VIX         close                n=9278   1990-01-02 -> 2026-09-22
  CBOE_VIX         high                 n=9278   1990-01-02 -> 2026-09-22
  CBOE_VIX         low                  n=9278   1990-01-02 -> 2026-09-22
  CBOE_VIX         open                 n=9278   1990-01-02 -> 2026-09-22
  SP500_COMPOSITE  cpi                  n=1833   1871-01-01 -> 2023-09-01
  SP500_COMPOSITE  dividend             n=1830   1871-01-01 -> 2023-06-01
  SP500_COMPOSITE  earnings             n=1830   1871-01-01 -> 2023-06-01
  SP500_COMPOSITE  index_level          n=1869   1871-01-01 -> 2026-09-01
  SP500_COMPOSITE  long_interest_rate   n=1833   1871-01-01 -> 2023-09-01
  SP500_COMPOSITE  pe10                 n=1713   1881-01-01 -> 2023-09-01
  SP500_COMPOSITE  real_dividend        n=1830   1871-01-01 -> 2023-06-01
  SP500_COMPOSITE  real_earnings        n=1830   1871-01-01 -> 2023-06-01
  SP500_COMPOSITE  real_price           n=1833   1871-01-01 -> 2023-09-01
```

## Idempotency

### Incremental rerun

Second run, watermarks in place, 7-day lookback. Only the trailing window is
re-read, and nothing is inserted:

```
=== Run summary ===
staged rows           : 25
fact rows before/after: 53513 / 53513
rows inserted         : 0
  sp500_monthly    read=1869  observations=1   skipped_by_watermark=1868  max_date=2026-09-01
  vix_daily        read=9278  observations=24  skipped_by_watermark=9272  max_date=2026-09-22
```

### Full re-process

The stricter test. All 53,513 observations are re-staged and merged again:

```
=== Run summary ===
staged rows           : 53513
fact rows before/after: 53513 / 53513
rows inserted         : 0
```

And a fingerprint over `(observation_key, metric_value, updated_at)` for every
row is byte-identical before and after that run:

```
BEFORE a full-refresh run:  fact rows: 53513  fingerprint: 23e48953bad3e024f43c4c921d69a3c4cd2b06a1
AFTER  a full-refresh run:  fact rows: 53513  fingerprint: 23e48953bad3e024f43c4c921d69a3c4cd2b06a1
```

`updated_at` being unchanged matters: it means unchanged rows were not rewritten
at all, which is stricter than "no duplicates appeared".

### Revisions are still applied

Idempotency must not mean "ignores changes". With the warehouse value tampered
with to simulate a stale row, re-running the upsert corrects it in place without
adding a row:

```
value before tampering: 14.210000
tampered to           : 999.000000
after re-upsert       : 14.210000     <- corrected in place
fact row count        : 53513          <- unchanged
```

## Not verified

1. **No GCP resource was created.** No `terraform apply`, no credentials
   configured, no billing account attached, no approval sought or given.
2. **The BigQuery SQL has never executed.** `sql/bigquery/*.sql` is written
   against documented BigQuery semantics and mirrors the DuckDB statements that
   do run, but MERGE behaviour, partition pruning and the analytics model are
   unproven against the real engine.
3. **`terraform validate` and `terraform plan` have not run** against the
   provider schema locally — see the explanation above. CI runs `validate`.
4. **GitHub Actions status is unknown at the time of writing.** The workflows
   are committed but had not yet run.
5. **Scale is untested.** The largest input here is ~9,000 rows. Nothing
   establishes behaviour at millions of rows; see the README's limitations.
