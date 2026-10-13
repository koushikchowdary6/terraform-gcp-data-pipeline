-- Idempotent upsert from staging into the analytics fact table.
--
-- Grain: one row per (series_id, metric_name, observation_date), identified by
-- the deterministic observation_key computed during ingestion.
--
-- Three properties this statement guarantees:
--   1. New observations are inserted.
--   2. Revised observations (same key, different value) are updated in place.
--   3. Unchanged observations are not touched at all - the anti-join filter
--      excludes them, so `updated_at` stays stable across no-op reruns. That
--      is what makes a repeated run genuinely idempotent rather than merely
--      "not duplicating".
--
-- DuckDB's INSERT ... ON CONFLICT is the local equivalent of the BigQuery
-- MERGE in sql/bigquery/02_merge.sql. Same semantics, different dialect.

CREATE TABLE IF NOT EXISTS analytics.fct_market_observation (
      observation_key   VARCHAR       NOT NULL PRIMARY KEY
    , series_id         VARCHAR       NOT NULL
    , metric_name       VARCHAR       NOT NULL
    , observation_date  DATE          NOT NULL
    , metric_value      DECIMAL(20,6) NOT NULL
    , source_name       VARCHAR       NOT NULL
    , batch_id          VARCHAR       NOT NULL
    , ingested_at       TIMESTAMP WITH TIME ZONE NOT NULL
    , updated_at        TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Collapse any same-key rows inside the batch before touching the target:
-- the highest source_row_number wins, matching the "last occurrence wins"
-- rule applied during ingestion.
WITH ranked AS (
    SELECT
          *
        , ROW_NUMBER() OVER (
              PARTITION BY observation_key
              ORDER BY source_row_number DESC, ingested_at DESC
          ) AS row_rank
    FROM staging.market_observation
)
, batch AS (
    SELECT * FROM ranked WHERE row_rank = 1
)
INSERT INTO analytics.fct_market_observation (
      observation_key, series_id, metric_name, observation_date
    , metric_value, source_name, batch_id, ingested_at, updated_at
)
SELECT
      b.observation_key
    , b.series_id
    , b.metric_name
    , b.observation_date
    , b.metric_value
    , b.source_name
    , b.batch_id
    , b.ingested_at
    , now()
FROM batch AS b
LEFT JOIN analytics.fct_market_observation AS t
       ON t.observation_key = b.observation_key
WHERE t.observation_key IS NULL          -- genuinely new
   OR t.metric_value <> b.metric_value   -- upstream revised the value
ON CONFLICT (observation_key) DO UPDATE SET
      metric_value = excluded.metric_value
    , source_name  = excluded.source_name
    , batch_id     = excluded.batch_id
    , ingested_at  = excluded.ingested_at
    , updated_at   = excluded.updated_at;
