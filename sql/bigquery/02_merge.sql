-- Incremental MERGE from the staging table into the partitioned fact table.
--
-- This is the BigQuery counterpart of sql/duckdb/02_upsert.sql. Semantics are
-- identical; only the dialect differs.
--
-- Cost notes:
--   * The fact table is partitioned on observation_date and clustered on
--     (series_id, metric_name) by Terraform.
--   * The `T.observation_date BETWEEN ...` predicate added to the ON clause
--     lets BigQuery prune partitions instead of scanning the whole table.
--     Without it, every MERGE would be a full-table scan - the single most
--     common source of surprise cost in a BigQuery pipeline.
--   * The WHEN MATCHED clause is guarded so unchanged rows are not rewritten,
--     which keeps `updated_at` stable across no-op reruns.
--
-- Placeholders ${project_id} and ${dataset} are substituted by pipeline/bq.py.

MERGE `${project_id}.${dataset}.fct_market_observation` AS T
USING (
    -- Collapse same-key rows inside the batch; highest source row wins.
    SELECT * EXCEPT (row_rank)
    FROM (
        SELECT
              S.*
            , ROW_NUMBER() OVER (
                  PARTITION BY observation_key
                  ORDER BY source_row_number DESC, ingested_at DESC
              ) AS row_rank
        FROM `${project_id}.${dataset}.stg_market_observation` AS S
    )
    WHERE row_rank = 1
) AS B
ON  T.observation_key = B.observation_key
-- Partition pruning: restrict the target scan to the window the batch covers.
AND T.observation_date BETWEEN (
        SELECT MIN(observation_date) FROM `${project_id}.${dataset}.stg_market_observation`
    ) AND (
        SELECT MAX(observation_date) FROM `${project_id}.${dataset}.stg_market_observation`
    )

WHEN MATCHED AND T.metric_value != B.metric_value THEN UPDATE SET
      metric_value = B.metric_value
    , source_name  = B.source_name
    , batch_id     = B.batch_id
    , ingested_at  = B.ingested_at
    , updated_at   = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
      observation_key, series_id, metric_name, observation_date
    , metric_value, source_name, batch_id, ingested_at, updated_at
) VALUES (
      B.observation_key, B.series_id, B.metric_name, B.observation_date
    , B.metric_value, B.source_name, B.batch_id, B.ingested_at, CURRENT_TIMESTAMP()
);
