-- Load one or more ingestion batches from Parquet into the staging table.
--
-- Staging is rebuilt on every run (CREATE OR REPLACE): it holds only what the
-- current run intends to merge. The durable record lives in the fact table.
--
-- Parameter $1: a Parquet glob, e.g. 'data/processed/*.parquet'.

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE TABLE staging.market_observation AS
SELECT
      observation_key
    , series_id
    , metric_name
    , CAST(observation_date AS DATE)            AS observation_date
    , CAST(metric_value AS DECIMAL(20, 6))      AS metric_value
    , source_name
    , CAST(source_row_number AS BIGINT)         AS source_row_number
    , batch_id
    , CAST(ingested_at AS TIMESTAMP WITH TIME ZONE) AS ingested_at
FROM read_parquet(?);
