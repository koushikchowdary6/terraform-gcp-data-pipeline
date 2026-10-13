-- Data-quality checks (BigQuery).
--
-- Mirrors sql/duckdb/04_quality.sql. Returns one row per check:
-- (check_name, status, observed, detail). pipeline/bq.py fails the run on any
-- row with status = 'FAIL'.
--
-- Placeholders ${project_id} and ${dataset} are substituted by pipeline/bq.py.

WITH fact AS (
    SELECT * FROM `${project_id}.${dataset}.fct_market_observation`
)
, staged AS (
    SELECT * FROM `${project_id}.${dataset}.stg_market_observation`
)
, duplicate_keys AS (
    SELECT observation_key FROM fact GROUP BY observation_key HAVING COUNT(*) > 1
)
, duplicate_grain AS (
    SELECT series_id, metric_name, observation_date
    FROM fact
    GROUP BY series_id, metric_name, observation_date
    HAVING COUNT(*) > 1
)
, staged_not_loaded AS (
    SELECT s.observation_key
    FROM staged AS s
    LEFT JOIN fact AS f USING (observation_key)
    WHERE f.observation_key IS NULL
)
, value_mismatch AS (
    SELECT s.observation_key
    FROM staged AS s
    JOIN fact AS f USING (observation_key)
    WHERE f.metric_value != s.metric_value
)

SELECT 'fact_table_not_empty' AS check_name
     , IF(COUNT(*) > 0, 'PASS', 'FAIL') AS status
     , COUNT(*) AS observed
     , 'rows in fct_market_observation' AS detail
FROM fact

UNION ALL
SELECT 'observation_key_is_unique', IF(COUNT(*) = 0, 'PASS', 'FAIL'), COUNT(*)
     , 'observation_key values appearing more than once'
FROM duplicate_keys

UNION ALL
SELECT 'business_grain_is_unique', IF(COUNT(*) = 0, 'PASS', 'FAIL'), COUNT(*)
     , '(series_id, metric_name, observation_date) combinations appearing more than once'
FROM duplicate_grain

UNION ALL
SELECT 'no_null_metric_values', IF(COUNT(*) = 0, 'PASS', 'FAIL'), COUNT(*)
     , 'rows with a NULL metric_value'
FROM fact WHERE metric_value IS NULL

UNION ALL
SELECT 'no_future_observation_dates', IF(COUNT(*) = 0, 'PASS', 'FAIL'), COUNT(*)
     , 'rows dated after today'
FROM fact WHERE observation_date > CURRENT_DATE()

UNION ALL
SELECT 'all_staged_rows_loaded', IF(COUNT(*) = 0, 'PASS', 'FAIL'), COUNT(*)
     , 'staged observation_keys missing from the fact table'
FROM staged_not_loaded

UNION ALL
SELECT 'staged_values_match_fact', IF(COUNT(*) = 0, 'PASS', 'FAIL'), COUNT(*)
     , 'staged keys whose fact value differs from the staged value'
FROM value_mismatch

UNION ALL
SELECT 'vix_close_within_plausible_range', IF(COUNT(*) = 0, 'PASS', 'FAIL'), COUNT(*)
     , 'CBOE_VIX close values outside (0, 500]'
FROM fact
WHERE series_id = 'CBOE_VIX' AND metric_name = 'close'
  AND (metric_value <= 0 OR metric_value > 500)

ORDER BY check_name;
