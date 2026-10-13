-- Data-quality checks.
--
-- Returns one row per check: (check_name, status, observed, detail).
-- status is 'PASS' or 'FAIL'; the pipeline fails the run on any FAIL unless
-- PIPELINE_FAIL_ON_QUALITY_ERROR=false.
--
-- Each check is written so that `observed` is the number of offending rows,
-- which makes a failure immediately actionable - the same predicate can be
-- re-run as a SELECT to list the bad rows.

WITH duplicate_keys AS (
    SELECT observation_key
    FROM analytics.fct_market_observation
    GROUP BY observation_key
    HAVING COUNT(*) > 1
)
, duplicate_grain AS (
    SELECT series_id, metric_name, observation_date
    FROM analytics.fct_market_observation
    GROUP BY series_id, metric_name, observation_date
    HAVING COUNT(*) > 1
)
, staged_not_loaded AS (
    SELECT s.observation_key
    FROM staging.market_observation AS s
    LEFT JOIN analytics.fct_market_observation AS f
           ON f.observation_key = s.observation_key
    WHERE f.observation_key IS NULL
)
, value_mismatch AS (
    -- Every staged key must end up in the fact table carrying the staged value.
    SELECT s.observation_key
    FROM staging.market_observation AS s
    JOIN analytics.fct_market_observation AS f
      ON f.observation_key = s.observation_key
    WHERE f.metric_value <> s.metric_value
)

SELECT 'fact_table_not_empty' AS check_name
     , CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
     , COUNT(*) AS observed
     , 'rows in analytics.fct_market_observation' AS detail
FROM analytics.fct_market_observation

UNION ALL
SELECT 'observation_key_is_unique'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , 'observation_key values appearing more than once'
FROM duplicate_keys

UNION ALL
SELECT 'business_grain_is_unique'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , '(series_id, metric_name, observation_date) combinations appearing more than once'
FROM duplicate_grain

UNION ALL
SELECT 'no_null_metric_values'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , 'rows with a NULL metric_value'
FROM analytics.fct_market_observation
WHERE metric_value IS NULL

UNION ALL
SELECT 'no_future_observation_dates'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , 'rows dated after today'
FROM analytics.fct_market_observation
WHERE observation_date > CURRENT_DATE

UNION ALL
SELECT 'all_staged_rows_loaded'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , 'staged observation_keys missing from the fact table'
FROM staged_not_loaded

UNION ALL
SELECT 'staged_values_match_fact'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , 'staged keys whose fact value differs from the staged value'
FROM value_mismatch

UNION ALL
SELECT 'vix_close_within_plausible_range'
     , CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
     , COUNT(*)
     , 'CBOE_VIX close values outside (0, 500]'
FROM analytics.fct_market_observation
WHERE series_id = 'CBOE_VIX'
  AND metric_name = 'close'
  AND (metric_value <= 0 OR metric_value > 500)

ORDER BY check_name;
