-- Example analytical queries (BigQuery).
--
-- Not run by the pipeline. Each one is written to be cheap: it reads the small
-- monthly mart, or filters the fact table on its clustering keys and partition
-- column so BigQuery prunes rather than scans.
--
-- Replace ${project_id} and ${dataset} before running in the console.

-- 1. Highest-volatility months on record, by VIX monthly close.
SELECT month_start, close_value AS vix_close, mom_pct_change, trailing_12m_avg_close
FROM `${project_id}.${dataset}.mart_metric_monthly`
WHERE series_id = 'CBOE_VIX' AND metric_name = 'close'
ORDER BY close_value DESC
LIMIT 10;

-- 2. Months where the S&P 500 closed more than 10% above its own trailing
--    12-month average.
SELECT
      month_start
    , close_value AS index_close
    , trailing_12m_avg_close
    , ROUND(SAFE_DIVIDE(100 * (close_value - trailing_12m_avg_close), trailing_12m_avg_close), 2)
          AS pct_above_trend
FROM `${project_id}.${dataset}.mart_metric_monthly`
WHERE series_id = 'SP500_COMPOSITE' AND metric_name = 'index_level'
  AND trailing_12m_avg_close IS NOT NULL
  AND close_value > 1.10 * trailing_12m_avg_close
ORDER BY month_start DESC
LIMIT 20;

-- 3. Rank calendar years by average VIX.
--    The WHERE clause hits both clustering keys, so this does not scan the
--    whole fact table.
WITH yearly AS (
    SELECT
          EXTRACT(YEAR FROM observation_date) AS calendar_year
        , AVG(metric_value)                   AS avg_vix
        , COUNT(*)                            AS trading_days
    FROM `${project_id}.${dataset}.fct_market_observation`
    WHERE series_id = 'CBOE_VIX' AND metric_name = 'close'
    GROUP BY calendar_year
)
SELECT
      calendar_year
    , ROUND(avg_vix, 3) AS avg_vix
    , trading_days
    , DENSE_RANK() OVER (ORDER BY avg_vix DESC) AS volatility_rank
FROM yearly
ORDER BY volatility_rank
LIMIT 15;

-- 4. Coverage report: what each series actually contains.
SELECT
      series_id
    , metric_name
    , COUNT(*)              AS observations
    , MIN(observation_date)  AS first_date
    , MAX(observation_date)  AS last_date
FROM `${project_id}.${dataset}.fct_market_observation`
GROUP BY series_id, metric_name
ORDER BY series_id, metric_name;

-- 5. Dry-run cost check. Run this in the console before any new query:
--    bq query --dry_run --use_legacy_sql=false '<your query>'
--    It reports bytes that would be processed without incurring charges.
