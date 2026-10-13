-- Analytical model: monthly summary per series and metric.
--
-- Demonstrates the aggregation and window-function patterns the BigQuery
-- version uses (sql/bigquery/03_analytics.sql), so local output can be
-- compared against cloud output.

CREATE OR REPLACE TABLE analytics.mart_metric_monthly AS
WITH monthly AS (
    SELECT
          series_id
        , metric_name
        , DATE_TRUNC('month', observation_date)              AS month_start
        , COUNT(*)                                           AS observation_count
        , AVG(metric_value)                                  AS avg_value
        , MIN(metric_value)                                  AS min_value
        , MAX(metric_value)                                  AS max_value
        -- Value on the last observed date in the month (the month's "close").
        , arg_max(metric_value, observation_date)            AS close_value
        , MAX(observation_date)                              AS last_observation_date
    FROM analytics.fct_market_observation
    GROUP BY series_id, metric_name, month_start
)
SELECT
      series_id
    , metric_name
    , month_start
    , observation_count
    , ROUND(avg_value, 4)   AS avg_value
    , min_value
    , max_value
    , close_value
    , last_observation_date
    , LAG(close_value) OVER (
          PARTITION BY series_id, metric_name ORDER BY month_start
      ) AS prev_month_close
    , ROUND(
          100.0 * (close_value - LAG(close_value) OVER (
              PARTITION BY series_id, metric_name ORDER BY month_start))
          / NULLIF(LAG(close_value) OVER (
              PARTITION BY series_id, metric_name ORDER BY month_start), 0)
      , 4) AS mom_pct_change
    -- Trailing 12-month average of monthly closes.
    , ROUND(AVG(close_value) OVER (
          PARTITION BY series_id, metric_name
          ORDER BY month_start
          ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
      ), 4) AS trailing_12m_avg_close
FROM monthly;
