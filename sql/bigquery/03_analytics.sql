-- Analytical model: monthly summary per series and metric.
--
-- Written as CREATE OR REPLACE TABLE rather than a view so downstream queries
-- scan a small aggregate (tens of thousands of rows) instead of re-reading the
-- fact table every time. For a table this size a view would also be correct;
-- the table is the cheaper default once more than one consumer exists.
--
-- Placeholders ${project_id} and ${dataset} are substituted by pipeline/bq.py.

CREATE OR REPLACE TABLE `${project_id}.${dataset}.mart_metric_monthly`
PARTITION BY month_start
CLUSTER BY series_id, metric_name
AS
WITH monthly AS (
    SELECT
          series_id
        , metric_name
        , DATE_TRUNC(observation_date, MONTH)                  AS month_start
        , COUNT(*)                                             AS observation_count
        , AVG(metric_value)                                    AS avg_value
        , MIN(metric_value)                                    AS min_value
        , MAX(metric_value)                                    AS max_value
        -- Value on the last observed date in the month (the month's "close").
        , ARRAY_AGG(metric_value ORDER BY observation_date DESC LIMIT 1)[OFFSET(0)] AS close_value
        , MAX(observation_date)                                AS last_observation_date
    FROM `${project_id}.${dataset}.fct_market_observation`
    GROUP BY series_id, metric_name, month_start
)
SELECT
      series_id
    , metric_name
    , month_start
    , observation_count
    , ROUND(avg_value, 4) AS avg_value
    , min_value
    , max_value
    , close_value
    , last_observation_date
    , LAG(close_value) OVER series_month AS prev_month_close
    , ROUND(
          SAFE_DIVIDE(
              100 * (close_value - LAG(close_value) OVER series_month)
            , LAG(close_value) OVER series_month
          )
      , 4) AS mom_pct_change
    , ROUND(
          AVG(close_value) OVER (
              PARTITION BY series_id, metric_name
              ORDER BY month_start
              ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
          )
      , 4) AS trailing_12m_avg_close
FROM monthly
WINDOW series_month AS (PARTITION BY series_id, metric_name ORDER BY month_start);
