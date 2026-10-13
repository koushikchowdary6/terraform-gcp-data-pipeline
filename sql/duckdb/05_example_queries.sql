-- Example analytical queries.
--
-- Not run by the pipeline. These are the queries to open in an interview or a
-- review to show what the modelled data answers. Run them with:
--   python -m pipeline.cli examples

-- 1. Most volatile months on record, by VIX monthly close.
SELECT
      month_start
    , close_value        AS vix_close
    , mom_pct_change     AS pct_change_vs_prev_month
    , trailing_12m_avg_close
FROM analytics.mart_metric_monthly
WHERE series_id = 'CBOE_VIX' AND metric_name = 'close'
ORDER BY close_value DESC
LIMIT 10;

-- 2. S&P 500 index level against its own trailing 12-month average,
--    flagging months that closed more than 10% above trend.
SELECT
      month_start
    , close_value                                   AS index_close
    , trailing_12m_avg_close
    , ROUND(100.0 * (close_value - trailing_12m_avg_close)
            / NULLIF(trailing_12m_avg_close, 0), 2) AS pct_above_trend
FROM analytics.mart_metric_monthly
WHERE series_id = 'SP500_COMPOSITE' AND metric_name = 'index_level'
  AND trailing_12m_avg_close IS NOT NULL
  AND close_value > 1.10 * trailing_12m_avg_close
ORDER BY month_start DESC
LIMIT 20;

-- 3. Rank each calendar year by average VIX, with a dense rank over all years.
WITH yearly AS (
    SELECT
          EXTRACT(YEAR FROM observation_date) AS calendar_year
        , AVG(metric_value)                   AS avg_vix
        , COUNT(*)                            AS trading_days
    FROM analytics.fct_market_observation
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
    , COUNT(*)                  AS observations
    , MIN(observation_date)      AS first_date
    , MAX(observation_date)      AS last_date
FROM analytics.fct_market_observation
GROUP BY series_id, metric_name
ORDER BY series_id, metric_name;
