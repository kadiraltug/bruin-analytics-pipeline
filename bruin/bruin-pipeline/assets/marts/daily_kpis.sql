"""@bruin
name: marts.daily_kpis
type: pg.sql
connection: pg-analytics-dest

materialization:
  type: table
  strategy: create+replace

depends_on:
  - staging.game_events
@bruin"""

WITH base AS (
  SELECT
    event_date,
    user_id,
    session_id,
    event_name,
    revenue_usd,
    duration_sec
  FROM staging.game_events
),
agg AS (
  SELECT
    event_date,
    COUNT(DISTINCT user_id) FILTER (WHERE event_name = 'session_start') AS dau,
    COUNT(*) FILTER (WHERE event_name = 'session_start') AS sessions,
    COUNT(DISTINCT user_id) FILTER (WHERE event_name = 'user_register') AS new_users,
    COUNT(DISTINCT user_id) FILTER (WHERE event_name = 'iap_purchase') AS payers,
    SUM(revenue_usd) FILTER (WHERE event_name = 'iap_purchase') AS iap_revenue_usd,
    SUM(revenue_usd) FILTER (WHERE event_name = 'ad_impression') AS ad_revenue_usd,
    SUM(revenue_usd) FILTER (WHERE event_name IN ('iap_purchase', 'ad_impression')) AS total_revenue_usd,
    AVG(duration_sec) FILTER (WHERE event_name = 'session_end' AND duration_sec > 0) AS avg_session_duration_sec
  FROM base
  GROUP BY 1
)
SELECT
  event_date,
  dau,
  sessions,
  new_users,
  payers,
  iap_revenue_usd,
  ad_revenue_usd,
  total_revenue_usd,
  CASE WHEN dau = 0 THEN 0 ELSE total_revenue_usd / dau END AS arpdau,
  CASE WHEN payers = 0 THEN 0 ELSE iap_revenue_usd / payers END AS arppu,
  CASE WHEN dau = 0 THEN 0 ELSE sessions::numeric / dau END AS sessions_per_user,
  COALESCE(avg_session_duration_sec, 0) AS avg_session_duration_sec
FROM agg
ORDER BY event_date;
