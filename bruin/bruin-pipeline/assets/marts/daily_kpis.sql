"""@bruin
name: marts.daily_kpis
type: pg.sql
connection: pg-analytics-dest

materialization:
  type: table
  strategy: merge

depends:
  - staging.game_events

columns:
  - name: event_date
    type: date
    primary_key: true
    checks:
      - name: not_null
  - name: dau
    type: bigint
    checks:
      - name: positive
  - name: sessions
    type: bigint
    checks:
      - name: positive
  - name: new_users
    type: bigint
    checks:
      - name: positive
  - name: payers
    type: bigint
    checks:
      - name: positive
  - name: iap_revenue_usd
    type: numeric
    checks:
      - name: positive
  - name: ad_revenue_usd
    type: numeric
    checks:
      - name: positive
  - name: total_revenue_usd
    type: numeric
    checks:
      - name: positive
  - name: arpdau
    type: numeric
  - name: arppu
    type: numeric
  - name: sessions_per_user
    type: numeric
  - name: avg_session_duration_sec
    type: numeric
@bruin"""

WITH last_date AS (
  SELECT COALESCE(MAX(event_date), '1900-01-01'::date) AS max_dt
  FROM marts.daily_kpis
),
base AS (
  SELECT
    s.event_date,
    s.user_id,
    s.session_id,
    s.event_name,
    s.revenue_usd,
    s.duration_sec
  FROM staging.game_events s, last_date l
  WHERE s.event_date >= l.max_dt
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
