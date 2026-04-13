"""@bruin
name: marts.rolling_churn_7d
type: pg.sql
connection: pg-analytics-dest

materialization:
  type: table
  strategy: create+replace

depends_on:
  - staging.game_events

columns:
  - name: base_users
    type: bigint
  - name: churned_7d
    type: bigint
  - name: churn_rate_7d
    type: numeric
@bruin"""

WITH max_date AS (
  SELECT MAX(event_date::date) AS latest FROM staging.game_events
),
activity AS (
  SELECT DISTINCT user_id, event_date::date AS active_date
  FROM staging.game_events
  WHERE event_name = 'session_start'
),
last_seen AS (
  SELECT user_id, MAX(active_date) AS last_active_date
  FROM activity
  GROUP BY user_id
),
base AS (
  SELECT DISTINCT a.user_id
  FROM activity a, max_date m
  WHERE a.active_date BETWEEN m.latest - 13 AND m.latest - 7
)
SELECT
  COUNT(*) AS base_users,
  COUNT(*) FILTER (
    WHERE ls.last_active_date <= m.latest - 7
  ) AS churned_7d,
  ROUND(
    COUNT(*) FILTER (
      WHERE ls.last_active_date <= m.latest - 7
    )::numeric / NULLIF(COUNT(*), 0) * 100,
    2
  ) AS churn_rate_7d
FROM base b
JOIN last_seen ls ON b.user_id = ls.user_id
CROSS JOIN max_date m;
