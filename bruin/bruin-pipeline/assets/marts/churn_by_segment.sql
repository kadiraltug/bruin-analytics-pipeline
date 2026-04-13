"""@bruin
name: marts.churn_by_segment
type: pg.sql
connection: pg-analytics-dest

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.game_events

columns:
  - name: platform
    type: string
    primary_key: true
  - name: country
    type: string
    primary_key: true
  - name: total_users
    type: bigint
    checks:
      - name: positive
  - name: churned_users
    type: bigint
  - name: churn_pct
    type: numeric
@bruin"""

WITH max_date AS (
  SELECT MAX(event_date::date) AS latest FROM staging.game_events
),
installs AS (
  SELECT DISTINCT ON (user_id)
    user_id, platform, country
  FROM staging.game_events
  WHERE event_name = 'user_register'
  ORDER BY user_id, event_ts
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
)
SELECT
  i.platform,
  i.country,
  COUNT(*) AS total_users,
  COUNT(*) FILTER (
    WHERE ls.last_active_date IS NULL
       OR ls.last_active_date <= m.latest - 7
  ) AS churned_users,
  ROUND(
    COUNT(*) FILTER (
      WHERE ls.last_active_date IS NULL
         OR ls.last_active_date <= m.latest - 7
    )::numeric / NULLIF(COUNT(*), 0) * 100,
    1
  ) AS churn_pct
FROM installs i
CROSS JOIN max_date m
LEFT JOIN last_seen ls ON i.user_id = ls.user_id
GROUP BY i.platform, i.country
ORDER BY churn_pct DESC;
