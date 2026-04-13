"""@bruin
name: marts.time_to_churn
type: pg.sql
connection: pg-analytics-dest

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.game_events

columns:
  - name: days_to_churn
    type: integer
    primary_key: true
  - name: user_count
    type: bigint
    checks:
      - name: positive
@bruin"""

WITH max_date AS (
  SELECT MAX(event_date::date) AS latest FROM staging.game_events
),
installs AS (
  SELECT user_id, MIN(event_date::date) AS install_date
  FROM staging.game_events
  WHERE event_name = 'user_register'
  GROUP BY user_id
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
  (ls.last_active_date - i.install_date) AS days_to_churn,
  COUNT(*) AS user_count
FROM installs i
JOIN last_seen ls ON i.user_id = ls.user_id
CROSS JOIN max_date m
WHERE ls.last_active_date >= i.install_date
  AND ls.last_active_date <= m.latest - 7
GROUP BY 1
ORDER BY 1;
