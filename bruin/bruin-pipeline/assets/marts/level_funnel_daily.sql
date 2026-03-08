"""@bruin
name: marts.level_funnel_daily
type: pg.sql
connection: pg-analytics-dest

materialization:
  type: table
  strategy: merge

depends_on:
  - staging.game_events

columns:
  - name: event_date
    type: date
    primary_key: true
    checks:
      - name: not_null
  - name: level
    type: integer
    primary_key: true
    checks:
      - name: positive
  - name: level_start_users
    type: bigint
    checks:
      - name: positive
  - name: level_complete_users
    type: bigint
    checks:
      - name: positive
  - name: win_users
    type: bigint
    checks:
      - name: positive
  - name: fail_users
    type: bigint
    checks:
      - name: positive
  - name: completion_rate
    type: numeric
  - name: win_rate
    type: numeric
@bruin"""

WITH last_date AS (
  SELECT COALESCE(MAX(event_date), '1900-01-01'::date) AS max_dt
  FROM marts.level_funnel_daily
),
lvl AS (
  SELECT
    s.event_date,
    s.level,
    s.event_name,
    s.user_id,
    s.result
  FROM staging.game_events s, last_date l
  WHERE s.event_name IN ('level_start', 'level_complete')
    AND s.level IS NOT NULL
    AND s.event_date >= l.max_dt
),
agg AS (
  SELECT
    event_date,
    level,
    COUNT(DISTINCT user_id) FILTER (WHERE event_name='level_start') AS level_start_users,
    COUNT(DISTINCT user_id) FILTER (WHERE event_name='level_complete') AS level_complete_users,
    COUNT(DISTINCT user_id) FILTER (WHERE event_name='level_complete' AND result='win') AS win_users,
    COUNT(DISTINCT user_id) FILTER (WHERE event_name='level_complete' AND result='fail') AS fail_users
  FROM lvl
  GROUP BY 1,2
)
SELECT
  event_date,
  level,
  level_start_users,
  level_complete_users,
  win_users,
  fail_users,
  CASE WHEN level_start_users=0 THEN 0
       ELSE level_complete_users::numeric / level_start_users END AS completion_rate,
  CASE WHEN win_users+fail_users=0 THEN 0
       ELSE win_users::numeric / (win_users+fail_users) END AS win_rate
FROM agg
ORDER BY event_date, level;