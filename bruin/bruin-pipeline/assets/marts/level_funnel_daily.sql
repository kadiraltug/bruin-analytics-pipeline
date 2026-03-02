"""@bruin
name: marts.level_funnel_daily
type: pg.sql
connection: pg-analytics-dest

materialization:
  type: table
  strategy: create+replace

depends_on:
  - staging.game_events
@bruin"""

WITH lvl AS (
  SELECT
    event_date,
    level,
    event_name,
    user_id,
    result
  FROM staging.game_events
  WHERE event_name IN ('level_start', 'level_complete')
    AND level IS NOT NULL
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