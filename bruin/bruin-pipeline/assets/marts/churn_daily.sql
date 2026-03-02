"""@bruin
name: marts.churn_daily
type: pg.sql
connection: pg-analytics-dest

materialization:
  type: table
  strategy: create+replace

depends_on:
  - staging.game_events
@bruin"""

WITH installs AS (
  SELECT
    user_id,
    MIN(event_date::date) AS install_date
  FROM staging.game_events
  WHERE event_name = 'user_register'
  GROUP BY 1
),
activity AS (
  SELECT DISTINCT
    user_id,
    event_date::date AS active_date
  FROM staging.game_events
  WHERE event_name = 'session_start'
),
max_date AS (
  SELECT MAX(event_date::date) AS latest FROM staging.game_events
)
SELECT
  i.install_date AS event_date,
  COUNT(*) AS installs,
  CASE WHEN i.install_date + 1 <= m.latest
       THEN COUNT(*) FILTER (WHERE a1.user_id IS NOT NULL) END AS d1_active,
  CASE WHEN i.install_date + 1 <= m.latest
       THEN ROUND((1 - (COUNT(*) FILTER (WHERE a1.user_id IS NOT NULL)::numeric / NULLIF(COUNT(*),0))) * 100, 2) END AS d1_churn_pct,
  CASE WHEN i.install_date + 7 <= m.latest
       THEN COUNT(*) FILTER (WHERE a7.user_id IS NOT NULL) END AS d7_active,
  CASE WHEN i.install_date + 7 <= m.latest
       THEN ROUND((1 - (COUNT(*) FILTER (WHERE a7.user_id IS NOT NULL)::numeric / NULLIF(COUNT(*),0))) * 100, 2) END AS d7_churn_pct,
  CASE WHEN i.install_date + 30 <= m.latest
       THEN COUNT(*) FILTER (WHERE a30.user_id IS NOT NULL) END AS d30_active,
  CASE WHEN i.install_date + 30 <= m.latest
       THEN ROUND((1 - (COUNT(*) FILTER (WHERE a30.user_id IS NOT NULL)::numeric / NULLIF(COUNT(*),0))) * 100, 2) END AS d30_churn_pct
FROM installs i
CROSS JOIN max_date m
LEFT JOIN activity a1  ON a1.user_id  = i.user_id AND a1.active_date  = i.install_date + 1
LEFT JOIN activity a7  ON a7.user_id  = i.user_id AND a7.active_date  = i.install_date + 7
LEFT JOIN activity a30 ON a30.user_id = i.user_id AND a30.active_date = i.install_date + 30
GROUP BY i.install_date, m.latest
ORDER BY 1;
