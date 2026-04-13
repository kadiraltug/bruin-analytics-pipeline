"""@bruin
name: marts.cohort_retention
type: pg.sql
connection: pg-analytics-dest

materialization:
  type: table
  strategy: merge

depends:
  - staging.game_events

columns:
  - name: cohort_date
    type: date
    primary_key: true
    checks:
      - name: not_null
  - name: day_n
    type: integer
    primary_key: true
  - name: retained
    type: bigint
  - name: cohort_size
    type: bigint
    checks:
      - name: positive
  - name: retention_pct
    type: numeric
@bruin"""

WITH last_date AS (
  SELECT COALESCE(MAX(cohort_date), '1900-01-01'::date) AS max_dt
  FROM marts.cohort_retention
),
registrations AS (
  SELECT user_id, MIN(event_date) AS cohort_date
  FROM staging.game_events
  WHERE event_name = 'user_register'
  GROUP BY user_id
),
recent_cohorts AS (
  SELECT r.*
  FROM registrations r, last_date l
  WHERE r.cohort_date >= l.max_dt - 14
),
cohort_sizes AS (
  SELECT cohort_date, COUNT(*) AS cohort_size
  FROM recent_cohorts
  GROUP BY cohort_date
),
sessions AS (
  SELECT DISTINCT s.user_id, s.event_date AS active_date
  FROM staging.game_events s
  WHERE s.event_name = 'session_start'
    AND s.user_id IN (SELECT user_id FROM recent_cohorts)
),
retention AS (
  SELECT
    r.cohort_date,
    (s.active_date - r.cohort_date) AS day_n,
    COUNT(DISTINCT s.user_id) AS retained
  FROM recent_cohorts r
  INNER JOIN sessions s
    ON r.user_id = s.user_id
    AND s.active_date >= r.cohort_date
  GROUP BY r.cohort_date, (s.active_date - r.cohort_date)
)
SELECT
  ret.cohort_date,
  ret.day_n,
  ret.retained,
  cs.cohort_size,
  ROUND(ret.retained::numeric / NULLIF(cs.cohort_size, 0) * 100, 1) AS retention_pct
FROM retention ret
JOIN cohort_sizes cs ON ret.cohort_date = cs.cohort_date
WHERE ret.day_n BETWEEN 0 AND 14
ORDER BY ret.cohort_date, ret.day_n;
