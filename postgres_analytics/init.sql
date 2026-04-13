CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS raw.game_events (
  event_id     text PRIMARY KEY,
  user_id      text,
  session_id   text,
  event_name   text,
  event_ts     bigint,
  platform     text,
  country      text,
  app_version  text,
  device_model text,
  level        integer,
  result       text,
  duration_sec integer,
  revenue_usd  numeric,
  currency     text,
  updated_at   bigint
);

CREATE INDEX IF NOT EXISTS idx_raw_ge_updated_at ON raw.game_events (updated_at);

CREATE TABLE IF NOT EXISTS staging.game_events (
  event_id     text PRIMARY KEY,
  user_id      text,
  session_id   text,
  event_name   text,
  event_ts     timestamp,
  platform     text,
  country      text,
  app_version  text,
  device_model text,
  level        integer,
  result       text,
  duration_sec integer,
  revenue_usd  numeric,
  currency     text,
  event_date   date,
  updated_at   timestamp
);

CREATE INDEX IF NOT EXISTS idx_stg_ge_updated_at ON staging.game_events (updated_at);
CREATE INDEX IF NOT EXISTS idx_stg_ge_event_date ON staging.game_events (event_date);
CREATE INDEX IF NOT EXISTS idx_stg_ge_event_name ON staging.game_events (event_name);
CREATE INDEX IF NOT EXISTS idx_stg_ge_user_event ON staging.game_events (user_id, event_name);

CREATE TABLE IF NOT EXISTS meta.load_state (
  asset_key       text PRIMARY KEY,
  last_updated_at bigint NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS marts.daily_kpis (
  event_date                date PRIMARY KEY,
  dau                       bigint,
  sessions                  bigint,
  new_users                 bigint,
  payers                    bigint,
  iap_revenue_usd           numeric,
  ad_revenue_usd            numeric,
  total_revenue_usd         numeric,
  arpdau                    numeric,
  arppu                     numeric,
  sessions_per_user         numeric,
  avg_session_duration_sec  numeric
);

CREATE INDEX IF NOT EXISTS idx_marts_daily_kpis_event_date
  ON marts.daily_kpis (event_date);

CREATE TABLE IF NOT EXISTS marts.level_funnel_daily (
  event_date           date,
  level                integer,
  level_start_users    bigint,
  level_complete_users bigint,
  win_users            bigint,
  fail_users           bigint,
  completion_rate      numeric,
  win_rate             numeric,
  PRIMARY KEY (event_date, level)
);

CREATE INDEX IF NOT EXISTS idx_marts_level_funnel_event_date
  ON marts.level_funnel_daily (event_date);

CREATE TABLE IF NOT EXISTS marts.churn_daily (
  event_date     date PRIMARY KEY,
  installs       bigint,
  d1_active      bigint,
  d1_churn_pct   numeric,
  d7_active      bigint,
  d7_churn_pct   numeric,
  d30_active     bigint,
  d30_churn_pct  numeric
);

CREATE INDEX IF NOT EXISTS idx_marts_churn_daily_event_date
  ON marts.churn_daily (event_date);

CREATE TABLE IF NOT EXISTS marts.cohort_retention (
  cohort_date   date,
  day_n         integer,
  retained      bigint,
  cohort_size   bigint,
  retention_pct numeric,
  PRIMARY KEY (cohort_date, day_n)
);

CREATE TABLE IF NOT EXISTS marts.churn_by_segment (
  platform      text,
  country       text,
  total_users   bigint,
  churned_users bigint,
  churn_pct     numeric,
  PRIMARY KEY (platform, country)
);

CREATE TABLE IF NOT EXISTS marts.time_to_churn (
  days_to_churn integer PRIMARY KEY,
  user_count    bigint
);

CREATE TABLE IF NOT EXISTS marts.rolling_churn_7d (
  base_users    bigint,
  churned_7d    bigint,
  churn_rate_7d numeric
);

CREATE DATABASE airflowdb;
