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

CREATE DATABASE airflowdb;
