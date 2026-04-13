"""@bruin
name: staging.game_events
type: python
image: python:3.11
connection: pg-analytics-dest

materialization:
  type: table
  strategy: merge
  parameters:
    enforce_schema: true

depends:
  - raw.game_events

columns:
  - name: event_id
    type: string
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: user_id
    type: string
    checks:
      - name: not_null
  - name: session_id
    type: string
  - name: event_name
    type: string
  - name: event_ts
    type: timestamp
  - name: platform
    type: string
  - name: country
    type: string
  - name: app_version
    type: string
  - name: device_model
    type: string
  - name: level
    type: integer
  - name: result
    type: string
  - name: duration_sec
    type: integer
  - name: revenue_usd
    type: numeric
  - name: currency
    type: string
  - name: event_date
    type: date
  - name: updated_at
    type: timestamp
@bruin"""

"""
Cleans and normalizes raw.game_events, then upserts into staging.game_events.
The watermark is derived from MAX(updated_at) in the target table itself.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import psycopg2
from utils.watermark import (
    get_required_env,
    get_target_watermark,
    update_watermark_for_dashboard,
)


# Normalize timestamps to naive UTC, handling both numeric and datetime inputs.
def _to_utc_naive_ts(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        dt = pd.to_datetime(s, unit="ms", utc=True, errors="coerce")
    else:
        dt = pd.to_datetime(s, utc=True, errors="coerce")
    dt = dt.dt.floor("us")
    return dt.dt.tz_convert(None)


# Read staging connection and watermark settings from env vars.
def _load_staging_config() -> dict:
    dest_dsn = get_required_env("STAGING_DEST_PG_DSN")
    source_table = get_required_env("STAGING_SOURCE_TABLE")
    state_table = get_required_env("STATE_TABLE")
    asset_key = get_required_env("STAGING_ASSET_KEY")
    lookback_ms = int(get_required_env("STAGING_LOOKBACK_MS"))
    return {
        "dest_dsn": dest_dsn,
        "source_table": source_table,
        "state_table": state_table,
        "asset_key": asset_key,
        "lookback_ms": lookback_ms,
    }


# Build the SQL that pulls incremental rows from raw.game_events.
def _build_staging_select_sql(source_table: str) -> str:
    return f"""
        SELECT
            event_id,
            user_id,
            session_id,
            event_name,
            event_ts,
            platform,
            country,
            app_version,
            device_model,
            level,
            result,
            duration_sec,
            COALESCE(revenue_usd, 0) AS revenue_usd,
            currency,
            updated_at
        FROM {source_table}
        WHERE updated_at > %s
        ORDER BY updated_at
    """


# Fetch one incremental batch for staging above the current watermark.
def _fetch_staging_chunk(dest_dsn: str, select_sql: str, watermark: int) -> pd.DataFrame:
    with psycopg2.connect(dest_dsn) as conn:
        return pd.read_sql_query(select_sql, conn, params=[int(watermark)])


# Apply type fixes, string cleaning and basic validation for staging schema.
def _clean_staging_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("updated_at").drop_duplicates("event_id", keep="last")

    if "event_ts" in df.columns:
        df["event_ts"] = _to_utc_naive_ts(df["event_ts"])
    if "updated_at" in df.columns:
        df["updated_at"] = _to_utc_naive_ts(df["updated_at"])

    for col in [
        "event_id",
        "user_id",
        "session_id",
        "event_name",
        "platform",
        "country",
        "app_version",
        "device_model",
        "result",
        "currency",
    ]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    cleaned = pd.DataFrame(
        {
            "event_id": df["event_id"],
            "user_id": df["user_id"],
            "session_id": df["session_id"],
            "event_name": df["event_name"].str.strip().str.lower(),
            "event_ts": df["event_ts"],
            "platform": df["platform"].str.strip().str.lower(),
            "country": df["country"].str.strip().str.upper(),
            "app_version": df["app_version"].str.strip(),
            "device_model": df["device_model"].str.strip(),
            "level": df["level"],
            "result": df["result"].str.strip().str.lower(),
            "duration_sec": df["duration_sec"],
            "revenue_usd": df["revenue_usd"].where(df["revenue_usd"] >= 0),
            "currency": df["currency"].str.strip().str.upper(),
            "event_date": df["event_ts"].dt.date,
            "updated_at": df["updated_at"],
        }
    )

    cleaned = cleaned[
        (cleaned["event_id"].notna())
        & (cleaned["user_id"].notna())
        & (cleaned["updated_at"].notna())
    ]
    return cleaned



def materialize() -> pd.DataFrame:
    cfg = _load_staging_config()

    wm_for_query = get_target_watermark(
        cfg["dest_dsn"],
        table="staging.game_events",
        column="updated_at",
        column_is_epoch_ms=False,
        lookback_ms=cfg["lookback_ms"],
    )

    select_sql = _build_staging_select_sql(cfg["source_table"])
    df = _fetch_staging_chunk(cfg["dest_dsn"], select_sql, wm_for_query)
    if df.empty:
        return df

    cleaned = _clean_staging_frame(df)
    if not cleaned.empty:
        update_watermark_for_dashboard(
            cfg["dest_dsn"],
            cfg["state_table"],
            cfg["asset_key"],
            cleaned["updated_at"],
            timestamp_ms=True,
        )
    return cleaned
