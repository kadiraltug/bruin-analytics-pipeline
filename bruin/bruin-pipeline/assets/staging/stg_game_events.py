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

depends_on:
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

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import psycopg2
from utils.watermark import (
    ensure_state_table,
    get_last_watermark,
    get_required_env,
    set_last_watermark,
)


def _to_utc_naive_ts(s: pd.Series) -> pd.Series:

    if pd.api.types.is_numeric_dtype(s):
        dt = pd.to_datetime(s, unit="ms", utc=True, errors="coerce")
    else:
        dt = pd.to_datetime(s, utc=True, errors="coerce")

    dt = dt.dt.floor("us")
    return dt.dt.tz_convert(None)


def materialize() -> pd.DataFrame:
    dest_dsn = get_required_env("STAGING_DEST_PG_DSN")
    source_table = get_required_env("STAGING_SOURCE_TABLE")
    state_table = get_required_env("STATE_TABLE")
    asset_key = get_required_env("STAGING_ASSET_KEY")
    lookback_ms = int(get_required_env("STAGING_LOOKBACK_MS"))

    with psycopg2.connect(dest_dsn) as conn:
        ensure_state_table(conn, state_table)
        last_wm = get_last_watermark(conn, state_table, asset_key)

    wm_for_query = max(0, last_wm - lookback_ms)

    select_sql = f"""
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

    with psycopg2.connect(dest_dsn) as conn:
        df = pd.read_sql_query(select_sql, conn, params=[int(wm_for_query)])

    if df.empty:
        return df

    df = df.sort_values("updated_at").drop_duplicates("event_id", keep="last")

    if "event_ts" in df.columns:
        df["event_ts"] = _to_utc_naive_ts(df["event_ts"])
    if "updated_at" in df.columns:
        df["updated_at"] = _to_utc_naive_ts(df["updated_at"])

    for col in [
        "event_id", "user_id", "session_id", "event_name",
        "platform", "country", "app_version", "device_model",
        "result", "currency",
    ]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    cleaned = pd.DataFrame({
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
    })

    cleaned = cleaned[
        (cleaned["event_id"].notna())
        & (cleaned["user_id"].notna())
        & (cleaned["updated_at"].notna())
    ]

    new_wm_ts = cleaned["updated_at"].max()
    new_wm_ms = int(new_wm_ts.timestamp() * 1000)

    with psycopg2.connect(dest_dsn) as conn:
        set_last_watermark(conn, state_table, asset_key, new_wm_ms)

    return cleaned
