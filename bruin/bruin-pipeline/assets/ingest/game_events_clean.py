"""@bruin
name: raw.game_events
type: python
image: python:3.11
connection: pg-analytics-dest

materialization:
  type: table
  strategy: merge
  parameters:
    enforce_schema: true

columns:
  - name: event_id
    type: string
    primary_key: true
  - name: user_id
    type: string
  - name: session_id
    type: string
  - name: event_name
    type: string
  - name: event_ts
    type: bigint
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
  - name: updated_at
    type: bigint
@bruin"""

""" 
This asset reads raw Kafka payloads from ingest.game_events_raw into raw.game_events
on the analytics Postgres, using updated_at as a watermark stored in meta.load_state.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import psycopg2
from utils.watermark import (
    compute_query_watermark,
    get_required_env,
    update_watermark_from_series,
)


# Read connection strings and watermark settings for this asset from env vars.
def _load_raw_config() -> dict:
    source_dsn = get_required_env("RAW_SOURCE_PG_DSN")
    source_table = get_required_env("RAW_SOURCE_PG_TABLE")
    dest_dsn = get_required_env("RAW_DEST_PG_DSN")
    state_table = get_required_env("STATE_TABLE")
    asset_key = os.getenv("RAW_ASSET_KEY", "raw.game_events")
    lookback_ms = int(os.getenv("RAW_LOOKBACK_MS", "0"))
    return {
        "source_dsn": source_dsn,
        "source_table": source_table,
        "dest_dsn": dest_dsn,
        "state_table": state_table,
        "asset_key": asset_key,
        "lookback_ms": lookback_ms,
    }


# Check if the ingest source table exists before we try to read from it.
def _source_table_exists(source_dsn: str, source_table: str) -> bool:
    schema, table = "public", source_table
    if "." in source_table:
        schema, table = source_table.split(".", 1)

    with psycopg2.connect(source_dsn) as src_conn:
        with src_conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table),
            )
            return cur.fetchone() is not None


# Create an empty dataframe that matches the raw.game_events schema.
def _empty_raw_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "event_id",
            "user_id",
            "session_id",
            "event_name",
            "event_ts",
            "platform",
            "country",
            "app_version",
            "device_model",
            "level",
            "result",
            "duration_sec",
            "revenue_usd",
            "currency",
            "updated_at",
        ]
    )


# Build the SQL that reads Kafka JSON payloads and casts them into columns.
def _build_select_sql(source_table: str) -> str:
    return f"""
        SELECT
          (_kafka__data::jsonb->>'event_id') AS event_id,
          (_kafka__data::jsonb->>'user_id') AS user_id,
          (_kafka__data::jsonb->>'session_id') AS session_id,
          (_kafka__data::jsonb->>'event_name') AS event_name,
          (_kafka__data::jsonb->>'event_ts')::bigint AS event_ts,
          (_kafka__data::jsonb->>'platform') AS platform,
          (_kafka__data::jsonb->>'country') AS country,
          (_kafka__data::jsonb->>'app_version') AS app_version,
          (_kafka__data::jsonb->>'device_model') AS device_model,
          NULLIF((_kafka__data::jsonb->>'level'), '')::integer AS level,
          (_kafka__data::jsonb->>'result') AS result,
          NULLIF((_kafka__data::jsonb->>'duration_sec'), '')::integer AS duration_sec,
          NULLIF((_kafka__data::jsonb->>'revenue_usd'), '')::numeric AS revenue_usd,
          (_kafka__data::jsonb->>'currency') AS currency,
          (_kafka__data::jsonb->>'updated_at')::bigint AS updated_at
        FROM {source_table}
        WHERE (_kafka__data::jsonb->>'updated_at')::bigint > %s
        ORDER BY (_kafka__data::jsonb->>'updated_at')::bigint
    """


# Fetch one incremental batch above the given watermark from the ingest table.
def _fetch_incremental_chunk(source_dsn: str, select_sql: str, watermark: int) -> pd.DataFrame:
    with psycopg2.connect(source_dsn) as src_conn:
        return pd.read_sql_query(select_sql, src_conn, params=[watermark])


# Main entrypoint that Bruin calls to materialize raw.game_events incrementally.
def materialize() -> pd.DataFrame:
    cfg = _load_raw_config()
    wm_for_query = compute_query_watermark(
        cfg["dest_dsn"],
        state_table=cfg["state_table"],
        asset_key=cfg["asset_key"],
        lookback_ms=cfg["lookback_ms"],
    )

    if not _source_table_exists(cfg["source_dsn"], cfg["source_table"]):
        return _empty_raw_frame()

    select_sql = _build_select_sql(cfg["source_table"])
    df = _fetch_incremental_chunk(cfg["source_dsn"], select_sql, wm_for_query)
    if not df.empty:
        update_watermark_from_series(
            cfg["dest_dsn"],
            cfg["state_table"],
            cfg["asset_key"],
            df["updated_at"],
            timestamp_ms=False,
        )
    return df
