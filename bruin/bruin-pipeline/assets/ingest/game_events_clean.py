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


def materialize() -> pd.DataFrame:
    """
    Incremental: read from ingest.game_events_raw (_kafka__data JSON),
    write to raw.game_events on analytics. Watermark in meta.load_state.
    """
    source_dsn = get_required_env("RAW_SOURCE_PG_DSN")
    source_table = get_required_env("RAW_SOURCE_PG_TABLE")
    dest_dsn = get_required_env("RAW_DEST_PG_DSN")
    state_table = get_required_env("STATE_TABLE")
    asset_key = os.getenv("RAW_ASSET_KEY", "raw.game_events")
    lookback_ms = int(os.getenv("RAW_LOOKBACK_MS", "0"))

    with psycopg2.connect(dest_dsn) as dest_conn:
        ensure_state_table(dest_conn, state_table)
        last_wm = get_last_watermark(dest_conn, state_table, asset_key)

    wm_for_query = max(0, last_wm - lookback_ms)

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
            if cur.fetchone() is None:
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

    select_sql = f"""
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

    with psycopg2.connect(source_dsn) as src_conn:
        df = pd.read_sql_query(select_sql, src_conn, params=[wm_for_query])

    if not df.empty:
        new_wm = int(df["updated_at"].max())
        with psycopg2.connect(dest_dsn) as dest_conn:
            set_last_watermark(dest_conn, state_table, asset_key, new_wm)

    return df
