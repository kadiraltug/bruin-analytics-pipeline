"""
This module contains utility functions for working with watermarks in the database.
"""

import os
import psycopg2

# Get a required environment variable.
def get_required_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"{name} must be set")
    return v

# Ensure the state table exists.
def ensure_state_table(conn, state_table: str) -> None:
    if "." in state_table:
        schema = state_table.split(".", 1)[0]
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {state_table} (
              asset_key       text PRIMARY KEY,
              last_updated_at bigint NOT NULL DEFAULT 0
            );
            """
        )
    conn.commit()

# Get the last watermark for an asset.
def get_last_watermark(conn, state_table: str, asset_key: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT last_updated_at FROM {state_table} WHERE asset_key=%s",
            (asset_key,),
        )
        row = cur.fetchone()
    return int(row[0]) if row else 0

# Set the last watermark for an asset.
def set_last_watermark(
    conn, state_table: str, asset_key: str, last_updated_at: int
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {state_table}(asset_key, last_updated_at)
            VALUES (%s, %s)
            ON CONFLICT (asset_key)
            DO UPDATE SET last_updated_at = EXCLUDED.last_updated_at;
            """,
            (asset_key, int(last_updated_at)),
        )
    conn.commit()


# Compute the watermark for a query based on the last watermark and lookback.
def compute_query_watermark(
    dsn: str, state_table: str, asset_key: str, lookback_ms: int
) -> int:
    with psycopg2.connect(dsn) as conn:
        ensure_state_table(conn, state_table)
        last_wm = get_last_watermark(conn, state_table, asset_key)
    return max(0, last_wm - lookback_ms)

# Update the watermark for an asset based on the max value in a series.
def update_watermark_from_series(
    dsn: str,
    state_table: str,
    asset_key: str,
    values,
    *,
    timestamp_ms: bool,
) -> None:

    if values is None:
        return
    try:
        latest = values.max()
    except Exception:
        return
    if latest is None:
        return

    if timestamp_ms:
        new_wm = int(latest.timestamp() * 1000)
    else:
        new_wm = int(latest)

    with psycopg2.connect(dsn) as conn:
        set_last_watermark(conn, state_table, asset_key, new_wm)
