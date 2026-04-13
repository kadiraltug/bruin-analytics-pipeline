"""
Watermark utilities.

The target table itself is the source of truth: get_target_watermark()
reads MAX(updated_at) from the output table.  If a batch was set but
Bruin failed to write it, the MAX hasn't moved and the batch will be
re-processed automatically.  No separate state verification is needed.

meta.load_state is still written by update_watermark_for_dashboard()
purely for the Streamlit Pipeline Health page.
"""

import os
import psycopg2


def get_required_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"{name} must be set")
    return v


def get_target_watermark(
    dsn: str,
    table: str,
    column: str,
    column_is_epoch_ms: bool = True,
    lookback_ms: int = 0,
) -> int:
    try:
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                if column_is_epoch_ms:
                    cur.execute(f"SELECT COALESCE(MAX({column}), 0) FROM {table}")
                else:
                    cur.execute(
                        f"SELECT COALESCE(EXTRACT(EPOCH FROM MAX({column})) * 1000, 0) "
                        f"FROM {table}"
                    )
                row = cur.fetchone()
        wm = int(float(row[0])) if row and row[0] else 0
    except Exception:
        wm = 0
    return max(0, wm - lookback_ms)


def _ensure_state_table(conn, state_table: str) -> None:
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


def _set_dashboard_watermark(
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


def update_watermark_for_dashboard(
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
        _ensure_state_table(conn, state_table)
        _set_dashboard_watermark(conn, state_table, asset_key, new_wm)
