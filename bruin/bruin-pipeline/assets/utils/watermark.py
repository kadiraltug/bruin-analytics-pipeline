
import os


def get_required_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"{name} must be set")
    return v


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


def get_last_watermark(conn, state_table: str, asset_key: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT last_updated_at FROM {state_table} WHERE asset_key=%s",
            (asset_key,),
        )
        row = cur.fetchone()
    return int(row[0]) if row else 0


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
