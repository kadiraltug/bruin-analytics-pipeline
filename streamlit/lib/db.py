import os

import pandas as pd
import psycopg2
import streamlit as st

DSN = os.getenv("ANALYTICS_PG_DSN")


@st.cache_resource
def _get_conn():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    return conn


def query(sql: str, params=None) -> pd.DataFrame:
    try:
        conn = _get_conn()
        if conn.closed:
            st.cache_resource.clear()
            conn = _get_conn()
        return pd.read_sql_query(sql, conn, params=params)
    except Exception as e:
        st.cache_resource.clear()
        st.warning(f"Query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=25)
def load_kpis():
    return query("SELECT * FROM marts.daily_kpis ORDER BY event_date")


@st.cache_data(ttl=25)
def load_funnel():
    return query("SELECT * FROM marts.level_funnel_daily ORDER BY event_date, level")


@st.cache_data(ttl=25)
def load_churn():
    return query("SELECT * FROM marts.churn_daily ORDER BY event_date")


@st.cache_data(ttl=25)
def load_cohort_retention():
    return query("""
        WITH registrations AS (
            SELECT user_id, MIN(event_date) AS cohort_date
            FROM staging.game_events
            WHERE event_name = 'user_register'
            GROUP BY user_id
        ),
        cohort_sizes AS (
            SELECT cohort_date, COUNT(*) AS cohort_size
            FROM registrations
            GROUP BY cohort_date
        ),
        sessions AS (
            SELECT DISTINCT user_id, event_date AS active_date
            FROM staging.game_events
            WHERE event_name = 'session_start'
        ),
        retention AS (
            SELECT
                r.cohort_date,
                (s.active_date - r.cohort_date) AS day_n,
                COUNT(DISTINCT s.user_id) AS retained
            FROM registrations r
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
            ROUND(ret.retained::numeric / NULLIF(cs.cohort_size, 0) * 100, 1)
                AS retention_pct
        FROM retention ret
        JOIN cohort_sizes cs ON ret.cohort_date = cs.cohort_date
        WHERE ret.day_n BETWEEN 0 AND 14
        ORDER BY ret.cohort_date, ret.day_n
    """)


@st.cache_data(ttl=25)
def load_churn_by_segment():
    return query("""
        WITH max_date AS (
            SELECT MAX(event_date::date) AS latest FROM staging.game_events
        ),
        installs AS (
            SELECT DISTINCT ON (user_id)
                user_id, platform, country
            FROM staging.game_events
            WHERE event_name = 'user_register'
            ORDER BY user_id, event_ts
        ),
        activity AS (
            SELECT DISTINCT
                user_id,
                event_date::date AS active_date
            FROM staging.game_events
            WHERE event_name = 'session_start'
        ),
        last_seen AS (
            SELECT
                user_id,
                MAX(active_date) AS last_active_date
            FROM activity
            GROUP BY user_id
        )
        SELECT
            i.platform,
            i.country,
            COUNT(*) AS total_users,
            COUNT(*) FILTER (
                WHERE ls.last_active_date IS NULL
                   OR ls.last_active_date <= m.latest - 7
            ) AS churned_users,
            ROUND(
                COUNT(*) FILTER (
                    WHERE ls.last_active_date IS NULL
                       OR ls.last_active_date <= m.latest - 7
                )::numeric
                / NULLIF(COUNT(*), 0) * 100,
                1
            ) AS churn_pct
        FROM installs i
        CROSS JOIN max_date m
        LEFT JOIN last_seen ls ON i.user_id = ls.user_id
        GROUP BY i.platform, i.country
        ORDER BY churn_pct DESC
    """)


@st.cache_data(ttl=25)
def load_time_to_churn():
    return query("""
        WITH max_date AS (
            SELECT MAX(event_date::date) AS latest FROM staging.game_events
        ),
        installs AS (
            SELECT
                user_id,
                MIN(event_date::date) AS install_date
            FROM staging.game_events
            WHERE event_name = 'user_register'
            GROUP BY user_id
        ),
        activity AS (
            SELECT DISTINCT
                user_id,
                event_date::date AS active_date
            FROM staging.game_events
            WHERE event_name = 'session_start'
        ),
        last_seen AS (
            SELECT
                user_id,
                MAX(active_date) AS last_active_date
            FROM activity
            GROUP BY user_id
        )
        SELECT
            (ls.last_active_date - i.install_date) AS days_to_churn,
            COUNT(*) AS user_count
        FROM installs i
        JOIN last_seen ls ON i.user_id = ls.user_id
        CROSS JOIN max_date m
        WHERE ls.last_active_date >= i.install_date
          AND ls.last_active_date <= m.latest - 7
        GROUP BY 1
        ORDER BY 1
    """)


@st.cache_data(ttl=25)
def load_rolling_churn_7d():
    return query("""
        WITH max_date AS (
            SELECT MAX(event_date::date) AS latest FROM staging.game_events
        ),
        activity AS (
            SELECT DISTINCT
                user_id,
                event_date::date AS active_date
            FROM staging.game_events
            WHERE event_name = 'session_start'
        ),
        last_seen AS (
            SELECT
                user_id,
                MAX(active_date) AS last_active_date
            FROM activity
            GROUP BY user_id
        ),
        base AS (
            SELECT DISTINCT a.user_id
            FROM activity a, max_date m
            WHERE a.active_date BETWEEN m.latest - 13 AND m.latest - 7
        )
        SELECT
            COUNT(*) AS base_users,
            COUNT(*) FILTER (
                WHERE ls.last_active_date <= m.latest - 7
            ) AS churned_7d,
            ROUND(
                COUNT(*) FILTER (
                    WHERE ls.last_active_date <= m.latest - 7
                )::numeric
                / NULLIF(COUNT(*), 0) * 100,
                2
            ) AS churn_rate_7d
        FROM base b
        JOIN last_seen ls ON b.user_id = ls.user_id
        CROSS JOIN max_date m
    """)


@st.cache_data(ttl=25)
def load_revenue_by_segment():
    return query("""
        SELECT
            event_date,
            platform,
            country,
            event_name,
            SUM(revenue_usd) AS revenue,
            COUNT(DISTINCT user_id) AS users
        FROM staging.game_events
        WHERE event_name IN ('iap_purchase', 'ad_impression')
        GROUP BY 1, 2, 3, 4
        ORDER BY 1
    """)


@st.cache_data(ttl=25)
def load_pipeline_health():
    counts = query("""
        SELECT 'raw.game_events' AS tbl, COUNT(*) AS rows FROM raw.game_events
        UNION ALL
        SELECT 'staging.game_events', COUNT(*) FROM staging.game_events
        UNION ALL
        SELECT 'marts.daily_kpis', COUNT(*) FROM marts.daily_kpis
        UNION ALL
        SELECT 'marts.level_funnel_daily', COUNT(*) FROM marts.level_funnel_daily
        UNION ALL
        SELECT 'marts.churn_daily', COUNT(*) FROM marts.churn_daily
    """)
    watermarks = query("SELECT * FROM meta.load_state ORDER BY asset_key")
    events = query("""
        SELECT event_name, COUNT(*) AS cnt
        FROM staging.game_events
        GROUP BY event_name ORDER BY cnt DESC
    """)
    return counts, watermarks, events


def delta_str(series: pd.Series):
    if len(series) < 1:
        return 0, None
    latest = series.iloc[-1]
    if len(series) < 2:
        return latest, None
    prev = series.iloc[-2]
    if prev == 0:
        return latest, None
    diff = latest - prev
    pct = diff / abs(prev) * 100
    sign = "+" if diff >= 0 else ""
    return latest, f"{sign}{pct:.1f}%"
