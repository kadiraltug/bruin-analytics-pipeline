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
    return query("SELECT * FROM marts.cohort_retention ORDER BY cohort_date, day_n")


@st.cache_data(ttl=25)
def load_churn_by_segment():
    return query("SELECT * FROM marts.churn_by_segment ORDER BY churn_pct DESC")


@st.cache_data(ttl=25)
def load_time_to_churn():
    return query("SELECT * FROM marts.time_to_churn ORDER BY days_to_churn")


@st.cache_data(ttl=25)
def load_rolling_churn_7d():
    return query("SELECT * FROM marts.rolling_churn_7d")


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
        WITH t AS (
            SELECT 1 AS pos, 'raw.game_events' AS tbl, COUNT(*) AS rows FROM raw.game_events
            UNION ALL
            SELECT 2, 'staging.game_events', COUNT(*) FROM staging.game_events
            UNION ALL
            SELECT 3, 'marts.daily_kpis', COUNT(*) FROM marts.daily_kpis
            UNION ALL
            SELECT 4, 'marts.level_funnel_daily', COUNT(*) FROM marts.level_funnel_daily
            UNION ALL
            SELECT 5, 'marts.churn_daily', COUNT(*) FROM marts.churn_daily
            UNION ALL
            SELECT 6, 'marts.cohort_retention', COUNT(*) FROM marts.cohort_retention
            UNION ALL
            SELECT 7, 'marts.churn_by_segment', COUNT(*) FROM marts.churn_by_segment
            UNION ALL
            SELECT 8, 'marts.time_to_churn', COUNT(*) FROM marts.time_to_churn
            UNION ALL
            SELECT 9, 'marts.rolling_churn_7d', COUNT(*) FROM marts.rolling_churn_7d
        )
        SELECT tbl, rows FROM t ORDER BY pos
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
