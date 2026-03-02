import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from lib.db import load_pipeline_health, query

st.set_page_config(layout="wide", page_title="Pipeline", page_icon="🔧")
st_autorefresh(interval=30_000, key="datarefresh")

st.title("🔧 Pipeline Health")

counts, watermarks, events = load_pipeline_health()

st.subheader("Table Row Counts")
if not counts.empty:
    cols = st.columns(len(counts))
    for i, row in counts.iterrows():
        cols[i].metric(row["tbl"], f"{int(row['rows']):,}")
else:
    st.warning("Could not read table counts.")

st.divider()
st.subheader("Watermarks (last processed timestamp)")

if not watermarks.empty:
    cols = st.columns(len(watermarks))
    for i, (_, row) in enumerate(watermarks.iterrows()):
        wm_ms = int(row["last_updated_at"])
        if wm_ms > 0:
            wm_dt = pd.to_datetime(wm_ms, unit="ms", utc=True)
            age = pd.Timestamp.utcnow() - wm_dt
            mins = int(age.total_seconds() // 60)
            secs = int(age.total_seconds() % 60)
            cols[i].metric(
                row["asset_key"],
                wm_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
                delta=f"{mins}m {secs}s ago",
                delta_color="off",
            )
        else:
            cols[i].metric(row["asset_key"], "Not started yet")
else:
    st.info("No watermark state found.")

st.divider()
st.subheader("Event Distribution")

if not events.empty:
    c1, c2 = st.columns(2)
    with c1:
        fig = px.pie(
            events, values="cnt", names="event_name", hole=0.4,
        )
        fig.update_layout(margin=dict(t=10))
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        fig = px.bar(
            events, x="cnt", y="event_name", orientation="h",
            labels={"cnt": "Count", "event_name": ""},
            color_discrete_sequence=["#636EFA"],
        )
        fig.update_layout(margin=dict(t=10), yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("Export Data")

export_choice = st.selectbox("Table", [
    "marts.daily_kpis",
    "marts.level_funnel_daily",
    "marts.churn_daily",
    "staging.game_events (last 1000)",
])

if st.button("Generate CSV"):
    if export_choice == "staging.game_events (last 1000)":
        df = query("SELECT * FROM staging.game_events ORDER BY updated_at DESC LIMIT 1000")
    else:
        df = query(f"SELECT * FROM {export_choice}")

    if not df.empty:
        st.download_button(
            "Download",
            df.to_csv(index=False),
            file_name=f"{export_choice.replace('.', '_').replace(' ', '_')}.csv",
            mime="text/csv",
        )
    else:
        st.warning("No data to export.")
