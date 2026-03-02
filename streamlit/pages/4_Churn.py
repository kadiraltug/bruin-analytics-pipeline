import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from lib.db import (
    load_churn,
    load_churn_by_segment,
    load_time_to_churn,
    load_rolling_churn_7d,
    delta_str,
)

st.set_page_config(layout="wide", page_title="Churn", page_icon="📉")
st_autorefresh(interval=30_000, key="datarefresh")

st.title("📉 Churn Analysis")

churn = load_churn()

if churn.empty:
    st.info("Waiting for churn data...")
    st.stop()

churn_sorted = churn.sort_values("event_date")

cols = st.columns(4)

val, d = delta_str(churn_sorted["d7_churn_pct"].fillna(0))
cols[0].metric("D7 Churn Rate", f"{val:.1f}%", delta=d, delta_color="inverse")

val, d = delta_str(churn_sorted["d1_churn_pct"].fillna(0))
cols[1].metric("D1 Churn Rate", f"{val:.1f}%", delta=d, delta_color="inverse")

latest = churn_sorted.iloc[-1]
cols[2].metric("Installs (Latest Cohort)", f"{int(latest['installs']):,}")

rolling = load_rolling_churn_7d()
if not rolling.empty:
    r = rolling.iloc[0]
    cols[3].metric(
        "7D Rolling Churn",
        f"{(r['churn_rate_7d'] or 0):.1f}%",
        help="Among users active in the last 7 days, % who have been inactive for 7+ days.",
        delta_color="inverse",
    )

st.divider()

c1, c2 = st.columns(2)

with c1:
    st.subheader("D1 / D7 / D30 Churn Over Time")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=churn_sorted["event_date"],
            y=churn_sorted["d1_churn_pct"],
            mode="lines+markers",
            name="D1 Churn %",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=churn_sorted["event_date"],
            y=churn_sorted["d7_churn_pct"],
            mode="lines+markers",
            name="D7 Churn %",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=churn_sorted["event_date"],
            y=churn_sorted["d30_churn_pct"],
            mode="lines+markers",
            name="D30 Churn %",
        )
    )
    fig.update_layout(yaxis_title="%", margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Installs vs Active (D1/D7/D30)")
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=churn_sorted["event_date"],
            y=churn_sorted["installs"],
            name="Installs",
            marker_color="#00CC96",
        )
    )
    fig.add_trace(
        go.Bar(
            x=churn_sorted["event_date"],
            y=churn_sorted["d1_active"],
            name="D1 Active",
            marker_color="#636EFA",
        )
    )
    fig.add_trace(
        go.Bar(
            x=churn_sorted["event_date"],
            y=churn_sorted["d7_active"],
            name="D7 Active",
            marker_color="#AB63FA",
        )
    )
    fig.add_trace(
        go.Bar(
            x=churn_sorted["event_date"],
            y=churn_sorted["d30_active"],
            name="D30 Active",
            marker_color="#EF553B",
        )
    )
    fig.update_layout(
        barmode="group",
        legend=dict(orientation="h", y=-0.15),
        margin=dict(t=10),
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()
ttc = load_time_to_churn()

c3, c4 = st.columns(2)

with c3:
    st.subheader("Time to Inactivity (days)")
    if not ttc.empty:
        fig = px.bar(
            ttc,
            x="days_to_churn",
            y="user_count",
            labels={"days_to_churn": "Days", "user_count": "Users"},
            color_discrete_sequence=["#AB63FA"],
        )
        fig.update_layout(margin=dict(t=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No inactivity data yet.")

with c4:
    st.subheader("Churn % by Platform & Country (7D Rolling)")
    segments = load_churn_by_segment()
    if not segments.empty:
        fig = px.bar(
            segments.sort_values("churn_pct", ascending=True),
            x="churn_pct",
            y=segments.apply(
                lambda r: f"{r['platform']} / {r['country']}", axis=1
            ),
            orientation="h",
            labels={"x": "Churn %", "y": ""},
            color="platform",
            color_discrete_map={"ios": "#636EFA", "android": "#00CC96"},
        )
        fig.update_layout(
            margin=dict(t=10),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No segment data yet.")

with st.expander("Raw Data"):
    st.dataframe(churn_sorted, use_container_width=True)
