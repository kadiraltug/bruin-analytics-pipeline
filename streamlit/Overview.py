import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from lib.db import load_kpis, delta_str

st.set_page_config(layout="wide", page_title="Overview", page_icon="📊")
st_autorefresh(interval=30_000, key="datarefresh")

st.title("📊 Overview")

kpis = load_kpis()

if kpis.empty:
    st.info("Waiting for data...")
    st.stop()

kpis_sorted = kpis.sort_values("event_date")

cols = st.columns(6)

val, d = delta_str(kpis_sorted["dau"])
cols[0].metric("DAU", f"{int(val):,}", delta=d)

val, d = delta_str(kpis_sorted["sessions"])
cols[1].metric("Sessions", f"{int(val):,}", delta=d)

val, d = delta_str(kpis_sorted["total_revenue_usd"].fillna(0))
cols[2].metric("Revenue", f"${val:,.2f}", delta=d)

val, d = delta_str(kpis_sorted["arpdau"].fillna(0))
cols[3].metric("ARPDAU", f"${val:,.3f}", delta=d)

val, d = delta_str(kpis_sorted["new_users"].fillna(0))
cols[4].metric("New Users", f"{int(val):,}", delta=d)

dau_last = kpis_sorted["dau"].iloc[-1]
payers_last = kpis_sorted["payers"].iloc[-1]
conv = payers_last / dau_last * 100 if dau_last > 0 else 0
cols[5].metric("Payer %", f"{conv:.1f}%")

st.divider()

c1, c2 = st.columns(2)

with c1:
    st.subheader("Revenue Composition")
    fig = px.bar(
        kpis_sorted,
        x="event_date",
        y=["iap_revenue_usd", "ad_revenue_usd"],
        barmode="stack",
        labels={"value": "USD", "event_date": "", "variable": "Source"},
        color_discrete_map={
            "iap_revenue_usd": "#00CC96",
            "ad_revenue_usd": "#636EFA",
        },
    )
    fig.update_layout(legend=dict(orientation="h", y=-0.15), margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Revenue Trend")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=kpis_sorted["event_date"],
            y=kpis_sorted["total_revenue_usd"],
            mode="lines+markers",
            name="Total Revenue",
            line=dict(color="#EF553B", width=2),
        )
    )
    fig.update_layout(yaxis_title="USD", margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

st.divider()

c3, c4 = st.columns(2)

with c3:
    st.subheader("DAU & New Users")
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=kpis_sorted["event_date"],
            y=kpis_sorted["dau"],
            name="DAU",
            marker_color="#636EFA",
        )
    )
    fig.add_trace(
        go.Bar(
            x=kpis_sorted["event_date"],
            y=kpis_sorted["new_users"],
            name="New Users",
            marker_color="#00CC96",
        )
    )
    fig.update_layout(
        barmode="group",
        legend=dict(orientation="h", y=-0.15),
        margin=dict(t=10),
    )
    st.plotly_chart(fig, use_container_width=True)

with c4:
    st.subheader("Sessions / User & Avg Duration")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=kpis_sorted["event_date"],
            y=kpis_sorted["sessions_per_user"],
            mode="lines+markers",
            name="Sessions / User",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=kpis_sorted["event_date"],
            y=kpis_sorted["avg_session_duration_sec"],
            mode="lines+markers",
            name="Avg Duration (s)",
            yaxis="y2",
            line=dict(dash="dot"),
        )
    )
    fig.update_layout(
        yaxis=dict(title="Sessions / User"),
        yaxis2=dict(title="Seconds", overlaying="y", side="right"),
        legend=dict(orientation="h", y=-0.15),
        margin=dict(t=10),
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()
c5, c6 = st.columns(2)

with c5:
    st.subheader("ARPPU")
    fig = px.line(
        kpis_sorted,
        x="event_date",
        y="arppu",
        markers=True,
        labels={"arppu": "USD", "event_date": ""},
    )
    fig.update_layout(margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

with c6:
    st.subheader("Payer Conversion Rate")
    conv_series = kpis_sorted.apply(
        lambda r: r["payers"] / r["dau"] * 100 if r["dau"] > 0 else 0,
        axis=1,
    )
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=kpis_sorted["event_date"],
            y=conv_series,
            mode="lines+markers",
            fill="tozeroy",
            line=dict(color="#AB63FA"),
        )
    )
    fig.update_layout(yaxis_title="%", margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

with st.expander("Raw Data"):
    st.dataframe(kpis_sorted, use_container_width=True)

