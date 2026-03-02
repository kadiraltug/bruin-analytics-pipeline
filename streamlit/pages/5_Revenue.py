import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from lib.db import load_kpis, load_revenue_by_segment

st.set_page_config(layout="wide", page_title="Revenue", page_icon="💰")
st_autorefresh(interval=30_000, key="datarefresh")

st.title("💰 Revenue Deep-Dive")

kpis = load_kpis()
seg = load_revenue_by_segment()

if kpis.empty:
    st.info("Waiting for revenue data...")
    st.stop()

kpis_sorted = kpis.sort_values("event_date")

c1, c2 = st.columns(2)

with c1:
    st.subheader("IAP vs Ad Revenue")
    iap_total = kpis_sorted["iap_revenue_usd"].sum()
    ad_total = kpis_sorted["ad_revenue_usd"].sum()
    fig = px.pie(
        names=["IAP", "Ad"],
        values=[iap_total, ad_total],
        color_discrete_sequence=["#00CC96", "#636EFA"],
        hole=0.4,
    )
    fig.update_layout(margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Cumulative Revenue")
    kpis_sorted = kpis_sorted.copy()
    kpis_sorted["cum_revenue"] = kpis_sorted["total_revenue_usd"].cumsum()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=kpis_sorted["event_date"], y=kpis_sorted["cum_revenue"],
        mode="lines", fill="tozeroy",
        line=dict(color="#EF553B", width=2),
    ))
    fig.update_layout(yaxis_title="USD", margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

if seg.empty:
    st.stop()

st.divider()
c3, c4 = st.columns(2)

with c3:
    st.subheader("Revenue by Platform")
    by_platform = (
        seg.groupby(["event_date", "platform"])["revenue"]
        .sum().reset_index()
    )
    fig = px.bar(
        by_platform, x="event_date", y="revenue", color="platform",
        barmode="group",
        color_discrete_map={"ios": "#636EFA", "android": "#00CC96"},
        labels={"revenue": "USD", "event_date": ""},
    )
    fig.update_layout(
        legend=dict(orientation="h", y=-0.15), margin=dict(t=10),
    )
    st.plotly_chart(fig, use_container_width=True)

with c4:
    st.subheader("Revenue by Country")
    by_country = (
        seg.groupby("country")["revenue"]
        .sum().reset_index()
        .sort_values("revenue", ascending=False)
    )
    fig = px.bar(
        by_country, x="country", y="revenue",
        labels={"revenue": "USD", "country": ""},
        color_discrete_sequence=["#AB63FA"],
    )
    fig.update_layout(margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("IAP vs Ad Revenue by Country")

by_country_src = (
    seg.groupby(["country", "event_name"])["revenue"]
    .sum().reset_index()
)
fig = px.bar(
    by_country_src, x="country", y="revenue", color="event_name",
    barmode="stack",
    color_discrete_map={
        "iap_purchase": "#00CC96",
        "ad_impression": "#636EFA",
    },
    labels={"revenue": "USD", "country": "", "event_name": "Source"},
)
fig.update_layout(
    legend=dict(orientation="h", y=-0.15), margin=dict(t=10),
)
st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("Paying Users by Platform")

payers_platform = (
    seg[seg["event_name"] == "iap_purchase"]
    .groupby(["event_date", "platform"])["users"]
    .sum().reset_index()
)
if not payers_platform.empty:
    fig = px.line(
        payers_platform, x="event_date", y="users", color="platform",
        markers=True,
        color_discrete_map={"ios": "#636EFA", "android": "#00CC96"},
        labels={"users": "Payers", "event_date": ""},
    )
    fig.update_layout(
        legend=dict(orientation="h", y=-0.15), margin=dict(t=10),
    )
    st.plotly_chart(fig, use_container_width=True)
