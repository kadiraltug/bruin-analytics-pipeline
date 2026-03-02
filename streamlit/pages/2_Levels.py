import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from lib.db import load_funnel

st.set_page_config(layout="wide", page_title="Levels", page_icon="🧩")
st_autorefresh(interval=30_000, key="datarefresh")

st.title("🧩 Level Performance")

funnel = load_funnel()

if funnel.empty:
    st.info("Waiting for level data...")
    st.stop()

ALL_DATES = "All dates (aggregate)"
available_dates = sorted(funnel["event_date"].unique())
options = [ALL_DATES] + list(available_dates)
selected = st.selectbox("Date", options, index=0)

if selected == ALL_DATES:
    day = (
        funnel
        .groupby("level", as_index=False)
        .agg(
            level_start_users=("level_start_users", "sum"),
            level_complete_users=("level_complete_users", "sum"),
            win_users=("win_users", "sum"),
            fail_users=("fail_users", "sum"),
        )
    )
    day["completion_rate"] = day["level_complete_users"] / day["level_start_users"].replace(0, float("nan"))
    day["win_rate"] = day["win_users"] / (day["win_users"] + day["fail_users"]).replace(0, float("nan"))
    day = day.fillna(0).sort_values("level")
    label = "All dates"
else:
    day = funnel[funnel["event_date"] == selected].sort_values("level")
    label = str(selected)

c1, c2 = st.columns([2, 1])

with c1:
    st.subheader(f"User Drop-off ({label})")
    fig = px.bar(
        day, x="level",
        y=["level_start_users", "level_complete_users"],
        barmode="group",
        color_discrete_map={
            "level_start_users": "#636EFA",
            "level_complete_users": "#00CC96",
        },
        labels={"value": "Users", "level": "Level", "variable": ""},
    )
    fig.update_layout(
        legend=dict(orientation="h", y=-0.15), margin=dict(t=10),
    )
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Win Rate by Level")
    fig = px.line(
        day, x="level", y="win_rate", markers=True,
        labels={"win_rate": "Win Rate", "level": "Level"},
    )
    fig.add_hline(y=0.5, line_dash="dash", line_color="red",
                  annotation_text="50%")
    fig.update_layout(margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("Completion Rate Heatmap (Level x Date)")

pivot = funnel.pivot_table(
    index="level", columns="event_date", values="completion_rate",
)
if not pivot.empty:
    fig = px.imshow(
        pivot,
        labels=dict(x="Date", y="Level", color="Completion Rate"),
        color_continuous_scale="RdYlGn",
        aspect="auto",
    )
    fig.update_layout(margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader(f"Win vs Fail ({label})")

c3, c4 = st.columns(2)
with c3:
    fig = px.bar(
        day, x="level", y=["win_users", "fail_users"],
        barmode="stack",
        color_discrete_map={"win_users": "#00CC96", "fail_users": "#EF553B"},
        labels={"value": "Users", "level": "Level", "variable": ""},
    )
    fig.update_layout(
        legend=dict(orientation="h", y=-0.15), margin=dict(t=10),
    )
    st.plotly_chart(fig, use_container_width=True)

with c4:
    st.subheader("Completion Rate by Level")
    fig = px.line(
        day, x="level", y="completion_rate", markers=True,
        labels={"completion_rate": "Rate", "level": "Level"},
    )
    fig.update_layout(margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

with st.expander("Raw Data"):
    st.dataframe(day, use_container_width=True)
