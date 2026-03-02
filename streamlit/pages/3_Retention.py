import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from lib.db import load_cohort_retention

st.set_page_config(layout="wide", page_title="Retention", page_icon="📅")
st_autorefresh(interval=30_000, key="datarefresh")

st.title("📅 Cohort Retention")

cohort = load_cohort_retention()

if cohort.empty:
    st.info("Need at least 2 simulated days to build cohort analysis...")
    st.stop()

st.subheader("Cohort Sizes (new users per day)")

sizes = (
    cohort[cohort["day_n"] == 0][["cohort_date", "cohort_size"]]
    .drop_duplicates()
    .sort_values("cohort_date")
)
fig = px.bar(
    sizes, x="cohort_date", y="cohort_size",
    labels={"cohort_date": "Registration Date", "cohort_size": "Users"},
    color_discrete_sequence=["#636EFA"],
)
fig.update_layout(margin=dict(t=10))
st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("Retention Heatmap (% active on Day N)")

pivot = cohort.pivot_table(
    index="cohort_date", columns="day_n", values="retention_pct",
)
if not pivot.empty:
    pivot.columns = [f"D{int(c)}" for c in pivot.columns]
    pivot.index = [str(d) for d in pivot.index]

    fig = px.imshow(
        pivot,
        labels=dict(x="Day", y="Cohort", color="%"),
        color_continuous_scale="Blues",
        aspect="auto",
        text_auto=".0f",
    )
    fig.update_layout(margin=dict(t=10), height=max(300, len(pivot) * 40))
    st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("Retention Curves (last 5 cohorts)")

recent_cohorts = sorted(cohort["cohort_date"].unique())[-5:]
recent = cohort[cohort["cohort_date"].isin(recent_cohorts)]

if not recent.empty:
    recent = recent.copy()
    recent["cohort_date"] = recent["cohort_date"].astype(str)
    fig = px.line(
        recent, x="day_n", y="retention_pct", color="cohort_date",
        markers=True,
        labels={"day_n": "Day", "retention_pct": "%", "cohort_date": "Cohort"},
    )
    fig.add_hline(y=50, line_dash="dash", line_color="gray",
                  annotation_text="50%")
    fig.update_layout(margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

with st.expander("Raw Data"):
    st.dataframe(cohort, use_container_width=True)
