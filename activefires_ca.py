import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from snowflake.snowpark import Session
from streamlit_plotly_events import plotly_events

# =========================
# CONFIG
# =========================
st.set_page_config(layout="wide")

dark_mode = st.sidebar.toggle("Dark Mode", True)
size_factor = st.sidebar.slider("Bubble Size", 0.5, 3.0, 1.2, 0.1)

# =========================
# SNOWFLAKE SESSION
# =========================
def get_session():
    return Session.builder.configs(st.secrets["snowflake"]).create()

# =========================
# LOAD FILTER VALUES (FAST)
# =========================
@st.cache_data
def load_filters():
    session = get_session()
    return session.sql("""
        SELECT DISTINCT 
            PROVINCE,
            RESPONSE_TYPE_DESCRIPTION,
            STAGE_OF_CONTROL_DESCRIPTION
        FROM EVA.GIS.VW_ACTIVEFIRES
    """).to_pandas()

filters_df = load_filters()

# =========================
# FILTER UI
# =========================
c1, c2, c3 = st.columns(3)

with c1:
    prov = st.selectbox("Province", ["All"] + sorted(filters_df["PROVINCE"].dropna().unique()))

with c2:
    resp = st.selectbox("Response", ["All"] + sorted(filters_df["RESPONSE_TYPE_DESCRIPTION"].dropna().unique()))

with c3:
    stage = st.selectbox("Stage", ["All"] + sorted(filters_df["STAGE_OF_CONTROL_DESCRIPTION"].dropna().unique()))

# =========================
# LOAD FILTERED DATA (SQL PUSH)
# =========================
@st.cache_data
def load_data(prov, resp, stage):
    session = get_session()

    query = """
    SELECT FIRENAME, PROVINCE, HECTARES,
           RESPONSE_TYPE_DESCRIPTION,
           STAGE_OF_CONTROL_DESCRIPTION,
           LAT, LON
    FROM EVA.GIS.VW_ACTIVEFIRES
    WHERE LAT IS NOT NULL AND LON IS NOT NULL
    """

    if prov != "All":
        query += f" AND PROVINCE = '{prov}'"
    if resp != "All":
        query += f" AND RESPONSE_TYPE_DESCRIPTION = '{resp}'"
    if stage != "All":
        query += f" AND STAGE_OF_CONTROL_DESCRIPTION = '{stage}'"

    return session.sql(query).to_pandas()

df = load_data(prov, resp, stage)

# =========================
# SESSION STATE
# =========================
if "selected_idx" not in st.session_state:
    st.session_state.selected_idx = []

# =========================
# APPLY SELECTION
# =========================
final_df = df.copy()

if st.session_state.selected_idx:
    final_df = df.iloc[st.session_state.selected_idx]

# =========================
# KPI CARDS
# =========================
k1, k2, k3 = st.columns(3)

def kpi(col, label, value):
    col.metric(label, value)

kpi(k1, "🔥 Fires", len(final_df))
kpi(k2, "🌲 Hectares", int(final_df["HECTARES"].sum()) if len(final_df) else 0)
kpi(k3, "📊 Avg Size", int(final_df["HECTARES"].mean()) if len(final_df) else 0)

st.markdown("---")

# =========================
# MAP (FIXED BUBBLES)
# =========================
st.subheader("🗺️ Fire Map")

if len(df) > 0:

    fig = px.scatter_mapbox(
        df,
        lat="LAT",
        lon="LON",
        size="HECTARES",          # 🔥 SIMPLE + RELIABLE
        size_max=30 * size_factor,
        color="STAGE_OF_CONTROL_DESCRIPTION",
        hover_name="FIRENAME",
        hover_data=["PROVINCE", "HECTARES"],
        zoom=3,
        height=650
    )

    fig.update_layout(
        mapbox_style="carto-darkmatter" if dark_mode else "carto-positron",
        showlegend=False,
        margin=dict(l=0, r=0, t=0, b=0)
    )

    selected = plotly_events(fig, click_event=True, select_event=True)

    if selected:
        st.session_state.selected_idx = [p["pointIndex"] for p in selected]

# =========================
# TABLE
# =========================
st.subheader("📋 Fire Details")
st.dataframe(final_df, use_container_width=True)

# =========================
# RESET
# =========================
if st.button("🔄 Reset Selection"):
    st.session_state.selected_idx = []
