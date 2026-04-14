import streamlit as st
import pydeck as pdk
import pandas as pd
import numpy as np
from snowflake.snowpark import Session

# =========================
# PAGE CONFIG
# =========================
st.set_page_config(page_title="Canada Active Fires Dashboard", layout="wide")

# =========================
# SIDEBAR
# =========================
st.sidebar.header("🎨 Appearance")
dark_mode = st.sidebar.toggle("Dark Mode", value=True)

st.sidebar.header("🗺️ Map Controls")
size_factor = st.sidebar.slider("Bubble Size", 0.5, 3.0, 1.2, 0.1)

# =========================
# THEME
# =========================
if dark_mode:
    bg, card, sidebar, text, filter_text = "#0E1117", "#1F2937", "#111827", "white", "white"
else:
    bg, card, sidebar, text, filter_text = "#F9FAFB", "#FFFFFF", "#F3F4F6", "#111827", "black"

# =========================
# CSS
# =========================
st.markdown(f"""
<style>
.stApp {{ background-color: {bg}; color: {text}; }}
section[data-testid="stSidebar"] {{ background-color: {sidebar}; }}

.top-bar {{
    position: sticky; top: 0; z-index: 999;
    background-color: {card};
    padding: 15px; border-radius: 12px; margin-bottom: 10px;
}}

.kpi-card {{
    background-color: {card};
    padding: 20px; border-radius: 12px; text-align: center;
}}

.insight {{
    background-color: {card};
    padding: 15px; border-radius: 10px; margin-bottom: 10px;
}}

.chip {{
    display:inline-block; padding:6px 12px; margin:4px;
    border-radius:20px; background:{card}; border:1px solid #ccc;
}}

div[data-baseweb="select"] * {{ color:{filter_text} !important; }}
</style>
""", unsafe_allow_html=True)

# =========================
# TITLE
# =========================
st.title("🔥 Canada Active Fires Dashboard")
st.markdown("<br>", unsafe_allow_html=True)

# =========================
# SNOWFLAKE OPTIMIZED QUERY
# =========================
def get_session():
    return Session.builder.configs(st.secrets["snowflake"]).create()

@st.cache_data
def load_data(province, response, stage):
    session = get_session()

    query = """
    SELECT 
        FIRENAME, PROVINCE, HECTARES,
        RESPONSE_TYPE_DESCRIPTION,
        STAGE_OF_CONTROL_DESCRIPTION,
        LAT, LON, STARTDATE, DAYS_ACTIVE
    FROM EVA.GIS.VW_ACTIVEFIRES
    WHERE 1=1
    """

    if province != "All":
        query += f" AND PROVINCE = '{province}'"
    if response != "All":
        query += f" AND RESPONSE_TYPE_DESCRIPTION = '{response}'"
    if stage != "All":
        query += f" AND STAGE_OF_CONTROL_DESCRIPTION = '{stage}'"

    return session.sql(query).to_pandas()

# =========================
# FILTER BAR
# =========================
st.markdown('<div class="top-bar">', unsafe_allow_html=True)

# (Load small distinct lists once)
@st.cache_data
def load_filter_values():
    session = get_session()
    df = session.sql("SELECT DISTINCT PROVINCE, RESPONSE_TYPE_DESCRIPTION, STAGE_OF_CONTROL_DESCRIPTION FROM EVA.GIS.VW_ACTIVEFIRES").to_pandas()
    return df

filters_df = load_filter_values()

c1, c2, c3 = st.columns(3)

with c1:
    provinces = ["All"] + sorted(filters_df["PROVINCE"].dropna().unique())
    selected_province = st.selectbox("Province", provinces)

with c2:
    responses = ["All"] + sorted(filters_df["RESPONSE_TYPE_DESCRIPTION"].dropna().unique())
    selected_response = st.selectbox("Response Type", responses)

with c3:
    stages = ["All"] + sorted(filters_df["STAGE_OF_CONTROL_DESCRIPTION"].dropna().unique())
    selected_stage = st.selectbox("Stage of Control", stages)

st.markdown('</div>', unsafe_allow_html=True)

# =========================
# LOAD FILTERED DATA (SQL PUSHDOWN)
# =========================
df = load_data(selected_province, selected_response, selected_stage)

# =========================
# CLICK SELECTION STATE
# =========================
if "selected_fire" not in st.session_state:
    st.session_state.selected_fire = None

# =========================
# MAP PREP
# =========================
COLOR_MAP = {
    "Out of Control": [220, 38, 38, 200],
    "Being Held": [245, 158, 11, 200],
    "Under Control": [34, 139, 34, 200],
    "Extinguished": [156, 163, 175, 200],
}

df["color"] = df["STAGE_OF_CONTROL_DESCRIPTION"].map(lambda x: COLOR_MAP.get(x, [200,200,200,180]))

# Radius scaling
min_r, max_r = 2000*size_factor, 20000*size_factor
log_h = np.log1p(df["HECTARES"].fillna(1))
df["radius"] = min_r + (max_r-min_r)*(log_h-log_h.min())/(log_h.max()-log_h.min()+1e-9)

# =========================
# INSIGHTS PANEL
# =========================
st.subheader("🧠 Top Insights")

if len(df) > 0:
    biggest = df.loc[df["HECTARES"].idxmax()]
    top_province = df["PROVINCE"].value_counts().idxmax()
    out_pct = (df["STAGE_OF_CONTROL_DESCRIPTION"] == "Out of Control").mean()*100
    avg_days = df["DAYS_ACTIVE"].mean()

    st.markdown(f"""
    <div class="insight">🔥 Largest fire: <b>{biggest["FIRENAME"]}</b> ({int(biggest["HECTARES"]):,} ha)</div>
    <div class="insight">📍 Most fires in: <b>{top_province}</b></div>
    <div class="insight">⚠️ Out of control: <b>{out_pct:.1f}%</b></div>
    <div class="insight">⏳ Avg duration: <b>{avg_days:.1f} days</b></div>
    """, unsafe_allow_html=True)

# =========================
# MAP (CLICK ENABLED)
# =========================
st.subheader("🗺️ Fire Map")

layer = pdk.Layer(
    "ScatterplotLayer",
    data=df,
    get_position=["LON","LAT"],
    get_color="color",
    get_radius="radius",
    pickable=True,
)

deck = pdk.Deck(
    layers=[layer],
    initial_view_state=pdk.ViewState(
        latitude=df["LAT"].mean() if len(df) else 56,
        longitude=df["LON"].mean() if len(df) else -96,
        zoom=4,
    ),
    map_style="dark" if dark_mode else "light",
)

event = st.pydeck_chart(deck)

# ⚡ CLICK HANDLING
if event and "picked" in event and len(event["picked"]) > 0:
    picked = event["picked"][0]
    st.session_state.selected_fire = picked["object"]["FIRENAME"]

# =========================
# TABLE (CLICK FILTER)
# =========================
st.subheader("📋 Fire Details")

table_df = df.copy()

if st.session_state.selected_fire:
    table_df = table_df[table_df["FIRENAME"] == st.session_state.selected_fire]
    st.info(f"Filtered by selected fire: {st.session_state.selected_fire}")

st.dataframe(table_df, use_container_width=True)