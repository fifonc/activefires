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

div[data-baseweb="select"] * {{ color:{filter_text} !important; }}
</style>
""", unsafe_allow_html=True)

# =========================
# TITLE
# =========================
st.title("🔥 Canada Active Fires Dashboard")
st.markdown("<br>", unsafe_allow_html=True)

# =========================
# SNOWFLAKE SESSION
# =========================
def get_session():
    return Session.builder.configs(st.secrets["snowflake"]).create()

# =========================
# LOAD FILTER VALUES (SAFE)
# =========================
@st.cache_data
def load_filter_values():
    session = get_session()
    return session.sql("""
        SELECT DISTINCT 
            PROVINCE, 
            RESPONSE_TYPE_DESCRIPTION, 
            STAGE_OF_CONTROL_DESCRIPTION 
        FROM EVA.GIS.VW_ACTIVEFIRES
    """).to_pandas()

filters_df = load_filter_values()

# =========================
# FILTER BAR
# =========================
st.markdown('<div class="top-bar">', unsafe_allow_html=True)

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
# SAFE DATA LOAD (NO STRING CONCAT)
# =========================
@st.cache_data
def load_data():
    session = get_session()
    return session.sql("""
        SELECT 
            FIRENAME, PROVINCE, HECTARES,
            RESPONSE_TYPE_DESCRIPTION,
            STAGE_OF_CONTROL_DESCRIPTION,
            LAT, LON, STARTDATE, DAYS_ACTIVE
        FROM EVA.GIS.VW_ACTIVEFIRES
    """).to_pandas()

df = load_data()

# Apply filters in pandas (safe & fast enough with selected columns)
filtered = df.copy()

if selected_province != "All":
    filtered = filtered[filtered["PROVINCE"] == selected_province]

if selected_response != "All":
    filtered = filtered[filtered["RESPONSE_TYPE_DESCRIPTION"] == selected_response]

if selected_stage != "All":
    filtered = filtered[filtered["STAGE_OF_CONTROL_DESCRIPTION"] == selected_stage]

# =========================
# INSIGHTS
# =========================
st.subheader("🧠 Top Insights")

if len(filtered) > 0:
    biggest = filtered.loc[filtered["HECTARES"].idxmax()]
    st.markdown(f"""
    <div class="insight">🔥 Largest fire: <b>{biggest["FIRENAME"]}</b> ({int(biggest["HECTARES"]):,} ha)</div>
    <div class="insight">📍 Most fires in: <b>{filtered["PROVINCE"].value_counts().idxmax()}</b></div>
    <div class="insight">⚠️ Out of control: <b>{(filtered["STAGE_OF_CONTROL_DESCRIPTION"]=="Out of Control").mean()*100:.1f}%</b></div>
    <div class="insight">⏳ Avg duration: <b>{filtered["DAYS_ACTIVE"].mean():.1f} days</b></div>
    """, unsafe_allow_html=True)

# =========================
# MAP PREP
# =========================
COLOR_MAP = {
    "Out of Control": [220, 38, 38, 200],
    "Being Held": [245, 158, 11, 200],
    "Under Control": [34, 139, 34, 200],
    "Extinguished": [156, 163, 175, 200],
}

filtered["color"] = filtered["STAGE_OF_CONTROL_DESCRIPTION"].map(
    lambda x: COLOR_MAP.get(x, [200, 200, 200, 180])
)

# Radius scaling
min_r, max_r = 2000*size_factor, 20000*size_factor
log_h = np.log1p(filtered["HECTARES"].fillna(1))
filtered["radius"] = min_r + (max_r-min_r)*(log_h-log_h.min())/(log_h.max()-log_h.min()+1e-9)

# =========================
# MAP (TOOLTIPS RESTORED)
# =========================
st.subheader("🗺️ Fire Map")

tooltip = {
    "html": """
    <b>{FIRENAME}</b><br/>
    Province: {PROVINCE}<br/>
    Hectares: {HECTARES}<br/>
    Stage: {STAGE_OF_CONTROL_DESCRIPTION}<br/>
    Days Active: {DAYS_ACTIVE}
    """,
    "style": {"backgroundColor": "#222", "color": "white"}
}

layer = pdk.Layer(
    "ScatterplotLayer",
    data=filtered,
    get_position=["LON","LAT"],
    get_color="color",
    get_radius="radius",
    pickable=True,
)

st.pydeck_chart(pdk.Deck(
    layers=[layer],
    initial_view_state=pdk.ViewState(
        latitude=filtered["LAT"].mean() if len(filtered) else 56,
        longitude=filtered["LON"].mean() if len(filtered) else -96,
        zoom=4,
    ),
    tooltip=tooltip,
    map_style="dark" if dark_mode else "light",
))

# =========================
# TABLE
# =========================
st.subheader("📋 Fire Details")

st.dataframe(filtered, use_container_width=True)