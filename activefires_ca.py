import streamlit as st
import pydeck as pdk
import pandas as pd
import numpy as np
from snowflake.snowpark import Session

# =========================
# PAGE CONFIG
# =========================
st.set_page_config(
    page_title="Canada Active Fires Dashboard",
    layout="wide"
)

# =========================
# SIDEBAR CONTROLS
# =========================
st.sidebar.header("🎨 Appearance")
dark_mode = st.sidebar.toggle("Dark Mode", value=True)

st.sidebar.header("🗺️ Map Controls")
size_factor = st.sidebar.slider("Bubble Size", 0.5, 3.0, 1.2, 0.1)

# =========================
# THEME COLORS
# =========================
if dark_mode:
    bg = "#0E1117"
    card = "#1F2937"
    sidebar = "#F59E0B"
    text = "#F59E0B"
    filter_text = "#F59E0B"
else:
    bg = "#F9FAFB"
    card = "#FFFFFF"
    sidebar = "#F3F4F6"
    text = "#111827"
    filter_text = "black"

# =========================
# GLOBAL CSS
# =========================
st.markdown(f"""
<style>
.stApp {{ background-color: {bg}; color: {text}; }}

h1 {{ margin-bottom: 10px; }}

section[data-testid="stSidebar"] {{
    background-color: {sidebar};
}}

.top-bar {{
    background-color: {card};
    padding: 15px;
    border-radius: 12px;
    margin-bottom: 15px;
}}

.kpi-card {{
    background-color: {card};
    padding: 20px;
    border-radius: 12px;
    text-align: center;
}}

.kpi-value {{
    font-size: 28px;
    font-weight: bold;
}}

.kpi-label {{
    font-size: 14px;
    opacity: 0.7;
}}

.stDataFrame {{
    background-color: {card} !important;
}}

.stDataFrame div {{
    color: {text} !important;
}}

div[data-baseweb="select"] {{
    background-color: {card} !important;
    border-radius: 8px;
}}

div[data-baseweb="select"] * {{
    color: {text} !important;
}}
</style>
""", unsafe_allow_html=True)

# =========================
# TITLE
# =========================
st.title("🔥 Canada Active Fires Dashboard")
st.markdown("<br>", unsafe_allow_html=True)

# =========================
# SNOWFLAKE CONNECTION
# =========================
def get_snowflake_session():
    return Session.builder.configs(
        st.secrets["snowflake"]
    ).create()

@st.cache_data
def load_data():
    session = get_snowflake_session()
    return session.sql("SELECT * FROM EVA.GIS.VW_ACTIVEFIRES").to_pandas()

df = load_data()

# =========================
# FILTER BAR
# =========================
st.markdown('<div class="top-bar">', unsafe_allow_html=True)

c1, c2, c3 = st.columns(3)

with c1:
    provinces = ["All"] + sorted(df["PROVINCE"].dropna().unique())
    selected_province = st.selectbox("Province", provinces)

with c2:
    responses = ["All"] + sorted(df["RESPONSE_TYPE_DESCRIPTION"].dropna().unique())
    selected_response = st.selectbox("Response Type", responses)

with c3:
    stages = ["All"] + sorted(df["STAGE_OF_CONTROL_DESCRIPTION"].dropna().unique())
    selected_stage = st.selectbox("Stage of Control", stages)

st.markdown('</div>', unsafe_allow_html=True)

# =========================
# FILTER DATA
# =========================
filtered = df.copy()

if selected_province != "All":
    filtered = filtered[filtered["PROVINCE"] == selected_province]

if selected_response != "All":
    filtered = filtered[filtered["RESPONSE_TYPE_DESCRIPTION"] == selected_response]

if selected_stage != "All":
    filtered = filtered[filtered["STAGE_OF_CONTROL_DESCRIPTION"] == selected_stage]

# =========================
# KPIs
# =========================
total_fires = len(filtered)
total_hectares = int(filtered["HECTARES"].sum()) if total_fires else 0
avg_size = int(filtered["HECTARES"].mean()) if total_fires else 0

k1, k2, k3 = st.columns(3)

def kpi(col, label, value, color):
    col.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-value" style="color:{color}">{value}</div>
        <div class="kpi-label">{label}</div>
    </div>
    """, unsafe_allow_html=True)

kpi(k1, "🔥 Active Fires", total_fires, "#EF4444")
kpi(k2, "🌲 Total Hectares", f"{total_hectares:,}", "#F59E0B")
kpi(k3, "📊 Avg Fire Size", f"{avg_size:,}", "#10B981")

st.markdown("---")

# =========================
# MAP PREP
# =========================
COLOR_MAP = {
    "Out of Control": [220, 38, 38, 200],
    "Being Held": [245, 158, 11, 200],
    "Under Control": [34, 139, 34, 200],
    "Extinguished": [156, 163, 175, 200],
}

DEFAULT_COLOR = [200, 200, 200, 180]

filtered["color"] = filtered["STAGE_OF_CONTROL_DESCRIPTION"].map(
    lambda x: COLOR_MAP.get(x, DEFAULT_COLOR)
)

# =========================
# RADIUS SCALING (LOG + SLIDER)
# =========================
min_radius = 2000 * size_factor
max_radius = 20000 * size_factor

if total_fires > 0:
    log_hectares = np.log1p(filtered["HECTARES"].fillna(1))

    h_min = log_hectares.min()
    h_max = log_hectares.max()
    h_range = h_max - h_min if h_max > h_min else 1

    filtered["radius"] = log_hectares.apply(
        lambda h: min_radius + (max_radius - min_radius) * ((h - h_min) / h_range)
    )
else:
    filtered["radius"] = min_radius

# =========================
# MAP
# =========================
st.subheader("🗺️ Fire Map")

view_state = pdk.ViewState(
    latitude=filtered["LAT"].mean() if total_fires else 56,
    longitude=filtered["LON"].mean() if total_fires else -96,
    zoom=4,
)

layer = pdk.Layer(
    "ScatterplotLayer",
    data=filtered,
    get_position=["LON", "LAT"],
    get_color="color",
    get_radius="radius",
    pickable=True,
)

# Legend
st.markdown(f"""
<div style="background:{card}; padding:10px; border-radius:10px; margin-bottom:10px">
<b>Legend:</b>
<span style="color:#EF4444">● Out of Control</span> |
<span style="color:#F59E0B">● Being Held</span> |
<span style="color:#22C55E">● Under Control</span> |
<span style="color:#9CA3AF">● Extinguished</span>
</div>
""", unsafe_allow_html=True)

tooltip = { 
    "html": """ 
    <b>{FIRENAME}</b>
    <br/> Province: {PROVINCE}
    <br/> Hectares: {HECTARES}
    <br/> Response: {RESPONSE_TYPE_DESCRIPTION}
    <br/> Stage: {STAGE_OF_CONTROL_DESCRIPTION}
    <br/> Days Active: {DAYS_ACTIVE} 
    """, 
    "style": {"backgroundColor": "#222", "color": "white"}, 
}

st.pydeck_chart(pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip=tooltip,
    map_style="dark" if dark_mode else "light",
))

# =========================
# TABLE
# =========================
st.subheader("📋 Fire Details")

st.dataframe(
    filtered[[
        "FIRENAME",
        "PROVINCE",
        "HECTARES",
        "RESPONSE_TYPE_DESCRIPTION",
        "STAGE_OF_CONTROL_DESCRIPTION",
        "STARTDATE",
        "DAYS_ACTIVE"
    ]],
    use_container_width=True,
)
