import streamlit as st
import pydeck as pdk
import pandas as pd
from snowflake.snowpark import Session

# =========================
# PAGE CONFIG
# =========================
st.set_page_config(
    page_title="Canada Active Fires Dashboard",
    layout="wide"
)

# =========================
# DARK MODE TOGGLE
# =========================
st.sidebar.header("🎨 Appearance")

dark_mode = st.sidebar.toggle("Dark Mode", value=False)

if dark_mode:
    st.markdown("""
        <style>
        .stApp {
            background-color: #0e1117;
            color: white;
        }
        </style>
    """, unsafe_allow_html=True)

st.header("🔥 Canada Active Fires Dashboard")

# =========================
# SNOWFLAKE CONNECTION
# =========================
def get_snowflake_session():
    return Session.builder.configs(
        st.secrets["snowflake"]
    ).create()

# =========================
# LOAD DATA (SNOWFLAKE ONLY)
# =========================
@st.cache_data
def load_data():
    session = get_snowflake_session()
    return session.sql("SELECT * FROM EVA.GIS.VW_ACTIVEFIRES").to_pandas()

df = load_data()

# =========================
# SIDEBAR FILTERS (DROPDOWNS)
# =========================
st.sidebar.header("🔎 Filters")

# Province dropdown
provinces = ["All"] + sorted(df["PROVINCE"].dropna().unique())
selected_province = st.sidebar.selectbox("Province", provinces)

# Response dropdown
responses = ["All"] + sorted(df["RESPONSE_TYPE_DESCRIPTION"].dropna().unique())
selected_response = st.sidebar.selectbox("Response Type", responses)

# Stage dropdown
stages = ["All"] + sorted(df["STAGE_OF_CONTROL_DESCRIPTION"].dropna().unique())
selected_stage = st.sidebar.selectbox("Stage of Control", stages)

# =========================
# APPLY FILTERS
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
total_hectares = int(filtered["HECTARES"].sum()) if total_fires > 0 else 0
avg_size = int(filtered["HECTARES"].mean()) if total_fires > 0 else 0

k1, k2, k3 = st.columns(3)

k1.metric("🔥 Active Fires", total_fires)
k2.metric("🌲 Total Hectares", f"{total_hectares:,}")
k3.metric("📊 Avg Fire Size", f"{avg_size:,}")

st.markdown("---")

# =========================
# MAP PREP
# =========================
COLOR_MAP = {
    "Out of Control": [220, 38, 38, 200],
    "Being Held": [245, 158, 11, 200],
    "Under Control": [34, 139, 34, 200],
    "Extinguished": [156, 163, 175, 200],
    "Unstaffed and Uncontained": [239, 68, 68, 200],
    "Unstaffed and Contained": [251, 191, 36, 200],
}
# DEFAULT_COLOR = [156, 163, 175, 200]
DEFAULT_COLOR = [240, 242, 246]

filtered = filtered.copy()
filtered["color"] = filtered["STAGE_OF_CONTROL_DESCRIPTION"].map(
    lambda x: COLOR_MAP.get(x, DEFAULT_COLOR)
)

# Radius scaling
min_radius, max_radius = 5000, 50000
ha_min = filtered["HECTARES"].min() if total_fires > 0 else 0
ha_max = filtered["HECTARES"].max() if total_fires > 0 else 1
ha_range = ha_max - ha_min if ha_max > ha_min else 1

filtered["radius"] = filtered["HECTARES"].apply(
    lambda h: min_radius + (max_radius - min_radius) * ((h - ha_min) / ha_range)
)

# =========================
# MAP VIEW
# =========================
if total_fires > 0:
    lat_center = filtered["LAT"].mean()
    lon_center = filtered["LON"].mean()
else:
    lat_center, lon_center = 56.0, -96.0

view_state = pdk.ViewState(
    latitude=lat_center,
    longitude=lon_center,
    zoom=4,
)

PROVINCES_GEOJSON = "https://raw.githubusercontent.com/codeforgermany/click_that_hood/main/public/data/canada.geojson"

boundary_layer = pdk.Layer(
    "GeoJsonLayer",
    data=PROVINCES_GEOJSON,
    stroked=True,
    filled=False,
    get_line_color=[120, 120, 120, 120],
    get_line_width=1,
)

fire_layer = pdk.Layer(
    "ScatterplotLayer",
    data=filtered,
    get_position=["LON", "LAT"],
    get_color="color",
    get_radius="radius",
    pickable=True,
)

tooltip = {
    "html": """
    <b>{FIRENAME}</b><br/>
    Province: {PROVINCE}<br/>
    Hectares: {HECTARES}<br/>
    Response: {RESPONSE_TYPE_DESCRIPTION}<br/>
    Stage: {STAGE_OF_CONTROL_DESCRIPTION}<br/>
    Days Active: {DAYS_ACTIVE}
    """,
    "style": {"backgroundColor": "#222", "color": "white"},
}

# =========================
# MAP
# =========================
st.subheader("🗺️ Fire Map")

st.pydeck_chart(pdk.Deck(
    layers=[boundary_layer, fire_layer],
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