import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
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
# CSS
# =========================
st.markdown(f"""
<style>
.stApp {{ background-color: {bg}; color: {text}; }}
section[data-testid="stSidebar"] {{ background-color: {sidebar}; }}

.top-bar {{
    position: sticky; top: 0; z-index: 999;
    background-color: {card};
    padding: 15px; border-radius: 12px;
}}

.kpi-card {{
    background-color: {card};
    padding: 20px;
    border-radius: 12px;
    text-align: center;
}}

.kpi-value {{
    font-size: 26px;
    font-weight: bold;
}}

.kpi-label {{
    font-size: 14px;
    opacity: 0.7;
}}

div[data-baseweb="select"] * {{
    color: {filter_text} !important;
}}
</style>
""", unsafe_allow_html=True)

# =========================
# TITLE
# =========================
st.title("🔥 Canada Active Fires Dashboard")
st.markdown("<br>", unsafe_allow_html=True)

# =========================
# SNOWFLAKE
# =========================
def get_session():
    return Session.builder.configs(st.secrets["snowflake"]).create()

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
# KPI CARDS (INSIGHTS)
# =========================
total_fires = len(filtered)
total_hectares = int(filtered["HECTARES"].sum()) if total_fires else 0
avg_size = int(filtered["HECTARES"].mean()) if total_fires else 0

largest_fire = filtered.loc[filtered["HECTARES"].idxmax()]["FIRENAME"] if total_fires else "-"
top_province = filtered["PROVINCE"].value_counts().idxmax() if total_fires else "-"

k1, k2, k3, k4 = st.columns(4)

def kpi(col, label, value):
    col.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
    </div>
    """, unsafe_allow_html=True)

kpi(k1, "🔥 Fires", total_fires)
kpi(k2, "🌲 Hectares", f"{total_hectares:,}")
kpi(k3, "📊 Avg Size", f"{avg_size:,}")
kpi(k4, "🏆 Largest Fire", largest_fire)

st.markdown("---")

# =========================
# MAP (PLOTLY CLICKABLE)
# =========================
st.subheader("🗺️ Fire Map")

if total_fires > 0:
    filtered["size"] = np.log1p(filtered["HECTARES"]) * 5 * size_factor

    fig = px.scatter_mapbox(
        filtered,
        lat="LAT",
        lon="LON",
        size="size",
        color="STAGE_OF_CONTROL_DESCRIPTION",
        hover_name="FIRENAME",
        hover_data=["PROVINCE", "HECTARES", "DAYS_ACTIVE"],
        zoom=3,
        height=500
    )

    fig.update_layout(
        mapbox_style="carto-darkmatter" if dark_mode else "carto-positron",
        margin={"r":0,"t":0,"l":0,"b":0}
    )

    selected = st.plotly_chart(fig, use_container_width=True)

# =========================
# CLICK FILTER STATE
# =========================
if "selected_fire" not in st.session_state:
    st.session_state.selected_fire = None

# NOTE: Streamlit limitation → workaround with selectbox
selected_fire = st.selectbox(
    "🔍 Select fire (click alternative)",
    ["None"] + filtered["FIRENAME"].dropna().unique().tolist()
)

if selected_fire != "None":
    st.session_state.selected_fire = selected_fire

# =========================
# TABLE
# =========================
st.subheader("📋 Fire Details")

table_df = filtered.copy()

if st.session_state.selected_fire:
    table_df = table_df[
        table_df["FIRENAME"] == st.session_state.selected_fire
    ]
    st.info(f"Filtered: {st.session_state.selected_fire}")

st.dataframe(table_df, use_container_width=True)