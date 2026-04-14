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

# =========================
# SIDEBAR
# =========================
dark_mode = st.sidebar.toggle("Dark Mode", value=True)
size_factor = st.sidebar.slider("Bubble Size", 0.5, 3.0, 1.2, 0.1)

# =========================
# THEME
# =========================
if dark_mode:
    bg, card, sidebar, text = "#0E1117", "#1F2937", "#F59E0B", "#F59E0B"
else:
    bg, card, sidebar, text = "#F9FAFB", "#FFFFFF", "#F3F4F6", "#111827"

# =========================
# CSS
# =========================
st.markdown(f"""
<style>
.stApp {{ background-color: {bg}; color: {text}; }}
section[data-testid="stSidebar"] {{ background-color: {sidebar}; }}

.kpi-card {{
    background-color: {card};
    padding: 20px;
    border-radius: 12px;
    text-align: center;
}}
</style>
""", unsafe_allow_html=True)

# =========================
# TITLE
# =========================
st.title("🔥 Canada Active Fires Dashboard")

# =========================
# SNOWFLAKE
# =========================
@st.cache_data
def load_data():
    session = Session.builder.configs(st.secrets["snowflake"]).create()
    return session.sql("""
        SELECT FIRENAME, PROVINCE, HECTARES,
               RESPONSE_TYPE_DESCRIPTION,
               STAGE_OF_CONTROL_DESCRIPTION,
               LAT, LON
        FROM EVA.GIS.VW_ACTIVEFIRES
    """).to_pandas()

df = load_data()

# =========================
# FILTERS (WORKING AGAIN)
# =========================
col1, col2, col3 = st.columns(3)

with col1:
    prov = st.selectbox("Province", ["All"] + sorted(df["PROVINCE"].dropna().unique()))
with col2:
    resp = st.selectbox("Response", ["All"] + sorted(df["RESPONSE_TYPE_DESCRIPTION"].dropna().unique()))
with col3:
    stage = st.selectbox("Stage", ["All"] + sorted(df["STAGE_OF_CONTROL_DESCRIPTION"].dropna().unique()))

filtered = df.copy()

if prov != "All":
    filtered = filtered[filtered["PROVINCE"] == prov]
if resp != "All":
    filtered = filtered[filtered["RESPONSE_TYPE_DESCRIPTION"] == resp]
if stage != "All":
    filtered = filtered[filtered["STAGE_OF_CONTROL_DESCRIPTION"] == stage]

# =========================
# SESSION STATE
# =========================
if "selected_points" not in st.session_state:
    st.session_state.selected_points = []

# =========================
# KPI CARDS (TOP AGAIN)
# =========================
def kpi(col, label, value):
    col.markdown(f"""
    <div class="kpi-card">
        <h2>{value}</h2>
        <p>{label}</p>
    </div>
    """, unsafe_allow_html=True)

k1, k2, k3 = st.columns(3)

kpi(k1, "🔥 Fires", len(filtered))
kpi(k2, "🌲 Total Hectares", int(filtered["HECTARES"].sum()) if len(filtered) else 0)
kpi(k3, "📊 Avg Size", int(filtered["HECTARES"].mean()) if len(filtered) else 0)

st.markdown("---")

# =========================
# MAP (LASSO ENABLED)
# =========================
if len(filtered) > 0:

    filtered["size"] = np.log1p(filtered["HECTARES"]) * 5 * size_factor

    fig = px.scatter_mapbox(
        filtered,
        lat="LAT",
        lon="LON",
        size="size",
        color="STAGE_OF_CONTROL_DESCRIPTION",
        hover_name="FIRENAME",
        zoom=3,
        height=500
    )

    fig.update_layout(
        mapbox_style="carto-darkmatter" if dark_mode else "carto-positron",
        showlegend=False,
        margin=dict(l=0, r=0, t=0, b=0)
    )

    selected = plotly_events(
        fig,
        click_event=True,
        select_event=True  # 🔥 THIS ENABLES LASSO
    )

# =========================
# STORE MULTI-SELECTION
# =========================
    if selected:
        indices = [p["pointIndex"] for p in selected]
        st.session_state.selected_points = indices

# =========================
# APPLY SELECTION FILTER
# =========================
final_df = filtered.copy()

if st.session_state.selected_points:
    final_df = filtered.iloc[st.session_state.selected_points]

# =========================
# TABLE
# =========================
st.dataframe(final_df, use_container_width=True)

# =========================
# RESET
# =========================
if st.button("🔄 Reset Selection"):
    st.session_state.selected_points = []
