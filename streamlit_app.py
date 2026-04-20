"""
Madrid Food Delivery Control Tower
===================================
Run with:
    pip install -r requirements_dashboard.txt
    streamlit run streamlit_app.py
"""
import sys
from pathlib import Path

# Ensure project root is on the path so dashboard.* imports work
sys.path.insert(0, str(Path(__file__).parent))

# Load .env if present (Azure credentials, etc.)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import streamlit as st

st.set_page_config(
    page_title="Madrid Delivery Control Tower",
    page_icon="🚴",
    layout="wide",
    initial_sidebar_state="collapsed",
)

from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=1000, key="live_refresh")
# ── Minimal global CSS ────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    /* Tighter metric cards */
    [data-testid="metric-container"] {
        background: #f8f9fb;
        border: 1px solid #e8eaed;
        border-radius: 8px;
        padding: 10px 14px;
    }
    [data-testid="metric-container"] label {
        font-size: 0.72rem;
        color: #666;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.4rem !important;
        font-weight: 600;
        color: #1B4079;
    }
    /* Section dividers */
    hr { margin: 1.2rem 0 0.6rem; border-color: #e8eaed; }
    /* Chart container */
    .stPlotlyChart { border-radius: 6px; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🚴 Madrid Delivery Control Tower")

# ── Render dashboard ──────────────────────────────────────────────────────────
from dashboard.ui.control_tower import render
render()
