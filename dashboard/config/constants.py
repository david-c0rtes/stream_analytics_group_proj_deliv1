"""Dashboard configuration constants — single source of truth for colors, SLA, revenue, and geography."""
from __future__ import annotations
from typing import Dict

# ── Color Palette ──────────────────────────────────────────────────────────────
COLORS: Dict[str, str] = {
    "orders_created": "#8FAD88",
    "orders_delivered": "#4D7C8A",
    "reference_lines": "#7F9C96",
    "active_orders": "#1B4079",
    "funnel_bars": "#1B4079",
    "heatmap_low": "#CBDF90",
    "heatmap_high": "#1B4079",
    "cancellation_user": "#E07B54",
    "cancellation_restaurant": "#C0392B",
    "cancellation_system": "#95A5A6",
    "prep_time": "#E8C547",
    "travel_time": "#4D7C8A",
    "courier_active": "#1B4079",
    "courier_online": "#4D7C8A",
    "gmv": "#4D7C8A",
}

# ── Revenue ───────────────────────────────────────────────────────────────────
# Assumed average order value for GMV estimation.
# Replace with actual order value data when available.
DEFAULT_AVG_ORDER_VALUE_EUR: float = 18.0

# Hours in a typical delivery day (used for EOD run-rate projection)
DELIVERY_DAY_HOURS: float = 16.0

# ── SLA ───────────────────────────────────────────────────────────────────────
SLA_DELIVERY_MINUTES: float = 45.0       # orders under this threshold are on-time
ON_TIME_SLA_TARGET_PCT: float = 80.0    # operational target for on-time rate

# ── Dashboard behaviour ───────────────────────────────────────────────────────
REFRESH_INTERVAL_SECONDS: int = 1
TIME_BUCKET_MINUTES: int = 15

# ── Madrid District WGS84 Centroids (Z01–Z21) ────────────────────────────────
# Real geographic centroids for Madrid's 21 administrative districts.
# Source: Madrid Open Data / own calculation. Easy to update — one dict, one place.
MADRID_ZONE_COORDS: Dict[str, dict] = {
    "Z01": {"lat": 40.4130, "lon": -3.7076, "name": "Centro"},
    "Z02": {"lat": 40.3940, "lon": -3.7050, "name": "Arganzuela"},
    "Z03": {"lat": 40.4070, "lon": -3.6830, "name": "Retiro"},
    "Z04": {"lat": 40.4230, "lon": -3.6790, "name": "Salamanca"},
    "Z05": {"lat": 40.4590, "lon": -3.6780, "name": "Chamartín"},
    "Z06": {"lat": 40.4620, "lon": -3.6960, "name": "Tetuán"},
    "Z07": {"lat": 40.4350, "lon": -3.7010, "name": "Chamberí"},
    "Z08": {"lat": 40.5010, "lon": -3.7050, "name": "Fuencarral-El Pardo"},
    "Z09": {"lat": 40.4450, "lon": -3.7430, "name": "Moncloa-Aravaca"},
    "Z10": {"lat": 40.3950, "lon": -3.7380, "name": "Latina"},
    "Z11": {"lat": 40.3740, "lon": -3.7270, "name": "Carabanchel"},
    "Z12": {"lat": 40.3830, "lon": -3.7130, "name": "Usera"},
    "Z13": {"lat": 40.3810, "lon": -3.6760, "name": "Puente de Vallecas"},
    "Z14": {"lat": 40.4000, "lon": -3.6550, "name": "Moratalaz"},
    "Z15": {"lat": 40.4380, "lon": -3.6430, "name": "Ciudad Lineal"},
    "Z16": {"lat": 40.4800, "lon": -3.6430, "name": "Hortaleza"},
    "Z17": {"lat": 40.3490, "lon": -3.7010, "name": "Villaverde"},
    "Z18": {"lat": 40.3600, "lon": -3.6490, "name": "Villa de Vallecas"},
    "Z19": {"lat": 40.3830, "lon": -3.6270, "name": "Vicálvaro"},
    "Z20": {"lat": 40.4310, "lon": -3.6290, "name": "San Blas-Canillejas"},
    "Z21": {"lat": 40.4770, "lon": -3.5870, "name": "Barajas"},
}
