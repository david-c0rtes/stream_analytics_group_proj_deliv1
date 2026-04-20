"""
Estimated revenue / GMV computations.

All outputs are clearly labelled as *estimated* — they use a configurable
assumed average order value (DEFAULT_AVG_ORDER_VALUE_EUR) and count only
delivered orders (conservative GMV).  Structure intentionally allows swap-in
of actual per-order values from the event stream in future.
"""
from __future__ import annotations

import pandas as pd

from dashboard.config.constants import (
    DEFAULT_AVG_ORDER_VALUE_EUR,
    DELIVERY_DAY_HOURS,
    TIME_BUCKET_MINUTES,
)


def compute_gmv(
    df: pd.DataFrame,
    avg_order_value: float = DEFAULT_AVG_ORDER_VALUE_EUR,
) -> dict:
    """
    Top-level GMV KPIs:
      - estimated_gmv_eur        Total estimated GMV from delivered orders
      - hourly_run_rate_eur      GMV per elapsed hour
      - eod_projection_eur       If current pace holds all day
    """
    empty = {"estimated_gmv_eur": 0.0, "hourly_run_rate_eur": 0.0, "eod_projection_eur": 0.0}
    if df.empty:
        return empty

    delivered_df = df[df["event_type"] == "order_delivered"]
    delivered_count = len(delivered_df)
    estimated_gmv = delivered_count * avg_order_value

    hourly_rate = 0.0
    eod = 0.0
    if not delivered_df.empty:
        t_min = delivered_df["event_time"].min()
        t_max = delivered_df["event_time"].max()
        elapsed_hours = max((t_max - t_min).total_seconds() / 3600.0, 0.1)
        hourly_rate = estimated_gmv / elapsed_hours
        eod = hourly_rate * DELIVERY_DAY_HOURS

    return {
        "estimated_gmv_eur": round(estimated_gmv, 2),
        "hourly_run_rate_eur": round(hourly_rate, 2),
        "eod_projection_eur": round(eod, 2),
    }


def compute_gmv_by_zone(
    df: pd.DataFrame,
    avg_order_value: float = DEFAULT_AVG_ORDER_VALUE_EUR,
) -> pd.DataFrame:
    """Estimated GMV per zone from delivered orders."""
    if df.empty:
        return pd.DataFrame(columns=["zone_id", "delivered_orders", "estimated_gmv_eur"])

    delivered_df = df[df["event_type"] == "order_delivered"]
    if delivered_df.empty:
        return pd.DataFrame(columns=["zone_id", "delivered_orders", "estimated_gmv_eur"])

    result = delivered_df.groupby("user_zone_id")["order_id"].count().reset_index()
    result.columns = ["zone_id", "delivered_orders"]
    result["estimated_gmv_eur"] = (result["delivered_orders"] * avg_order_value).round(2)
    return result.sort_values("estimated_gmv_eur", ascending=False).reset_index(drop=True)


def compute_gmv_over_time(
    df: pd.DataFrame,
    avg_order_value: float = DEFAULT_AVG_ORDER_VALUE_EUR,
) -> pd.DataFrame:
    """Estimated GMV per time bucket (from delivered orders)."""
    if df.empty:
        return pd.DataFrame(columns=["bucket", "delivered_orders", "estimated_gmv_eur"])

    freq = f"{TIME_BUCKET_MINUTES}min"
    delivered_df = df[df["event_type"] == "order_delivered"].copy()
    if delivered_df.empty:
        return pd.DataFrame(columns=["bucket", "delivered_orders", "estimated_gmv_eur"])

    delivered_df["bucket"] = delivered_df["event_time"].dt.floor(freq)
    result = delivered_df.groupby("bucket")["order_id"].count().reset_index()
    result.columns = ["bucket", "delivered_orders"]
    result["estimated_gmv_eur"] = (result["delivered_orders"] * avg_order_value).round(2)
    return result
