"""Courier metric computations."""
from __future__ import annotations

import pandas as pd

from dashboard.config.constants import TIME_BUCKET_MINUTES


# ── KPIs ─────────────────────────────────────────────────────────────────────

def compute_courier_kpis(df: pd.DataFrame) -> dict:
    """Top-level courier KPIs from courier events."""
    if df.empty:
        return {"active_couriers": 0, "online_couriers": 0, "deliveries_completed": 0, "utilization_rate": None}

    active = df[df["event_type"] == "courier_assigned_order"]["courier_id"].nunique()
    online = df[df["event_type"] == "courier_online"]["courier_id"].nunique()
    # Fall back: use all distinct couriers seen if no explicit online events
    if online == 0:
        online = df["courier_id"].dropna().nunique()
    deliveries = df[df["event_type"] == "courier_delivery_completed"]["courier_id"].count()

    # Utilization: deliveries per unique active courier
    utilization = round(deliveries / active, 2) if active > 0 else None

    return {
        "active_couriers": int(active),
        "online_couriers": int(online),
        "deliveries_completed": int(deliveries),
        "utilization_rate": utilization,
    }


# ── Time-series ───────────────────────────────────────────────────────────────

def compute_courier_activity(df: pd.DataFrame) -> pd.DataFrame:
    """Active and online courier counts per time bucket."""
    if df.empty:
        return pd.DataFrame(columns=["bucket", "active_couriers", "online_couriers"])

    freq = f"{TIME_BUCKET_MINUTES}min"
    assigned = df[df["event_type"] == "courier_assigned_order"].copy()
    online_df = df[df["event_type"] == "courier_online"].copy()

    series: dict[str, pd.Series] = {}

    if not assigned.empty:
        assigned["bucket"] = assigned["event_time"].dt.floor(freq)
        series["active_couriers"] = assigned.groupby("bucket")["courier_id"].nunique()

    if not online_df.empty:
        online_df["bucket"] = online_df["event_time"].dt.floor(freq)
        series["online_couriers"] = online_df.groupby("bucket")["courier_id"].nunique()

    if not series:
        return pd.DataFrame(columns=["bucket", "active_couriers", "online_couriers"])

    all_idx = None
    for s in series.values():
        all_idx = s.index if all_idx is None else all_idx.union(s.index)

    result = pd.DataFrame(index=all_idx)
    for k, s in series.items():
        result[k] = s
    result = result.fillna(0).astype(int).reset_index().rename(columns={"index": "bucket"})

    if "active_couriers" not in result.columns:
        result["active_couriers"] = 0
    if "online_couriers" not in result.columns:
        result["online_couriers"] = 0

    return result.sort_values("bucket").reset_index(drop=True)


# ── Supply / Demand ───────────────────────────────────────────────────────────

def compute_zone_courier_demand(order_df: pd.DataFrame, courier_df: pd.DataFrame) -> pd.DataFrame:
    """
    Zone × time-bucket demand/courier ratio for the stress heatmap.
    demand = orders created in zone, supply = active couriers in zone.
    """
    if order_df.empty:
        return pd.DataFrame()

    freq = f"{TIME_BUCKET_MINUTES}min"
    created = order_df[order_df["event_type"] == "order_created"].copy()
    if created.empty:
        return pd.DataFrame()

    created["bucket"] = created["event_time"].dt.floor(freq)
    demand = (
        created.groupby(["bucket", "user_zone_id"])["order_id"]
        .count()
        .reset_index()
        .rename(columns={"user_zone_id": "zone_id", "order_id": "orders"})
    )

    if not courier_df.empty:
        assigned = courier_df[courier_df["event_type"] == "courier_assigned_order"].copy()
        if not assigned.empty:
            assigned["bucket"] = assigned["event_time"].dt.floor(freq)
            supply = (
                assigned.groupby(["bucket", "courier_zone_id"])["courier_id"]
                .nunique()
                .reset_index()
                .rename(columns={"courier_zone_id": "zone_id", "courier_id": "couriers"})
            )
            merged = demand.merge(supply, on=["bucket", "zone_id"], how="outer").fillna(0)
        else:
            merged = demand.copy()
            merged["couriers"] = 0
    else:
        merged = demand.copy()
        merged["couriers"] = 0

    merged["demand_courier_ratio"] = merged["orders"] / merged["couriers"].clip(lower=0.1)
    return merged
