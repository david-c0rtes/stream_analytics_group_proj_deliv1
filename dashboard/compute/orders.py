"""Order metric computations — operates on the raw events DataFrame."""
from __future__ import annotations

import pandas as pd

from dashboard.config.constants import SLA_DELIVERY_MINUTES, TIME_BUCKET_MINUTES

_CANCEL_TYPES = [
    "order_cancelled_user",
    "order_cancelled_restaurant",
]
_TERMINAL_TYPES = ["order_delivered"] + _CANCEL_TYPES


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_numeric(series: pd.Series, min_val: float = 0.0) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return s[s > min_val]


def _join_created_delivered(df: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with created_at, delivered_at, full_delivery_min per order."""
    created = (
        df[df["event_type"] == "order_created"][["order_id", "event_time", "user_zone_id"]]
        .rename(columns={"event_time": "created_at"})
    )
    delivered = (
        df[df["event_type"] == "order_delivered"][["order_id", "event_time"]]
        .rename(columns={"event_time": "delivered_at"})
    )
    if created.empty or delivered.empty:
        return pd.DataFrame()
    joined = created.merge(delivered, on="order_id", how="inner")
    joined["full_delivery_min"] = (
        joined["delivered_at"] - joined["created_at"]
    ).dt.total_seconds() / 60.0
    return joined[joined["full_delivery_min"] > 0]


# ── KPIs ─────────────────────────────────────────────────────────────────────

def compute_order_kpis(df: pd.DataFrame) -> dict:
    """Top-level order KPIs."""
    empty = {
        "total_created": 0,
        "total_delivered": 0,
        "created_today": 0,
        "delivered_today": 0,
        "total_cancelled": 0,
        "cancellation_rate_pct": 0.0,
        "orders_in_flight": 0,
        "avg_prep_time_min": None,
        "p95_prep_time_min": None,
        "avg_travel_time_min": None,
        "p95_travel_time_min": None,
        "avg_full_delivery_min": None,
        "p95_delivery_min": None,
        "on_time_rate_pct": None,
    }
    if df.empty:
        return empty

    created_events = df[df["event_type"] == "order_created"]
    # Use the latest created-order timestamp as a stable "now" watermark.
    # This avoids counting far-future terminal events in real-time replay modes.
    if not created_events.empty:
        ref_time = created_events["event_time"].max()
        df_snapshot = df[df["event_time"] <= ref_time].copy()
    else:
        ref_time = df["event_time"].max()
        df_snapshot = df.copy()

    created_ids = set(df_snapshot[df_snapshot["event_type"] == "order_created"]["order_id"])
    terminal_ids = set(df_snapshot[df_snapshot["event_type"].isin(_TERMINAL_TYPES)]["order_id"])
    cancelled_ids = set(df_snapshot[df_snapshot["event_type"].isin(_CANCEL_TYPES)]["order_id"])

    total_created = len(created_ids)
    total_delivered = df_snapshot[df_snapshot["event_type"] == "order_delivered"]["order_id"].nunique()
    total_cancelled = len(cancelled_ids)
    in_flight = len(created_ids - terminal_ids)
    cancel_rate = (total_cancelled / total_created * 100) if total_created > 0 else 0.0
    # "Today" is based on the current simulation watermark day.
    latest_day = ref_time.normalize()
    created_today = df_snapshot[
        (df_snapshot["event_type"] == "order_created") & (df_snapshot["event_time"].dt.normalize() == latest_day)
    ]["order_id"].nunique()
    delivered_today = df_snapshot[
        (df_snapshot["event_type"] == "order_delivered") & (df_snapshot["event_time"].dt.normalize() == latest_day)
    ]["order_id"].nunique()

    delivered_df = df_snapshot[df_snapshot["event_type"] == "order_delivered"]
    avg_prep = p95_prep = avg_travel = p95_travel = avg_full = p95_full = on_time = None

    if not delivered_df.empty:
        prep = _safe_numeric(delivered_df.get("actual_prep_time", pd.Series(dtype=float)))
        if not prep.empty:
            avg_prep = round(prep.mean(), 1)
            p95_prep = round(prep.quantile(0.95), 1)

        travel = _safe_numeric(delivered_df.get("actual_delivery_time", pd.Series(dtype=float)))
        if not travel.empty:
            avg_travel = round(travel.mean(), 1)
            p95_travel = round(travel.quantile(0.95), 1)

    joined = _join_created_delivered(df_snapshot)
    if not joined.empty:
        vals = joined["full_delivery_min"]
        avg_full = round(vals.mean(), 1)
        p95_full = round(vals.quantile(0.95), 1)
        on_time = round((vals <= SLA_DELIVERY_MINUTES).mean() * 100, 1)

    return {
        "total_created": total_created,
        "total_delivered": total_delivered,
        "created_today": created_today,
        "delivered_today": delivered_today,
        "total_cancelled": total_cancelled,
        "cancellation_rate_pct": round(cancel_rate, 1),
        "orders_in_flight": in_flight,
        "avg_prep_time_min": avg_prep,
        "p95_prep_time_min": p95_prep,
        "avg_travel_time_min": avg_travel,
        "p95_travel_time_min": p95_travel,
        "avg_full_delivery_min": avg_full,
        "p95_delivery_min": p95_full,
        "on_time_rate_pct": on_time,
    }


# ── Time-series ───────────────────────────────────────────────────────────────

def compute_orders_over_time(df: pd.DataFrame) -> pd.DataFrame:
    """Orders created / delivered / active per time bucket."""
    if df.empty:
        return pd.DataFrame(columns=["bucket", "orders_created", "orders_delivered", "active_orders"])

    freq = f"{TIME_BUCKET_MINUTES}min"
    created = df[df["event_type"] == "order_created"].copy()
    delivered = df[df["event_type"] == "order_delivered"].copy()

    if created.empty:
        return pd.DataFrame(columns=["bucket", "orders_created", "orders_delivered", "active_orders"])

    created["bucket"] = created["event_time"].dt.floor(freq)
    delivered["bucket"] = delivered["event_time"].dt.floor(freq)

    c = created.groupby("bucket")["order_id"].count().rename("orders_created")
    d = delivered.groupby("bucket")["order_id"].count().rename("orders_delivered")

    idx = pd.date_range(created["bucket"].min(), created["bucket"].max(), freq=freq, tz="UTC")
    result = pd.DataFrame(index=idx)
    result.index.name = "bucket"
    result = result.join(c).join(d).fillna(0).reset_index()
    result[["orders_created", "orders_delivered"]] = result[["orders_created", "orders_delivered"]].astype(int)
    result["active_orders"] = (result["orders_created"] - result["orders_delivered"]).cumsum().clip(lower=0)
    return result


def compute_on_time_trend(df: pd.DataFrame) -> pd.DataFrame:
    """On-time delivery rate per time bucket."""
    joined = _join_created_delivered(df)
    if joined.empty:
        return pd.DataFrame(columns=["bucket", "on_time_rate_pct"])

    freq = f"{TIME_BUCKET_MINUTES}min"
    joined["on_time"] = joined["full_delivery_min"] <= SLA_DELIVERY_MINUTES
    joined["bucket"] = joined["delivered_at"].dt.floor(freq)
    trend = joined.groupby("bucket").agg(on_time_rate_pct=("on_time", lambda x: x.mean() * 100)).reset_index()
    return trend


def compute_delivery_percentile_trend(df: pd.DataFrame) -> pd.DataFrame:
    """P50 and P95 full delivery time per time bucket."""
    joined = _join_created_delivered(df)
    if joined.empty:
        return pd.DataFrame(columns=["bucket", "p50", "p95"])

    freq = f"{TIME_BUCKET_MINUTES}min"
    joined["bucket"] = joined["delivered_at"].dt.floor(freq)
    result = (
        joined.groupby("bucket")["full_delivery_min"]
        .agg(p50=lambda x: x.quantile(0.50), p95=lambda x: x.quantile(0.95))
        .reset_index()
    )
    return result


def compute_cancellation_mix(df: pd.DataFrame) -> pd.DataFrame:
    """Cancellation counts by type per time bucket."""
    if df.empty:
        return pd.DataFrame()

    freq = f"{TIME_BUCKET_MINUTES}min"
    cancel_df = df[df["event_type"].isin(_CANCEL_TYPES)].copy()
    if cancel_df.empty:
        return pd.DataFrame()

    cancel_df["bucket"] = cancel_df["event_time"].dt.floor(freq)
    result = (
        cancel_df.groupby(["bucket", "event_type"])["order_id"]
        .count()
        .unstack(fill_value=0)
    )
    result.columns.name = None
    for col in _CANCEL_TYPES:
        if col not in result.columns:
            result[col] = 0
    return result.reset_index()


# ── Zone metrics ──────────────────────────────────────────────────────────────

def compute_zone_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Per-zone order counts and cancellation rate."""
    if df.empty:
        return pd.DataFrame(columns=["zone_id", "total_orders", "delivered_orders", "cancellation_rate"])

    created = df[df["event_type"] == "order_created"]
    delivered_df = df[df["event_type"] == "order_delivered"]
    cancelled_df = df[df["event_type"].isin(_CANCEL_TYPES)]

    zone_created = created.groupby("user_zone_id")["order_id"].count().rename("total_orders")
    zone_delivered = delivered_df.groupby("user_zone_id")["order_id"].count().rename("delivered_orders")
    zone_cancelled = cancelled_df.groupby("user_zone_id")["order_id"].count().rename("cancelled_orders")

    result = (
        pd.DataFrame(zone_created)
        .join(zone_delivered, how="outer")
        .join(zone_cancelled, how="outer")
        .fillna(0)
    )
    result.index.name = "zone_id"
    result = result.reset_index()
    result["cancellation_rate"] = (result["cancelled_orders"] / result["total_orders"].clip(lower=1) * 100).round(1)
    return result


def compute_full_delivery_by_zone(df: pd.DataFrame) -> pd.DataFrame:
    """Average full delivery time per zone (slowest first)."""
    joined = _join_created_delivered(df)
    if joined.empty:
        return pd.DataFrame(columns=["zone_id", "avg_full_delivery_min"])

    result = (
        joined.groupby("user_zone_id")["full_delivery_min"]
        .mean()
        .round(1)
        .reset_index()
        .rename(columns={"user_zone_id": "zone_id", "full_delivery_min": "avg_full_delivery_min"})
        .sort_values("avg_full_delivery_min", ascending=False)
    )
    return result


# ── Bottleneck stage times ────────────────────────────────────────────────────

def compute_stage_times(df: pd.DataFrame) -> dict[str, float]:
    """
    Average time spent in each delivery stage:
      Wait for Confirm → Prep → Wait for Courier → Travel
    Returns an ordered dict; missing stages are omitted (not zero-filled).
    """
    if df.empty:
        return {}

    stages: dict[str, float] = {}

    def _avg_gap(type_a: str, type_b: str) -> float | None:
        ta = df[df["event_type"] == type_a][["order_id", "event_time"]].rename(columns={"event_time": "t_a"})
        tb = df[df["event_type"] == type_b][["order_id", "event_time"]].rename(columns={"event_time": "t_b"})
        if ta.empty or tb.empty:
            return None
        joined = ta.merge(tb, on="order_id", how="inner")
        gap = (joined["t_b"] - joined["t_a"]).dt.total_seconds() / 60.0
        valid = gap[gap >= 0]
        return round(valid.mean(), 1) if not valid.empty else None

    v = _avg_gap("order_created", "order_confirmed")
    if v is not None:
        stages["Wait – Confirm"] = v

    delivered_df = df[df["event_type"] == "order_delivered"]
    if not delivered_df.empty:
        prep = _safe_numeric(delivered_df.get("actual_prep_time", pd.Series(dtype=float)))
        if not prep.empty:
            stages["Prep"] = round(prep.mean(), 1)

    v = _avg_gap("order_ready_for_pickup", "order_picked_up")
    if v is not None:
        stages["Wait – Courier"] = v

    if not delivered_df.empty:
        travel = _safe_numeric(delivered_df.get("actual_delivery_time", pd.Series(dtype=float)))
        if not travel.empty:
            stages["Travel"] = round(travel.mean(), 1)

    return stages


# ── At-risk orders ────────────────────────────────────────────────────────────

def compute_at_risk_orders(df: pd.DataFrame) -> pd.DataFrame:
    """In-flight orders above the median elapsed time — candidates for intervention."""
    empty_cols = ["order_id", "zone", "elapsed_min", "status"]
    if df.empty:
        return pd.DataFrame(columns=empty_cols)

    created = df[df["event_type"] == "order_created"][["order_id", "event_time", "user_zone_id"]].rename(
        columns={"event_time": "created_at", "user_zone_id": "zone"}
    )
    terminal_ids = set(df[df["event_type"].isin(_TERMINAL_TYPES)]["order_id"])
    in_flight = created[~created["order_id"].isin(terminal_ids)].copy()

    if in_flight.empty:
        return pd.DataFrame(columns=empty_cols)

    ref_time = df["event_time"].max()
    in_flight["elapsed_min"] = (ref_time - in_flight["created_at"]).dt.total_seconds() / 60.0
    median_elapsed = in_flight["elapsed_min"].median()
    at_risk = (
        in_flight[in_flight["elapsed_min"] >= median_elapsed]
        .sort_values("elapsed_min", ascending=False)
        .assign(elapsed_min=lambda d: d["elapsed_min"].round(1), status="In-Flight")
    )
    return at_risk[empty_cols].reset_index(drop=True)
