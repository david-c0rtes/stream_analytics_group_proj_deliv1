"""
Chart rendering functions for the Madrid Delivery Control Tower.

Each function accepts pre-computed DataFrames/dicts and returns a Plotly Figure
(or a DataFrame for table components).  All functions degrade gracefully on
empty or missing data.
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from dashboard.config.constants import (
    COLORS,
    MADRID_ZONE_COORDS,
    ON_TIME_SLA_TARGET_PCT,
    SLA_DELIVERY_MINUTES,
)

_PLOT_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", size=12),
)
_MARGIN = dict(l=40, r=20, t=30, b=40)
_GRID = dict(gridcolor="#f0f0f0", zeroline=False)
_LEGEND_TOP = dict(orientation="h", y=1.05, x=0, font=dict(size=11))


def _layout(**overrides) -> dict:
    """Merge _PLOT_BASE with a default margin and per-chart overrides (overrides win)."""
    return {**_PLOT_BASE, "margin": _MARGIN, **overrides}


# Keep alias for backwards compat inside this module
_PLOT_DEFAULTS = _layout()


# ── Utility ───────────────────────────────────────────────────────────────────

def _empty_fig(msg: str = "No data available yet.") -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=msg, x=0.5, y=0.5, xref="paper", yref="paper",
        showarrow=False, font=dict(size=13, color="#aaa"),
    )
    fig.update_layout(
        **_PLOT_DEFAULTS,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        height=300,
    )
    return fig


def _zone_label(zone_id: str) -> str:
    info = MADRID_ZONE_COORDS.get(zone_id, {})
    return f"{zone_id} – {info.get('name', zone_id)}"


# ── 1. Madrid Zone Map ────────────────────────────────────────────────────────

def chart_madrid_zone_map(
    zone_metrics: pd.DataFrame,
    delivery_by_zone: pd.DataFrame,
) -> go.Figure:
    """
    Scatter mapbox: Madrid districts.
    Bubble size = order volume.  Colour = avg full delivery time.
    Hover shows zone, orders, delivery time, cancellation rate.
    """
    coords = pd.DataFrame(
        [{"zone_id": k, "lat": v["lat"], "lon": v["lon"], "district": v["name"]}
         for k, v in MADRID_ZONE_COORDS.items()]
    )

    if zone_metrics.empty:
        # Still render map with neutral bubbles so layout is preserved
        merged = coords.copy()
        merged["total_orders"] = 1
        merged["avg_delivery_min"] = 0.0
        merged["cancellation_rate"] = 0.0
    else:
        merged = coords.merge(zone_metrics, on="zone_id", how="left")
        if not delivery_by_zone.empty:
            merged = merged.merge(
                delivery_by_zone.rename(columns={"avg_full_delivery_min": "avg_delivery_min"}),
                on="zone_id", how="left",
            )
        else:
            merged["avg_delivery_min"] = 0.0

        for col, default in [("total_orders", 0), ("cancellation_rate", 0.0), ("avg_delivery_min", 0.0)]:
            if col not in merged.columns:
                merged[col] = default
            merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(default)

    merged["size_val"] = (merged["total_orders"].clip(lower=1)).astype(float)

    fig = px.scatter_mapbox(
        merged,
        lat="lat",
        lon="lon",
        size="size_val",
        size_max=45,
        color="avg_delivery_min",
        color_continuous_scale=[[0, COLORS["heatmap_low"]], [1, COLORS["heatmap_high"]]],
        hover_name="district",
        hover_data={
            "zone_id": True,
            "total_orders": True,
            "cancellation_rate": True,
            "avg_delivery_min": True,
            "lat": False,
            "lon": False,
            "size_val": False,
        },
        labels={
            "avg_delivery_min": "Avg Delivery (min)",
            "total_orders": "Orders",
            "cancellation_rate": "Cancel %",
            "zone_id": "Zone",
        },
        mapbox_style="open-street-map",
        center={"lat": 40.4168, "lon": -3.7038},
        zoom=10,
        height=450,
    )
    fig.update_coloraxes(colorbar_title="Avg Delivery<br>(min)")
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)")
    return fig


# ── 2. Orders Over Time ───────────────────────────────────────────────────────

def chart_orders_over_time(df: pd.DataFrame) -> go.Figure:
    """Line chart: orders created, delivered, and cumulative active orders."""
    if df.empty or "bucket" not in df.columns:
        return _empty_fig("Waiting for order data…")

    fig = go.Figure()

    if "orders_created" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["bucket"], y=df["orders_created"],
            name="Created", mode="lines+markers",
            line=dict(color=COLORS["orders_created"], width=2),
            marker=dict(size=5),
        ))

    if "orders_delivered" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["bucket"], y=df["orders_delivered"],
            name="Delivered", mode="lines+markers",
            line=dict(color=COLORS["orders_delivered"], width=2),
            marker=dict(size=5),
        ))

    if "active_orders" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["bucket"], y=df["active_orders"],
            name="Active (cumul.)", mode="lines",
            line=dict(color=COLORS["active_orders"], width=1.5, dash="dot"),
            fill="tozeroy",
            fillcolor="rgba(27,64,121,0.08)",
        ))

    fig.update_layout(
        **_PLOT_DEFAULTS,
        xaxis=dict(title="Time", **_GRID),
        yaxis=dict(title="Orders", **_GRID),
        legend=_LEGEND_TOP,
        height=360,
    )
    return fig


# ── 3. Courier Activity ───────────────────────────────────────────────────────

def chart_courier_activity(df: pd.DataFrame) -> go.Figure:
    """Line chart: active couriers and online couriers over time."""
    if df.empty or "bucket" not in df.columns:
        return _empty_fig("No courier activity data.")

    fig = go.Figure()

    if "active_couriers" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["bucket"], y=df["active_couriers"],
            name="Active (On Delivery)", mode="lines+markers",
            line=dict(color=COLORS["courier_active"], width=2),
            marker=dict(size=5),
        ))

    if "online_couriers" in df.columns and df["online_couriers"].sum() > 0:
        fig.add_trace(go.Scatter(
            x=df["bucket"], y=df["online_couriers"],
            name="Online", mode="lines+markers",
            line=dict(color=COLORS["courier_online"], width=2, dash="dash"),
            marker=dict(size=5),
        ))

    fig.update_layout(
        **_PLOT_DEFAULTS,
        xaxis=dict(title="Time", **_GRID),
        yaxis=dict(title="Couriers", **_GRID),
        legend=_LEGEND_TOP,
        height=340,
    )
    return fig


# ── 4. Avg Delivery Time by Zone ──────────────────────────────────────────────

def chart_avg_delivery_by_zone(df: pd.DataFrame) -> go.Figure:
    """Horizontal bar: avg full delivery time by zone, slowest first."""
    if df.empty or "zone_id" not in df.columns:
        return _empty_fig("No delivery time data by zone.")

    d = df.copy()
    d["label"] = d["zone_id"].map(_zone_label)
    d = d.sort_values("avg_full_delivery_min", ascending=True)   # ascending → longest at top in hbar

    fig = go.Figure(go.Bar(
        x=d["avg_full_delivery_min"],
        y=d["label"],
        orientation="h",
        marker_color=COLORS["funnel_bars"],
        text=d["avg_full_delivery_min"].map(lambda v: f"{v:.1f} min"),
        textposition="outside",
        cliponaxis=False,
    ))

    # SLA reference line
    fig.add_vline(
        x=SLA_DELIVERY_MINUTES,
        line_dash="dash",
        line_color=COLORS["reference_lines"],
        annotation_text=f"SLA {SLA_DELIVERY_MINUTES:.0f} min",
        annotation_position="top",
    )

    fig.update_layout(
        **_layout(margin=dict(l=160, r=70, t=30, b=40)),
        xaxis=dict(title="Avg Full Delivery (min)", **_GRID),
        yaxis=dict(title=""),
        height=max(280, len(d) * 28 + 80),
    )
    return fig


# ── 5. Cancellation Mix Over Time ─────────────────────────────────────────────

def chart_cancellation_mix(df: pd.DataFrame) -> go.Figure:
    """Stacked bar: cancellation types over time."""
    if df.empty or "bucket" not in df.columns:
        return _empty_fig("No cancellation data yet.")

    col_cfg = {
        "order_cancelled_user": ("User", COLORS["cancellation_user"]),
        "order_cancelled_restaurant": ("Restaurant", COLORS["cancellation_restaurant"]),
        "order_cancelled_system": ("System", COLORS["cancellation_system"]),
    }

    fig = go.Figure()
    for col, (label, color) in col_cfg.items():
        if col in df.columns:
            fig.add_trace(go.Bar(x=df["bucket"], y=df[col], name=label, marker_color=color))

    fig.update_layout(
        **_PLOT_DEFAULTS,
        barmode="stack",
        xaxis=dict(title="Time", **_GRID),
        yaxis=dict(title="Cancellations", **_GRID),
        legend=_LEGEND_TOP,
        height=340,
    )
    return fig


# ── 6. On-Time Delivery Rate Trend ────────────────────────────────────────────

def chart_on_time_trend(df: pd.DataFrame) -> go.Figure:
    """Line chart: on-time delivery rate % with SLA target reference line."""
    if df.empty or "bucket" not in df.columns:
        return _empty_fig("No on-time data yet.")

    fig = go.Figure(go.Scatter(
        x=df["bucket"],
        y=df["on_time_rate_pct"],
        name="On-Time Rate",
        mode="lines+markers",
        line=dict(color=COLORS["orders_delivered"], width=2),
        marker=dict(size=6),
        fill="tozeroy",
        fillcolor="rgba(77,124,138,0.12)",
    ))

    fig.add_hline(
        y=ON_TIME_SLA_TARGET_PCT,
        line_dash="dash",
        line_color=COLORS["reference_lines"],
        annotation_text=f"SLA target {ON_TIME_SLA_TARGET_PCT:.0f}%",
        annotation_position="bottom right",
        annotation_font_color=COLORS["reference_lines"],
    )

    fig.update_layout(
        **_PLOT_DEFAULTS,
        xaxis=dict(title="Time", **_GRID),
        yaxis=dict(title="On-Time Rate (%)", range=[0, 108], **_GRID),
        height=340,
    )
    return fig


# ── 7. On-Time Gauge ──────────────────────────────────────────────────────────

def chart_on_time_gauge(on_time_pct: float | None) -> go.Figure:
    """Gauge indicator for current on-time delivery rate."""
    value = on_time_pct if on_time_pct is not None else 0.0
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        domain={"x": [0, 1], "y": [0, 1]},
        title={"text": "On-Time Rate", "font": {"size": 14}},
        delta={"reference": ON_TIME_SLA_TARGET_PCT, "suffix": "%"},
        number={"suffix": "%", "font": {"size": 24}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1},
            "bar": {"color": COLORS["orders_delivered"], "thickness": 0.28},
            "threshold": {
                "line": {"color": COLORS["reference_lines"], "width": 3},
                "thickness": 0.75,
                "value": ON_TIME_SLA_TARGET_PCT,
            },
            "steps": [
                {"range": [0, ON_TIME_SLA_TARGET_PCT * 0.7], "color": "#fde8e8"},
                {"range": [ON_TIME_SLA_TARGET_PCT * 0.7, ON_TIME_SLA_TARGET_PCT], "color": "#fff9c4"},
                {"range": [ON_TIME_SLA_TARGET_PCT, 100], "color": "#e8f5e9"},
            ],
        },
    ))
    fig.update_layout(
        height=240,
        margin=dict(l=20, r=20, t=40, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


# ── 8. P50 vs P95 Delivery Trend ─────────────────────────────────────────────

def chart_p50_p95_trend(df: pd.DataFrame) -> go.Figure:
    """Line chart: P50 and P95 full delivery time vs SLA threshold."""
    if df.empty or "bucket" not in df.columns:
        return _empty_fig("Not enough data for percentile trend.")

    fig = go.Figure()

    if "p50" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["bucket"], y=df["p50"].round(1),
            name="P50 (Median)", mode="lines+markers",
            line=dict(color=COLORS["orders_delivered"], width=2),
            marker=dict(size=5),
        ))

    if "p95" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["bucket"], y=df["p95"].round(1),
            name="P95", mode="lines+markers",
            line=dict(color=COLORS["cancellation_user"], width=2, dash="dash"),
            marker=dict(size=5),
        ))

    fig.add_hline(
        y=SLA_DELIVERY_MINUTES,
        line_dash="dot",
        line_color=COLORS["reference_lines"],
        annotation_text=f"SLA {SLA_DELIVERY_MINUTES:.0f} min",
        annotation_position="bottom right",
        annotation_font_color=COLORS["reference_lines"],
    )

    fig.update_layout(
        **_PLOT_DEFAULTS,
        xaxis=dict(title="Time", **_GRID),
        yaxis=dict(title="Delivery Time (min)", **_GRID),
        legend=_LEGEND_TOP,
        height=300,
    )
    return fig


# ── 9. Bottleneck Waterfall ───────────────────────────────────────────────────

def chart_bottleneck_waterfall(stage_times: dict[str, float]) -> go.Figure:
    """Waterfall chart showing average time in each delivery stage."""
    if not stage_times:
        return _empty_fig("Stage time data not available.")

    stages = list(stage_times.keys())
    times = [round(v, 1) for v in stage_times.values()]
    total = round(sum(times), 1)

    fig = go.Figure(go.Waterfall(
        name="Delivery Stages",
        orientation="v",
        measure=["relative"] * len(stages) + ["total"],
        x=stages + ["Total"],
        y=times + [0],
        text=[f"{t} min" for t in times] + [f"{total} min"],
        textposition="outside",
        connector=dict(line=dict(color="#ccc", width=1)),
        increasing=dict(marker=dict(color=COLORS["funnel_bars"])),
        totals=dict(marker=dict(color=COLORS["orders_delivered"])),
        cliponaxis=False,
    ))

    fig.update_layout(
        **_PLOT_DEFAULTS,
        yaxis=dict(title="Minutes", **_GRID),
        height=360,
    )
    return fig


# ── 10. Zone Stress Heatmap ───────────────────────────────────────────────────

def chart_zone_stress_heatmap(df: pd.DataFrame) -> go.Figure:
    """Heatmap: demand / courier ratio by zone × time bucket."""
    if df.empty:
        return _empty_fig("No supply/demand data.")

    d = df.copy()
    d["zone_label"] = d["zone_id"].map(_zone_label)

    pivot = d.pivot_table(
        index="zone_label", columns="bucket",
        values="demand_courier_ratio", aggfunc="mean",
    ).fillna(0)

    if pivot.empty:
        return _empty_fig()

    x_labels = [
        b.strftime("%H:%M") if hasattr(b, "strftime") else str(b)
        for b in pivot.columns
    ]

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=x_labels,
        y=list(pivot.index),
        colorscale=[[0, COLORS["heatmap_low"]], [1, COLORS["heatmap_high"]]],
        colorbar=dict(title="Orders / Courier"),
        hoverongaps=False,
        hovertemplate="Zone: %{y}<br>Time: %{x}<br>Ratio: %{z:.2f}<extra></extra>",
    ))

    fig.update_layout(
        **_layout(margin=dict(l=160, r=20, t=20, b=60)),
        xaxis=dict(title="Time Bucket"),
        yaxis=dict(title=""),
        height=max(300, len(pivot) * 22 + 80),
    )
    return fig


# ── 11. Supply vs Demand Gap ──────────────────────────────────────────────────

def chart_supply_demand_gap(orders_trend: pd.DataFrame, courier_activity: pd.DataFrame) -> go.Figure:
    """
    Dual-axis chart: new orders per bucket (bars) vs active couriers (line).
    Visual supply pressure indicator.
    """
    if orders_trend.empty:
        return _empty_fig("No supply/demand data.")

    fig = go.Figure()

    if "orders_created" in orders_trend.columns:
        fig.add_trace(go.Bar(
            x=orders_trend["bucket"],
            y=orders_trend["orders_created"],
            name="New Orders",
            marker_color=COLORS["orders_created"],
            opacity=0.75,
        ))

    if not courier_activity.empty and "active_couriers" in courier_activity.columns:
        fig.add_trace(go.Scatter(
            x=courier_activity["bucket"],
            y=courier_activity["active_couriers"],
            name="Active Couriers",
            mode="lines+markers",
            line=dict(color=COLORS["active_orders"], width=2),
            yaxis="y2",
        ))

    fig.update_layout(
        **_layout(margin=dict(l=50, r=60, t=30, b=40)),
        barmode="overlay",
        xaxis=dict(title="Time", **_GRID),
        yaxis=dict(title="New Orders", **_GRID),
        yaxis2=dict(title="Active Couriers", overlaying="y", side="right", showgrid=False),
        legend=_LEGEND_TOP,
        height=300,
    )
    return fig


# ── 12. GMV Over Time ─────────────────────────────────────────────────────────

def chart_gmv_over_time(df: pd.DataFrame) -> go.Figure:
    """Bar chart: estimated GMV per time bucket."""
    if df.empty or "bucket" not in df.columns:
        return _empty_fig("No revenue data yet.")

    fig = go.Figure(go.Bar(
        x=df["bucket"],
        y=df["estimated_gmv_eur"],
        marker_color=COLORS["gmv"],
        text=df["estimated_gmv_eur"].map(lambda v: f"€{v:.0f}"),
        textposition="outside",
        cliponaxis=False,
    ))

    fig.update_layout(
        **_PLOT_DEFAULTS,
        xaxis=dict(title="Time", **_GRID),
        yaxis=dict(title="Estimated GMV (€)", **_GRID),
        height=300,
    )
    return fig


# ── 13. Order Outcome Donut ───────────────────────────────────────────────────

def chart_order_donut(kpis: dict) -> go.Figure:
    """Donut: delivered / cancelled / in-flight share of total orders."""
    delivered = kpis.get("total_delivered", 0)
    cancelled = kpis.get("total_cancelled", 0)
    in_flight = kpis.get("orders_in_flight", 0)
    total = delivered + cancelled + in_flight

    if total == 0:
        return _empty_fig("No order data.")

    fig = go.Figure(go.Pie(
        labels=["Delivered", "Cancelled", "In-Flight"],
        values=[delivered, cancelled, in_flight],
        hole=0.52,
        marker_colors=[COLORS["orders_delivered"], COLORS["cancellation_user"], COLORS["active_orders"]],
        textinfo="percent+label",
        insidetextorientation="horizontal",
    ))
    fig.update_layout(
        height=260,
        margin=dict(l=20, r=20, t=30, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
    )
    return fig


# ── 14. At-Risk Orders Table ──────────────────────────────────────────────────

def table_at_risk_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Pass-through; formatting done in control_tower via st.dataframe."""
    return df.rename(columns={
        "order_id": "Order ID",
        "zone": "Zone",
        "elapsed_min": "Elapsed (min)",
        "status": "Status",
    })


# ── 15. Zone Rankings Table ───────────────────────────────────────────────────

def table_zone_rankings(
    zone_metrics: pd.DataFrame,
    gmv_by_zone: pd.DataFrame,
    delivery_by_zone: pd.DataFrame,
) -> pd.DataFrame:
    """Combined zone rankings table — orders, delivery time, cancellation rate, est. GMV."""
    if zone_metrics.empty:
        return pd.DataFrame()

    result = zone_metrics.copy()
    result["district"] = result["zone_id"].map(
        lambda z: MADRID_ZONE_COORDS.get(z, {}).get("name", z)
    )

    if not delivery_by_zone.empty:
        result = result.merge(delivery_by_zone[["zone_id", "avg_full_delivery_min"]], on="zone_id", how="left")
    else:
        result["avg_full_delivery_min"] = None

    if not gmv_by_zone.empty:
        result = result.merge(gmv_by_zone[["zone_id", "estimated_gmv_eur"]], on="zone_id", how="left")
    else:
        result["estimated_gmv_eur"] = 0.0

    result["estimated_gmv_eur"] = pd.to_numeric(result.get("estimated_gmv_eur", 0), errors="coerce").fillna(0)
    result = result.sort_values("total_orders", ascending=False).reset_index(drop=True)

    display = result[["zone_id", "district", "total_orders", "avg_full_delivery_min", "cancellation_rate", "estimated_gmv_eur"]]
    display = display.rename(columns={
        "zone_id": "Zone",
        "district": "District",
        "total_orders": "Orders",
        "avg_full_delivery_min": "Avg Delivery (min)",
        "cancellation_rate": "Cancel Rate %",
        "estimated_gmv_eur": "Est. GMV (€)",
    })
    return display
