"""
Madrid Delivery Control Tower — main layout renderer.

Sections:
  1. KPI Ribbon
  2. Madrid Zone Map + Order Trend
  3. Courier Activity + On-Time Trend
  4. Avg Delivery by Zone + Cancellation Mix
  5. Prep vs Travel KPIs + Bottleneck Waterfall
  6. Supply & Demand Intelligence
  7. Revenue Intelligence
  8. At-Risk Orders + Zone Rankings
"""
from __future__ import annotations

import streamlit as st

from dashboard.config.constants import (
    DEFAULT_AVG_ORDER_VALUE_EUR,
    MADRID_ZONE_COORDS,
    SLA_DELIVERY_MINUTES,
)
from dashboard.data.loader import data_freshness, load_courier_events, load_order_events
from dashboard.compute.orders import (
    compute_at_risk_orders,
    compute_cancellation_mix,
    compute_delivery_percentile_trend,
    compute_full_delivery_by_zone,
    compute_on_time_trend,
    compute_order_kpis,
    compute_orders_over_time,
    compute_stage_times,
    compute_zone_metrics,
)
from dashboard.compute.couriers import (
    compute_courier_activity,
    compute_courier_kpis,
    compute_zone_courier_demand,
)
from dashboard.compute.revenue import compute_gmv, compute_gmv_by_zone, compute_gmv_over_time
import plotly.express as px

from dashboard.ui.charts import (
    chart_avg_delivery_by_zone,
    chart_bottleneck_waterfall,
    chart_cancellation_mix,
    chart_courier_activity,
    chart_gmv_over_time,
    chart_madrid_zone_map,
    chart_on_time_gauge,
    chart_on_time_trend,
    chart_order_donut,
    chart_orders_over_time,
    chart_p50_p95_trend,
    chart_supply_demand_gap,
    chart_zone_stress_heatmap,
    table_at_risk_orders,
    table_zone_rankings,
)

# ── KPI card helper ───────────────────────────────────────────────────────────

def _kpi(col, label: str, value, delta=None, help_text: str | None = None) -> None:
    with col:
        display_val = "—" if value is None else str(value)
        st.metric(label=label, value=display_val, delta=delta, help=help_text)


def _section(title: str) -> None:
    st.divider()
    st.markdown(f"### {title}")


# ── Main render ───────────────────────────────────────────────────────────────

def render() -> None:
    """Render the full control tower dashboard."""

    # ── Load data ─────────────────────────────────────────────────────────────
    df_orders = load_order_events()
    df_couriers = load_courier_events()

    # ── Compute everything ────────────────────────────────────────────────────
    kpis = compute_order_kpis(df_orders)
    courier_kpis = compute_courier_kpis(df_couriers)
    gmv = compute_gmv(df_orders)
    orders_trend = compute_orders_over_time(df_orders)
    zone_metrics = compute_zone_metrics(df_orders)
    courier_activity = compute_courier_activity(df_couriers)
    on_time_trend = compute_on_time_trend(df_orders)
    cancel_mix = compute_cancellation_mix(df_orders)
    delivery_by_zone = compute_full_delivery_by_zone(df_orders)
    zone_stress = compute_zone_courier_demand(df_orders, df_couriers)
    gmv_by_zone = compute_gmv_by_zone(df_orders)
    gmv_over_time = compute_gmv_over_time(df_orders)
    stage_times = compute_stage_times(df_orders)
    percentile_trend = compute_delivery_percentile_trend(df_orders)
    at_risk = compute_at_risk_orders(df_orders)
    zone_rankings = table_zone_rankings(zone_metrics, gmv_by_zone, delivery_by_zone)

    # ── Data freshness badge ──────────────────────────────────────────────────
    freshness = data_freshness(df_orders, df_couriers)
    latest = freshness.get("latest_event")
    latest_str = latest.strftime("%Y-%m-%d %H:%M UTC") if latest is not None else "no data"
    st.caption(
        f"**{freshness['order_count']:,}** order events · "
        f"**{freshness['courier_count']:,}** courier events · "
        f"Latest: **{latest_str}** · "
        f"Live — refreshing every second"
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 1 — KPI Ribbon
    # ═══════════════════════════════════════════════════════════════════════════

    c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(8)
    _kpi(c1, "Orders Today", kpis["created_today"], help_text="Unique orders created on the latest stream day")
    _kpi(c2, "Delivered Today", kpis["delivered_today"], help_text="Unique orders delivered on the latest stream day")
    _kpi(c3, "Cancelled", kpis["total_cancelled"],
         delta=f"{kpis['cancellation_rate_pct']}%", help_text="% of created orders")
    _kpi(c4, "In Flight", kpis["orders_in_flight"], help_text="Created but not yet terminal")
    _kpi(c5, "Avg Delivery",
         f"{kpis['avg_full_delivery_min']} min" if kpis["avg_full_delivery_min"] else None,
         help_text="Created → Delivered")
    _kpi(c6, "On-Time %",
         f"{kpis['on_time_rate_pct']}%" if kpis["on_time_rate_pct"] is not None else None,
         help_text=f"Orders delivered within {int(SLA_DELIVERY_MINUTES)} min SLA")
    _kpi(c7, "Active Couriers", courier_kpis["active_couriers"])
    _kpi(c8, "Est. GMV Today",
         f"€{gmv['estimated_gmv_eur']:,.0f}",
         help_text=f"Delivered orders × €{DEFAULT_AVG_ORDER_VALUE_EUR:.0f} assumed AOV (estimated)")

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 2 — Madrid Zone Map + Order Trend
    # ═══════════════════════════════════════════════════════════════════════════

    _section("Madrid Zone Activity")
    col_map, col_orders = st.columns([1.3, 1])

    with col_map:
        st.markdown("**Zone Activity Map** — bubble size = orders · colour = avg delivery time")
        st.plotly_chart(
            chart_madrid_zone_map(zone_metrics, delivery_by_zone),
            use_container_width=True,
        )

    with col_orders:
        st.markdown("**Orders Over Time**")
        st.plotly_chart(chart_orders_over_time(orders_trend), use_container_width=True)

        st.markdown("**Order Outcome Mix**")
        st.plotly_chart(chart_order_donut(kpis), use_container_width=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 3 — Courier Activity + On-Time Trend
    # ═══════════════════════════════════════════════════════════════════════════

    _section("Courier Activity & Service Level")
    col_c, col_ot = st.columns(2)

    with col_c:
        st.markdown("**Courier Activity Over Time**")
        st.plotly_chart(chart_courier_activity(courier_activity), use_container_width=True)

    with col_ot:
        st.markdown("**On-Time Delivery Rate Trend**")
        st.plotly_chart(chart_on_time_trend(on_time_trend), use_container_width=True)

    # Gauge + P50/P95 in next row
    col_gauge, col_pct = st.columns([1, 2])
    with col_gauge:
        st.plotly_chart(chart_on_time_gauge(kpis["on_time_rate_pct"]), use_container_width=True)
    with col_pct:
        st.markdown("**P50 vs P95 Delivery Time Trend**")
        st.plotly_chart(chart_p50_p95_trend(percentile_trend), use_container_width=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 4 — Avg Delivery by Zone + Cancellation Mix
    # ═══════════════════════════════════════════════════════════════════════════

    _section("Zone Performance & Cancellations")
    col_dz, col_cx = st.columns(2)

    with col_dz:
        st.markdown("**Avg Full Delivery Time by Zone** *(slowest first)*")
        st.plotly_chart(chart_avg_delivery_by_zone(delivery_by_zone), use_container_width=True)

    with col_cx:
        st.markdown("**Cancellation Mix Over Time**")
        st.plotly_chart(chart_cancellation_mix(cancel_mix), use_container_width=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 5 — Prep vs Travel KPIs + Bottleneck Waterfall
    # ═══════════════════════════════════════════════════════════════════════════

    _section("Delivery Stage Analysis")
    kpi_cols = st.columns(4)
    _kpi(kpi_cols[0], "Avg Prep Time",
         f"{kpis['avg_prep_time_min']} min" if kpis["avg_prep_time_min"] else None,
         help_text="actual_prep_time from delivered events")
    _kpi(kpi_cols[1], "P95 Prep Time",
         f"{kpis['p95_prep_time_min']} min" if kpis["p95_prep_time_min"] else None)
    _kpi(kpi_cols[2], "Avg Travel Time",
         f"{kpis['avg_travel_time_min']} min" if kpis["avg_travel_time_min"] else None,
         help_text="actual_delivery_time (pickup → delivery)")
    _kpi(kpi_cols[3], "P95 Travel Time",
         f"{kpis['p95_travel_time_min']} min" if kpis["p95_travel_time_min"] else None)

    st.markdown("**Avg Time per Delivery Stage** *(bottleneck analysis)*")
    st.plotly_chart(chart_bottleneck_waterfall(stage_times), use_container_width=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 6 — Supply & Demand Intelligence
    # ═══════════════════════════════════════════════════════════════════════════

    _section("Supply & Demand Intelligence")
    col_gap, col_heat = st.columns(2)

    with col_gap:
        st.markdown("**Supply vs Demand Gap** — new orders (bars) vs active couriers (line)")
        st.plotly_chart(
            chart_supply_demand_gap(orders_trend, courier_activity),
            use_container_width=True,
        )

    with col_heat:
        st.markdown("**Zone Stress Heatmap** — demand/courier ratio")
        st.plotly_chart(chart_zone_stress_heatmap(zone_stress), use_container_width=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 7 — Revenue Intelligence
    # ═══════════════════════════════════════════════════════════════════════════

    _section("Revenue Intelligence *(estimated)*")
    rev_cols = st.columns(3)
    _kpi(rev_cols[0], "Est. GMV Today",
         f"€{gmv['estimated_gmv_eur']:,.0f}",
         help_text=f"Delivered orders × €{DEFAULT_AVG_ORDER_VALUE_EUR:.0f} assumed AOV")
    _kpi(rev_cols[1], "Hourly Run-Rate",
         f"€{gmv['hourly_run_rate_eur']:,.0f} / hr",
         help_text="GMV ÷ elapsed hours")
    _kpi(rev_cols[2], "EOD Projection",
         f"€{gmv['eod_projection_eur']:,.0f}",
         help_text=f"If current pace holds for 16-hr delivery day")

    col_gmv_time, col_gmv_zone = st.columns(2)

    with col_gmv_time:
        st.markdown("**Estimated GMV Over Time**")
        st.plotly_chart(chart_gmv_over_time(gmv_over_time), use_container_width=True)

    with col_gmv_zone:
        st.markdown("**Estimated GMV by Zone** *(top 10)*")
        if not gmv_by_zone.empty:
            top10 = gmv_by_zone.head(10).copy()
            top10["zone_label"] = top10["zone_id"].map(
                lambda z: f"{z} – {MADRID_ZONE_COORDS.get(z, {}).get('name', z)}"
            )
            fig_gmv_zone = px.bar(
                top10.sort_values("estimated_gmv_eur"),
                x="estimated_gmv_eur", y="zone_label",
                orientation="h",
                color_discrete_sequence=["#4D7C8A"],
                labels={"estimated_gmv_eur": "Est. GMV (€)", "zone_label": ""},
                height=320,
            )
            fig_gmv_zone.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=140, r=40, t=10, b=40),
            )
            st.plotly_chart(fig_gmv_zone, use_container_width=True)
        else:
            st.info("No delivered orders yet.")

    st.caption("⚠ All revenue figures are *estimated* using an assumed average order value of "
               f"€{DEFAULT_AVG_ORDER_VALUE_EUR:.0f}. Not accounting-grade revenue.")

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 8 — At-Risk Orders + Zone Rankings
    # ═══════════════════════════════════════════════════════════════════════════

    _section("Operational Intelligence")
    col_risk, col_rank = st.columns([1, 1.4])

    with col_risk:
        st.markdown("**At-Risk Orders** *(in-flight, above-median elapsed time)*")
        risk_display = table_at_risk_orders(at_risk)
        if risk_display.empty:
            st.success("No at-risk orders — all in-flight orders within normal range.")
        else:
            st.dataframe(
                risk_display,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Elapsed (min)": st.column_config.NumberColumn(format="%.1f"),
                },
            )

    with col_rank:
        st.markdown("**Zone Rankings**")
        if zone_rankings.empty:
            st.info("No zone data yet.")
        else:
            st.dataframe(
                zone_rankings,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Avg Delivery (min)": st.column_config.NumberColumn(format="%.1f"),
                    "Cancel Rate %": st.column_config.NumberColumn(format="%.1f"),
                    "Est. GMV (€)": st.column_config.NumberColumn(format="€%.0f"),
                },
            )
