"""
Order lifecycle event generator.

Emits order lifecycle events (created, confirmed, preparing, etc.) with
realistic timing derived from Madrid datasets.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from ..loaders.data_context import DataContext
from ..models.cancellation import CancellationModel
from ..models.demand import DemandModel
from ..models.travel import TravelModel


ORDER_EVENT_TYPES = [
    "order_created",
    "order_confirmed",
    "order_preparing",
    "order_ready_for_pickup",
    "order_picked_up",
    "order_delivered",
    "order_cancelled_user",
    "order_cancelled_restaurant",
    "order_cancelled_system",
]


class OrderEventGenerator:
    """Generates order lifecycle events for the streaming simulation."""

    def __init__(
        self,
        data_context: DataContext,
        demand_model: DemandModel,
        travel_model: TravelModel,
        cancellation_model: CancellationModel,
        config: dict,
        rng: Any,
    ) -> None:
        self.ctx = data_context
        self.demand = demand_model
        self.travel = travel_model
        self.cancellation = cancellation_model
        self.config = config
        self.rng = rng
        self._user_counter = 0
        self._order_counter = 0

    def _next_user_id(self) -> str:
        self._user_counter += 1
        return f"U{self._user_counter:06d}"

    def _next_order_id(self) -> str:
        self._order_counter += 1
        return f"ORD{self._order_counter:08d}"

    @staticmethod
    def _to_iso(ts: datetime) -> str:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def _sample_prep_time(self, restaurant: dict[str, Any]) -> tuple[float, float]:
        mean = float(restaurant.get("base_prep_time_mean_min", 15.0))
        std = float(restaurant.get("base_prep_time_std_min", 4.0))
        latency = float(restaurant.get("latency_pressure_factor", 1.0))
        estimated = max(5.0, self.rng.gauss(mean, std))
        actual = max(5.0, self.rng.gauss(mean * latency, std * 1.2))
        return round(estimated, 1), round(actual, 1)

    def _base_event(
        self,
        event_type: str,
        event_time: datetime,
        order_id: str,
        user_id: str,
        restaurant_id: str,
        user_zone_id: str,
        restaurant_zone_id: str,
        courier_id: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_version": "1.0",
            "event_time": self._to_iso(event_time),
            "ingestion_time": self._to_iso(datetime.now(timezone.utc)),
            "is_late_event": bool(kwargs.get("is_late_event", False)),
            "event_delay_seconds": kwargs.get("event_delay_seconds"),
            "order_id": order_id,
            "user_id": user_id,
            "restaurant_id": restaurant_id,
            "courier_id": courier_id,
            "user_zone_id": user_zone_id,
            "restaurant_zone_id": restaurant_zone_id,
            "estimated_prep_time": kwargs.get("estimated_prep_time"),
            "actual_prep_time": kwargs.get("actual_prep_time"),
            "estimated_delivery_time": kwargs.get("estimated_delivery_time"),
            "actual_delivery_time": kwargs.get("actual_delivery_time"),
            "promo_applied": kwargs.get("promo_applied"),
            "restaurant_category": kwargs.get("restaurant_category", ""),
            "match_day_flag": bool(kwargs.get("match_day_flag", False)),
            "holiday_flag": bool(kwargs.get("holiday_flag", False)),
            "anomaly_flag": bool(kwargs.get("anomaly_flag", False)),
        }

    def generate_order_created(
        self,
        event_time: datetime,
        user_zone_id: str,
        restaurant: dict[str, Any],
        promo_applied: str,
        match_day: bool,
        holiday: bool,
    ) -> dict[str, Any]:
        user_id = self._next_user_id()
        order_id = self._next_order_id()
        restaurant_id = str(restaurant.get("restaurant_id", ""))
        restaurant_zone_id = str(restaurant.get("zone_id", ""))
        category = str(restaurant.get("category", ""))

        estimated_prep, actual_prep = self._sample_prep_time(restaurant)
        route = self.travel.get_route(restaurant_zone_id, user_zone_id)
        estimated_delivery = float(route["mean_time_min"]) if route else 15.0

        event = self._base_event(
            event_type="order_created",
            event_time=event_time,
            order_id=order_id,
            user_id=user_id,
            restaurant_id=restaurant_id,
            user_zone_id=user_zone_id,
            restaurant_zone_id=restaurant_zone_id,
            courier_id=None,
            estimated_prep_time=estimated_prep,
            actual_prep_time=actual_prep,
            estimated_delivery_time=estimated_delivery,
            actual_delivery_time=None,
            promo_applied=promo_applied or None,
            restaurant_category=category,
            match_day_flag=match_day,
            holiday_flag=holiday,
            anomaly_flag=False,
            event_delay_seconds=None,
        )

        return {
            "event": event,
            "order_context": {
                "order_id": order_id,
                "user_id": user_id,
                "restaurant_id": restaurant_id,
                "user_zone_id": user_zone_id,
                "restaurant_zone_id": restaurant_zone_id,
                "restaurant": restaurant,
                "estimated_prep_time": estimated_prep,
                "actual_prep_time": actual_prep,
                "estimated_delivery_time": estimated_delivery,
                "promo_applied": promo_applied or None,
                "match_day_flag": match_day,
                "holiday_flag": holiday,
            },
        }

    def emit_order_confirmed(
        self, event_time: datetime, order_context: dict[str, Any], courier_id: Optional[str] = None
    ) -> dict[str, Any]:
        return self._base_event("order_confirmed", event_time, courier_id=courier_id, **order_context)

    def emit_order_preparing(
        self, event_time: datetime, order_context: dict[str, Any], courier_id: Optional[str] = None
    ) -> dict[str, Any]:
        return self._base_event("order_preparing", event_time, courier_id=courier_id, **order_context)

    def emit_order_ready_for_pickup(
        self,
        event_time: datetime,
        order_context: dict[str, Any],
        courier_id: Optional[str] = None,
        actual_prep_time: Optional[float] = None,
    ) -> dict[str, Any]:
        payload = dict(order_context)
        if actual_prep_time is not None:
            payload["actual_prep_time"] = actual_prep_time
        return self._base_event("order_ready_for_pickup", event_time, courier_id=courier_id, **payload)

    def emit_order_picked_up(
        self,
        event_time: datetime,
        order_context: dict[str, Any],
        courier_id: Optional[str],
        actual_prep_time: Optional[float],
    ) -> dict[str, Any]:
        payload = dict(order_context)
        payload["actual_prep_time"] = actual_prep_time
        return self._base_event("order_picked_up", event_time, courier_id=courier_id, **payload)

    def emit_order_delivered(
        self,
        event_time: datetime,
        order_context: dict[str, Any],
        courier_id: Optional[str],
        actual_prep_time: Optional[float],
        actual_delivery_time: Optional[float],
        anomaly_flag: bool = False,
    ) -> dict[str, Any]:
        payload = dict(order_context)
        payload["actual_prep_time"] = actual_prep_time
        payload["actual_delivery_time"] = actual_delivery_time
        payload["anomaly_flag"] = anomaly_flag
        return self._base_event("order_delivered", event_time, courier_id=courier_id, **payload)

    def emit_order_cancelled(
        self,
        event_time: datetime,
        order_context: dict[str, Any],
        cancel_type: str,
        courier_id: Optional[str] = None,
    ) -> dict[str, Any]:
        cancel_type = (cancel_type or "system").lower()
        if cancel_type not in {"user", "restaurant", "system"}:
            cancel_type = "system"
        return self._base_event(
            f"order_cancelled_{cancel_type}",
            event_time,
            courier_id=courier_id,
            **order_context,
        )

    def emit_cancellation(
        self,
        order_id: str,
        reason: str,
        cancel_time: datetime,
        order_events: list[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if not order_events:
            return None
        first = order_events[0]
        if first.get("event_type") != "order_created":
            return None
        order_context = {
            "order_id": first["order_id"],
            "user_id": first["user_id"],
            "restaurant_id": first["restaurant_id"],
            "user_zone_id": first["user_zone_id"],
            "restaurant_zone_id": first["restaurant_zone_id"],
            "restaurant": {"category": first.get("restaurant_category", "")},
            "estimated_prep_time": first.get("estimated_prep_time"),
            "actual_prep_time": first.get("actual_prep_time"),
            "estimated_delivery_time": first.get("estimated_delivery_time"),
            "promo_applied": first.get("promo_applied"),
            "match_day_flag": first.get("match_day_flag", False),
            "holiday_flag": first.get("holiday_flag", False),
        }
        cancel_type = reason.replace("order_cancelled_", "")
        return self.emit_order_cancelled(cancel_time, order_context, cancel_type, None)

    def generate_orders_for_window(
        self,
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        base_rate = float(self.config.get("demand", {}).get("base_orders_per_minute", 2.0))
        window_minutes = max(1.0, (window_end - window_start).total_seconds() / 60.0)
        expected_orders = base_rate * window_minutes * self.demand.get_time_multiplier(window_start)
        num_orders = max(0, int(round(expected_orders + self.rng.gauss(0.0, max(0.5, expected_orders ** 0.5)))))

        active_promos = self.demand.get_active_promos(window_start)
        holiday = self.demand.is_holiday(window_start)
        match_primary = set()
        match_secondary = set()
        match_day = False
        for match in self.config.get("football_matches", []):
            primary = self.ctx.district_to_zone(match.get("primary_zone", ""))
            if primary:
                match_primary.add(primary)
                match_day = True
            for eff in self.ctx.football_effects:
                secondary = self.ctx.district_to_zone(eff.secondary_district)
                if secondary:
                    match_secondary.add(secondary)

        zone_ids = self.ctx.zone_ids
        if not zone_ids:
            return results
        spawn_probs = self.ctx.spawn_probabilities or [1.0] * len(zone_ids)

        for _ in range(num_orders):
            user_zone_id = self.rng.choices(zone_ids, weights=spawn_probs, k=1)[0]
            if not self.demand.should_spawn_order(window_start, user_zone_id):
                continue
            restaurant = self.demand.select_restaurant(
                user_zone_id,
                window_start,
                match_day,
                match_primary,
                match_secondary,
                active_promos,
            )
            if not restaurant:
                continue

            promo_name = ""
            for name, categories in active_promos.items():
                if restaurant.get("category") in categories:
                    promo_name = name
                    break

            created_result = self.generate_order_created(
                window_start,
                user_zone_id,
                restaurant,
                promo_name,
                match_day,
                holiday,
            )
            first_event = created_result["event"]
            order_context = created_result["order_context"]
            order_events: list[dict[str, Any]] = [first_event]

            user_sensitivity = self.travel.get_user_cancel_sensitivity(
                order_context["restaurant_zone_id"], user_zone_id
            )
            if self.cancellation.user_cancels(0, order_context["estimated_delivery_time"], user_sensitivity):
                order_events.append(self.emit_order_cancelled(window_start, order_context, "user", None))
                results.append({"order_events": order_events, "order_context": order_context})
                continue

            confirm_time = window_start + timedelta(minutes=1)
            order_events.append(self.emit_order_confirmed(confirm_time, order_context, None))
            order_events.append(self.emit_order_preparing(confirm_time, order_context, None))

            actual_prep = float(order_context["actual_prep_time"])
            ready_time = window_start + timedelta(minutes=actual_prep)
            if self.cancellation.restaurant_cancels(actual_prep):
                order_events.append(self.emit_order_cancelled(ready_time, order_context, "restaurant", None))
                results.append({"order_events": order_events, "order_context": order_context})
                continue

            order_events.append(self.emit_order_ready_for_pickup(ready_time, order_context, None, actual_prep))
            results.append({"order_events": order_events, "order_context": order_context})

        return results
