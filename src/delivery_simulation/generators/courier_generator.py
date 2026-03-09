"""
Courier Operations Event Generator.

Produces the Courier Operations Feed with events representing courier
operational activity. Events are joinable with the Order Lifecycle Feed
via order_id and courier_id.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Iterator, Optional

from ..loaders.data_context import DataContext
from ..models.travel import TravelModel


@dataclass
class CourierState:
    """Tracks a courier's current operational state."""

    courier_id: str
    zone_id: str
    shift_id: str
    status: str  # online, busy, offline
    order_id: Optional[str] = None
    restaurant_zone_id: Optional[str] = None
    user_zone_id: Optional[str] = None
    zone_hops_to_restaurant: int = 0
    zone_hops_to_user: int = 0
    estimated_travel_time: float = 0.0
    actual_travel_time: Optional[float] = None
    offline_reason: Optional[str] = None


COURIER_EVENT_TYPES = [
    "courier_online",
    "courier_offline",
    "courier_location_update",
    "courier_assigned_order",
    "courier_arrived_restaurant",
    "courier_pickup_completed",
    "courier_arrived_customer_zone",
    "courier_delivery_completed",
    "courier_abandoned_delivery",
    "courier_reassigned",
    "courier_offline_mid_delivery",
]


def _parse_ts(ts: datetime) -> str:
    """Format datetime as ISO8601."""
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _generate_event_id() -> str:
    return f"evt_{uuid.uuid4().hex[:16]}"


class CourierEventGenerator:
    """
    Generates courier operational events aligned with order lifecycle.

    Couriers are assigned to orders; their events (arrived_restaurant,
    pickup_completed, arrived_customer_zone, delivery_completed) are
    emitted at times derived from the travel matrix and order timeline.
    """

    def __init__(
        self,
        data_context: DataContext,
        travel_model: TravelModel,
        config: dict[str, Any],
        rng: Optional[Any] = None,
    ):
        self.ctx = data_context
        self.travel = travel_model
        self.config = config
        self._rng = rng
        self._couriers: dict[str, CourierState] = {}
        self._shift_counter = 0

    def _next_shift_id(self) -> str:
        self._shift_counter += 1
        return f"SHIFT_{self._shift_counter:08d}"

    def _ensure_courier(self, courier_id: str, zone_id: str) -> CourierState:
        if courier_id not in self._couriers:
            shift_id = self._next_shift_id()
            self._couriers[courier_id] = CourierState(
                courier_id=courier_id,
                zone_id=zone_id,
                shift_id=shift_id,
                status="online",
            )
        return self._couriers[courier_id]

    def _base_event(
        self,
        event_type: str,
        event_time: datetime,
        courier_id: str,
        courier_zone_id: str,
        order_id: Optional[str] = None,
        restaurant_id: Optional[str] = None,
        restaurant_zone_id: Optional[str] = None,
        user_zone_id: Optional[str] = None,
        shift_id: Optional[str] = None,
        courier_status: Optional[str] = None,
        zone_hops_to_restaurant: Optional[int] = None,
        zone_hops_to_user: Optional[int] = None,
        estimated_travel_time: Optional[float] = None,
        actual_travel_time: Optional[float] = None,
        offline_reason: Optional[str] = None,
        is_late: bool = False,
        delay_seconds: int = 0,
    ) -> dict[str, Any]:
        ingestion = datetime.utcnow()
        return {
            "event_id": _generate_event_id(),
            "event_type": event_type,
            "event_version": "1.0",
            "event_time": _parse_ts(event_time),
            "ingestion_time": _parse_ts(ingestion),
            "is_late_event": is_late,
            "event_delay_seconds": delay_seconds,
            "courier_id": courier_id,
            "courier_zone_id": courier_zone_id,
            "order_id": order_id,
            "restaurant_id": restaurant_id,
            "restaurant_zone_id": restaurant_zone_id,
            "user_zone_id": user_zone_id,
            "shift_id": shift_id or "",
            "courier_status": courier_status or "online",
            "zone_hops_to_restaurant": zone_hops_to_restaurant if zone_hops_to_restaurant is not None else 0,
            "zone_hops_to_user": zone_hops_to_user if zone_hops_to_user is not None else 0,
            "estimated_travel_time": estimated_travel_time if estimated_travel_time is not None else 0.0,
            "actual_travel_time": actual_travel_time,
            "offline_reason": offline_reason,
        }

    def emit_online(
        self,
        courier_id: str,
        zone_id: str,
        event_time: datetime,
    ) -> dict[str, Any]:
        """Emit courier_online event."""
        state = self._ensure_courier(courier_id, zone_id)
        state.status = "online"
        state.zone_id = zone_id
        return self._base_event(
            "courier_online",
            event_time,
            courier_id=courier_id,
            courier_zone_id=zone_id,
            shift_id=state.shift_id,
            courier_status="online",
        )

    def emit_offline(
        self,
        courier_id: str,
        zone_id: str,
        event_time: datetime,
        reason: str = "end_of_shift",
    ) -> dict[str, Any]:
        """Emit courier_offline event."""
        state = self._ensure_courier(courier_id, zone_id)
        state.status = "offline"
        return self._base_event(
            "courier_offline",
            event_time,
            courier_id=courier_id,
            courier_zone_id=zone_id,
            shift_id=state.shift_id,
            courier_status="offline",
            offline_reason=reason,
        )

    def emit_location_update(
        self,
        courier_id: str,
        zone_id: str,
        event_time: datetime,
    ) -> dict[str, Any]:
        """Emit courier_location_update event."""
        state = self._ensure_courier(courier_id, zone_id)
        state.zone_id = zone_id
        return self._base_event(
            "courier_location_update",
            event_time,
            courier_id=courier_id,
            courier_zone_id=zone_id,
            shift_id=state.shift_id,
            courier_status=state.status,
        )

    def emit_assigned_order(
        self,
        courier_id: str,
        courier_zone_id: str,
        order_id: str,
        restaurant_zone_id: str,
        user_zone_id: str,
        event_time: datetime,
        zone_hops_to_restaurant: int,
        zone_hops_to_user: int,
        estimated_travel_time: float,
    ) -> dict[str, Any]:
        """Emit courier_assigned_order event."""
        state = self._ensure_courier(courier_id, courier_zone_id)
        state.status = "busy"
        state.order_id = order_id
        state.restaurant_zone_id = restaurant_zone_id
        state.user_zone_id = user_zone_id
        state.zone_hops_to_restaurant = zone_hops_to_restaurant
        state.zone_hops_to_user = zone_hops_to_user
        state.estimated_travel_time = estimated_travel_time
        return self._base_event(
            "courier_assigned_order",
            event_time,
            courier_id=courier_id,
            courier_zone_id=courier_zone_id,
            order_id=order_id,
            restaurant_zone_id=restaurant_zone_id,
            user_zone_id=user_zone_id,
            shift_id=state.shift_id,
            courier_status="busy",
            zone_hops_to_restaurant=zone_hops_to_restaurant,
            zone_hops_to_user=zone_hops_to_user,
            estimated_travel_time=estimated_travel_time,
        )

    def emit_arrived_restaurant(
        self,
        courier_id: str,
        order_id: str,
        restaurant_zone_id: str,
        user_zone_id: str,
        event_time: datetime,
        zone_hops_to_restaurant: int,
        zone_hops_to_user: int,
        actual_travel_time: float,
    ) -> dict[str, Any]:
        """Emit courier_arrived_restaurant event."""
        state = self._couriers.get(courier_id)
        if state:
            state.actual_travel_time = actual_travel_time
        return self._base_event(
            "courier_arrived_restaurant",
            event_time,
            courier_id=courier_id,
            courier_zone_id=restaurant_zone_id,
            order_id=order_id,
            restaurant_zone_id=restaurant_zone_id,
            user_zone_id=user_zone_id,
            shift_id=state.shift_id if state else "",
            courier_status="busy",
            zone_hops_to_restaurant=zone_hops_to_restaurant,
            zone_hops_to_user=zone_hops_to_user,
            actual_travel_time=actual_travel_time,
        )

    def emit_pickup_completed(
        self,
        courier_id: str,
        order_id: str,
        restaurant_zone_id: str,
        user_zone_id: str,
        event_time: datetime,
        zone_hops_to_user: int,
    ) -> dict[str, Any]:
        """Emit courier_pickup_completed event."""
        state = self._couriers.get(courier_id)
        return self._base_event(
            "courier_pickup_completed",
            event_time,
            courier_id=courier_id,
            courier_zone_id=restaurant_zone_id,
            order_id=order_id,
            restaurant_zone_id=restaurant_zone_id,
            user_zone_id=user_zone_id,
            shift_id=state.shift_id if state else "",
            courier_status="busy",
            zone_hops_to_user=zone_hops_to_user,
        )

    def emit_arrived_customer_zone(
        self,
        courier_id: str,
        order_id: str,
        restaurant_zone_id: str,
        user_zone_id: str,
        event_time: datetime,
        zone_hops_to_user: int,
        actual_travel_time: float,
    ) -> dict[str, Any]:
        """Emit courier_arrived_customer_zone event."""
        state = self._couriers.get(courier_id)
        if state:
            state.zone_id = user_zone_id
        return self._base_event(
            "courier_arrived_customer_zone",
            event_time,
            courier_id=courier_id,
            courier_zone_id=user_zone_id,
            order_id=order_id,
            restaurant_zone_id=restaurant_zone_id,
            user_zone_id=user_zone_id,
            shift_id=state.shift_id if state else "",
            courier_status="busy",
            zone_hops_to_user=zone_hops_to_user,
            actual_travel_time=actual_travel_time,
        )

    def emit_delivery_completed(
        self,
        courier_id: str,
        order_id: str,
        restaurant_zone_id: str,
        user_zone_id: str,
        event_time: datetime,
        actual_travel_time: float,
    ) -> dict[str, Any]:
        """Emit courier_delivery_completed event."""
        state = self._couriers.get(courier_id)
        if state:
            state.status = "online"
            state.order_id = None
            state.restaurant_zone_id = None
            state.user_zone_id = None
            state.zone_id = user_zone_id
        return self._base_event(
            "courier_delivery_completed",
            event_time,
            courier_id=courier_id,
            courier_zone_id=user_zone_id,
            order_id=order_id,
            restaurant_zone_id=restaurant_zone_id,
            user_zone_id=user_zone_id,
            shift_id=state.shift_id if state else "",
            courier_status="online",
            actual_travel_time=actual_travel_time,
        )

    def emit_abandoned_delivery(
        self,
        courier_id: str,
        order_id: str,
        restaurant_zone_id: str,
        user_zone_id: str,
        event_time: datetime,
        reason: str = "courier_abandoned",
    ) -> dict[str, Any]:
        """Emit courier_abandoned_delivery event."""
        state = self._couriers.get(courier_id)
        if state:
            state.status = "offline"
            state.order_id = None
        return self._base_event(
            "courier_abandoned_delivery",
            event_time,
            courier_id=courier_id,
            courier_zone_id=user_zone_id or restaurant_zone_id,
            order_id=order_id,
            restaurant_zone_id=restaurant_zone_id,
            user_zone_id=user_zone_id,
            shift_id=state.shift_id if state else "",
            courier_status="offline",
            offline_reason=reason,
        )

    def emit_reassigned(
        self,
        courier_id: str,
        order_id: str,
        restaurant_zone_id: str,
        user_zone_id: str,
        event_time: datetime,
    ) -> dict[str, Any]:
        """Emit courier_reassigned event (order taken over by another courier)."""
        state = self._couriers.get(courier_id)
        if state:
            state.order_id = None
            state.status = "online"
        return self._base_event(
            "courier_reassigned",
            event_time,
            courier_id=courier_id,
            courier_zone_id=state.zone_id if state else "",
            order_id=order_id,
            restaurant_zone_id=restaurant_zone_id,
            user_zone_id=user_zone_id,
            shift_id=state.shift_id if state else "",
            courier_status="online",
        )

    def emit_offline_mid_delivery(
        self,
        courier_id: str,
        order_id: str,
        restaurant_zone_id: str,
        user_zone_id: str,
        event_time: datetime,
        reason: str = "technical_issue",
    ) -> dict[str, Any]:
        """Emit courier_offline_mid_delivery event."""
        state = self._couriers.get(courier_id)
        if state:
            state.status = "offline"
        return self._base_event(
            "courier_offline_mid_delivery",
            event_time,
            courier_id=courier_id,
            courier_zone_id=state.zone_id if state else "",
            order_id=order_id,
            restaurant_zone_id=restaurant_zone_id,
            user_zone_id=user_zone_id,
            shift_id=state.shift_id if state else "",
            courier_status="offline",
            offline_reason=reason,
        )

    def initialize_couriers(
        self, count: int, event_time: datetime
    ) -> list[dict[str, Any]]:
        """
        Initialize courier pool. Returns list of {courier_id, zone_id} dicts.
        Emits courier_online events for each (caller yields those).
        """
        import random
        rng = self._rng or random
        pool = []
        zone_ids = self.ctx.zone_ids or ["Z01"]
        for i in range(1, count + 1):
            cid = f"C{i:05d}"
            zone_id = rng.choice(zone_ids) if hasattr(rng, "choice") else random.choice(zone_ids)
            self._ensure_courier(cid, zone_id)
            pool.append({"courier_id": cid, "zone_id": zone_id})
        return pool

    def get_available_couriers(self) -> list[dict[str, Any]]:
        """
        Return couriers that are currently available to take a new order.

        A courier is considered available when their status is "online".
        """
        return [
            {"courier_id": state.courier_id, "zone_id": state.zone_id}
            for state in self._couriers.values()
            if state.status == "online"
        ]

    def generate_courier_events_for_order(
        self,
        order_evts: list[dict],
        courier: dict[str, Any],
        order_id: str,
        order_context: dict | None = None,
    ) -> list[dict[str, Any]]:
        """
        Generate full courier event sequence for an order.
        Requires order_context with restaurant_zone_id, user_zone_id.
        Returns (courier_events, pickup_time, delivery_time) - but we return
        just courier_events; pickup/delivery times are in the events.
        """
        from datetime import datetime as dt_type
        events = []
        if not order_evts or not order_context:
            return events
        first = order_evts[0]
        ctx = order_context or {}
        restaurant_zone_id = ctx.get("restaurant_zone_id") or first.get("restaurant_zone_id", "Z01")
        user_zone_id = ctx.get("user_zone_id") or first.get("user_zone_id", "Z01")
        restaurant_id = ctx.get("restaurant_id")
        courier_id = courier.get("courier_id", "C00001")
        courier_zone_id = courier.get("zone_id", "Z01")
        ready_evt = next((e for e in order_evts if e.get("event_type") == "order_ready_for_pickup"), order_evts[-1])
        ts = ready_evt.get("event_time", "")
        if isinstance(ts, str):
            ts = ts.replace("Z", "+00:00")
            while "+00:00+00:00" in ts:
                ts = ts.replace("+00:00+00:00", "+00:00")
            t_ready = dt_type.fromisoformat(ts) if ts else dt_type.utcnow()
        else:
            t_ready = ts
        travel_to_rest, hops_to_rest, _, cancel_f = self.travel.sample_travel_time(courier_zone_id, restaurant_zone_id)
        travel_to_user, hops_to_user, _, _ = self.travel.sample_travel_time(restaurant_zone_id, user_zone_id)
        from datetime import timedelta
        t_assigned = t_ready
        events.append(self.emit_assigned_order(
            courier_id, courier_zone_id, order_id, restaurant_zone_id, user_zone_id,
            t_assigned, hops_to_rest, hops_to_user, travel_to_rest + travel_to_user
        ))
        t_arrived_rest = t_ready + timedelta(minutes=travel_to_rest)
        events.append(self.emit_arrived_restaurant(
            courier_id, order_id, restaurant_zone_id, user_zone_id,
            t_arrived_rest, hops_to_rest, hops_to_user, travel_to_rest
        ))
        t_pickup = t_arrived_rest + timedelta(minutes=0.5)
        events.append(self.emit_pickup_completed(
            courier_id, order_id, restaurant_zone_id, user_zone_id, t_pickup, hops_to_user
        ))
        t_arrived_user = t_pickup + timedelta(minutes=travel_to_user)
        events.append(self.emit_arrived_customer_zone(
            courier_id, order_id, restaurant_zone_id, user_zone_id,
            t_arrived_user, hops_to_user, travel_to_user
        ))
        t_delivered = t_arrived_user + timedelta(minutes=0.5)
        events.append(self.emit_delivery_completed(
            courier_id, order_id, restaurant_zone_id, user_zone_id, t_delivered, travel_to_user + 0.5
        ))
        if restaurant_id:
            for evt in events:
                evt["restaurant_id"] = restaurant_id
        # Attach pickup/delivery times for engine to emit order events (internal, stripped before output)
        return events
