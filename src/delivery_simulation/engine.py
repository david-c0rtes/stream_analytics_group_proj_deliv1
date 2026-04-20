"""
Simulation engine orchestrating order and courier event generation.

Coordinates both feeds, applies streaming edge cases (late/duplicate/out-of-order events),
and produces joinable JSON and AVRO streams.
"""

from __future__ import annotations

import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import SimulationConfig, load_simulation_config, generate_random_football_schedule
from .loaders.data_context import DataContext
from .generators.order_generator import OrderEventGenerator
from .generators.courier_generator import CourierEventGenerator
from .models.demand import DemandModel
from .models.travel import TravelModel
from .models.cancellation import CancellationModel


class SimulationEngine:
    """
    Main simulation engine that runs the food delivery streaming simulation.

    Produces two independent but joinable feeds: order lifecycle and courier operations.
    """

    def __init__(
        self,
        config: SimulationConfig,
        data_context: DataContext,
        seed: int | None = None,
    ) -> None:
        self.config = config
        self.data_context = data_context
        self.rng = random.Random(seed or config.seed)
        raw = config.raw

        demand_model = DemandModel(data_context, raw, self.rng)
        travel_model = TravelModel(data_context, self.rng)
        cancellation_model = CancellationModel(data_context, self.rng)

        # Resolve football schedule: random or static
        start_dt = self._parse_simulation_start() or datetime.now(timezone.utc)
        football_matches = generate_random_football_schedule(raw, start_dt, self.rng)
        order_config = {**raw, "football_matches": football_matches}

        self.order_gen = OrderEventGenerator(
            data_context, demand_model, travel_model, cancellation_model, order_config, self.rng
        )
        self.courier_gen = CourierEventGenerator(
            data_context, travel_model, raw, self.rng
        )

        self._order_counter = 0
        self._user_counter = 0
        self._courier_counter = 0
        self._shift_counter = 0

    def _generate_id(self, prefix: str) -> str:
        """Generate a unique ID with prefix."""
        return f"{prefix}{uuid.uuid4().hex[:12].upper()}"

    def _maybe_apply_edge_case(
        self, event: dict[str, Any], event_time: datetime
    ) -> list[dict[str, Any]]:
        """
        Apply streaming edge cases: late events, duplicates, out-of-order.

        Returns a list of events (may include original + duplicates, or modified).
        """
        events = [event]

        # Late event: delay ingestion_time (sample from Normal distribution)
        late_prob = self.config.sample_late_event_probability(self.rng)
        if self.rng.random() < late_prob:
            delay_sec = self.config.sample_late_event_delay_seconds(self.rng)
            events[0] = {**event, "is_late_event": True, "event_delay_seconds": delay_sec}
            it_str = events[0].get("ingestion_time", "")
            it_str = it_str.replace("Z", "+00:00") if isinstance(it_str, str) else ""
            try:
                it_dt = datetime.fromisoformat(it_str) + timedelta(seconds=delay_sec)
                events[0]["ingestion_time"] = it_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            except (ValueError, TypeError):
                pass

        # Duplicate event (sample from Normal distribution)
        dup_prob = self.config.sample_duplicate_event_probability(self.rng)
        if self.rng.random() < dup_prob:
            dup = {**events[0], "event_id": self._generate_id("EVT")}
            events.append(dup)

        return events

    def _maybe_out_of_order(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Occasionally swap adjacent events at emit-time to simulate out-of-order arrival.
        Business event_time is kept unchanged; only emission order changes.
        """
        if len(events) < 2:
            return events
        out = list(events)
        i = 1
        while i < len(out):
            oo_prob = self.config.sample_out_of_order_probability(self.rng)
            if self.rng.random() < oo_prob:
                out[i - 1], out[i] = out[i], out[i - 1]
                i += 2
                continue
            i += 1
        return out

    @staticmethod
    def _event_time_to_dt(event: dict[str, Any], fallback: datetime) -> datetime:
        raw = event.get("event_time")
        if not isinstance(raw, str):
            return fallback
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return fallback

    def _parse_simulation_start(self) -> datetime | None:
        """Parse simulation_start from config (YYYY-MM-DD HH:MM) to UTC datetime."""
        s = self.config.simulation_start
        if not s:
            return None
        try:
            dt = datetime.strptime(s.strip(), "%Y-%m-%d %H:%M")
            return dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return None

    def run(
        self,
        start_time: datetime | None = None,
        duration_minutes: int | None = None,
    ) -> Iterator[tuple[str, dict[str, Any]]]:
        """
        Run the simulation and yield (feed_name, event) tuples.

        feed_name is either "order_lifecycle" or "courier_operations".
        When realtime_mode is True, throttles by wall-clock so events arrive in real time.
        """
        start = start_time or self._parse_simulation_start() or datetime.now(timezone.utc).replace(microsecond=0)
        duration = duration_minutes or self.config.simulation_duration_minutes
        end_time = start + timedelta(minutes=duration)
        step_minutes = self.config.tick_interval_minutes
        realtime = self.config.realtime_mode
        real_sec_per_sim_min = self.config.real_seconds_per_sim_minute

        # Initialize courier pool (internal courier state in generator)
        self.courier_gen.initialize_couriers(
            self.config.num_couriers,
            start,
        )

        current_time = start
        pending_orders: list[dict[str, Any]] = []

        while current_time < end_time:
            current_time = current_time + timedelta(minutes=step_minutes)

            # 1. Generate new orders for this time window
            new_orders = self.order_gen.generate_orders_for_window(
                current_time - timedelta(minutes=step_minutes),
                current_time,
            )

            for order_bundle in new_orders:
                order_events = order_bundle.get("order_events", [])
                order_context = order_bundle.get("order_context", {})
                for evt in self._maybe_out_of_order(order_events):
                    for final_evt in self._maybe_apply_edge_case(evt, evt.get("event_time")):
                        yield ("order_lifecycle", final_evt)

                # Only add to pending if order reached ready_for_pickup (needs courier)
                has_ready = any(
                    e.get("event_type") == "order_ready_for_pickup" for e in order_events
                )
                if has_ready:
                    first = order_events[0] if order_events else {}
                    pending_orders.append(
                        {
                            "order_events": order_events,
                            "order_context": order_context,
                            "order_id": first.get("order_id"),
                            "created_at": current_time,
                        }
                    )

            # 2. Assign couriers to pending orders and generate courier events
            assigned = []
            available_couriers = self.courier_gen.get_available_couriers()
            for po in pending_orders:
                if not available_couriers:
                    break
                courier = self.rng.choice(available_couriers)
                # Prevent assigning the same courier to multiple orders in the same tick
                available_couriers.remove(courier)
                order_evts = po["order_events"]
                order_context = po.get("order_context", {})

                courier_events = self.courier_gen.generate_courier_events_for_order(
                    order_evts=order_evts,
                    courier=courier,
                    order_id=po["order_id"],
                    order_context=order_context,
                )

                # Optional edge case: courier goes offline mid-delivery.
                courier_events_to_emit = list(courier_events)
                delivery_evt_for_order = next(
                    (e for e in courier_events_to_emit if e.get("event_type") == "courier_delivery_completed"),
                    None,
                )
                if courier_events_to_emit and self.rng.random() < self.config.sample_courier_offline_mid_delivery_probability(self.rng):
                    assigned_evt = next(
                        (e for e in courier_events_to_emit if e.get("event_type") == "courier_assigned_order"),
                        None,
                    )
                    pickup_evt = next(
                        (e for e in courier_events_to_emit if e.get("event_type") == "courier_pickup_completed"),
                        None,
                    )
                    arrived_user_evt = next(
                        (e for e in courier_events_to_emit if e.get("event_type") == "courier_arrived_customer_zone"),
                        None,
                    )
                    if assigned_evt and pickup_evt and arrived_user_evt:
                        t_pickup = self._event_time_to_dt(pickup_evt, current_time)
                        t_arrived_user = self._event_time_to_dt(arrived_user_evt, t_pickup + timedelta(minutes=1))
                        t_offline = t_pickup + timedelta(minutes=1)
                        t_abandon = t_offline + timedelta(seconds=30)
                        old_courier_id = courier.get("courier_id", "")
                        rest_zone = order_context.get("restaurant_zone_id", "")
                        user_zone = order_context.get("user_zone_id", "")

                        replacement = next(
                            (c for c in available_couriers if c.get("courier_id") != old_courier_id),
                            None,
                        )

                        courier_events_to_emit = [
                            e
                            for e in courier_events_to_emit
                            if e.get("event_type")
                            in {"courier_assigned_order", "courier_arrived_restaurant", "courier_pickup_completed"}
                        ]
                        courier_events_to_emit.append(
                            self.courier_gen.emit_offline_mid_delivery(
                                old_courier_id,
                                po["order_id"],
                                rest_zone,
                                user_zone,
                                t_offline,
                                reason="technical_issue",
                            )
                        )
                        courier_events_to_emit.append(
                            self.courier_gen.emit_abandoned_delivery(
                                old_courier_id,
                                po["order_id"],
                                rest_zone,
                                user_zone,
                                t_abandon,
                                reason="courier_offline_mid_delivery",
                            )
                        )

                        if replacement:
                            repl_cid = replacement.get("courier_id", "")
                            repl_zone = replacement.get("zone_id", user_zone)
                            available_couriers = [c for c in available_couriers if c.get("courier_id") != repl_cid]
                            reassigned_at = t_abandon + timedelta(seconds=10)
                            courier_events_to_emit.append(
                                self.courier_gen.emit_reassigned(
                                    old_courier_id,
                                    po["order_id"],
                                    rest_zone,
                                    user_zone,
                                    reassigned_at,
                                )
                            )
                            courier_events_to_emit.append(
                                self.courier_gen.emit_assigned_order(
                                    repl_cid,
                                    repl_zone,
                                    po["order_id"],
                                    rest_zone,
                                    user_zone,
                                    reassigned_at,
                                    int(assigned_evt.get("zone_hops_to_restaurant") or 0),
                                    int(assigned_evt.get("zone_hops_to_user") or 0),
                                    float(assigned_evt.get("estimated_travel_time") or 0.0),
                                )
                            )
                            remaining_min = max(1.0, (t_arrived_user - t_pickup).total_seconds() / 60.0)
                            t_repl_arrive = reassigned_at + timedelta(minutes=remaining_min)
                            courier_events_to_emit.append(
                                self.courier_gen.emit_arrived_customer_zone(
                                    repl_cid,
                                    po["order_id"],
                                    rest_zone,
                                    user_zone,
                                    t_repl_arrive,
                                    int(assigned_evt.get("zone_hops_to_user") or 0),
                                    remaining_min,
                                )
                            )
                            t_repl_deliver = t_repl_arrive + timedelta(minutes=0.5)
                            replacement_delivery = self.courier_gen.emit_delivery_completed(
                                repl_cid,
                                po["order_id"],
                                rest_zone,
                                user_zone,
                                t_repl_deliver,
                                remaining_min + 0.5,
                            )
                            courier_events_to_emit.append(replacement_delivery)
                            delivery_evt_for_order = replacement_delivery

                # Remove internal fields before yielding
                for cevt in self._maybe_out_of_order(courier_events_to_emit):
                    cevt.pop("_pickup_time", None)
                    cevt.pop("_delivery_time", None)
                    if (
                        cevt.get("event_type") == "courier_delivery_completed"
                        and self.rng.random() < self.config.sample_anomaly_injection_probability(self.rng)
                    ):
                        cevt["actual_travel_time"] = -5.0
                    for final_cevt in self._maybe_apply_edge_case(cevt, cevt.get("event_time")):
                        yield ("courier_operations", final_cevt)

                # Emit order_picked_up and order_delivered from order generator
                pickup_evt = next(
                    (e for e in courier_events_to_emit if e.get("event_type") == "courier_pickup_completed"),
                    None,
                )
                delivery_evt = delivery_evt_for_order
                if delivery_evt and order_context:
                    ts_pickup = pickup_evt.get("event_time", "") if pickup_evt else ""
                    ts_delivery = delivery_evt.get("event_time", "")
                    if isinstance(ts_pickup, str):
                        ts_pickup = ts_pickup.replace("Z", "+00:00")
                        t_pickup = datetime.fromisoformat(ts_pickup) if ts_pickup else datetime.utcnow()
                    else:
                        t_pickup = datetime.utcnow()
                    if isinstance(ts_delivery, str):
                        ts_delivery = ts_delivery.replace("Z", "+00:00")
                        t_delivery = datetime.fromisoformat(ts_delivery)
                    else:
                        t_delivery = datetime.utcnow()
                    actual_prep = float(order_context.get("actual_prep_time") or 0)
                    actual_delivery = (t_delivery - t_pickup).total_seconds() / 60.0
                    missing_step = self.rng.random() < self.config.sample_missing_step_probability(self.rng)
                    anomaly = self.rng.random() < self.config.sample_anomaly_injection_probability(self.rng)
                    if anomaly:
                        actual_prep = -3.0
                        actual_delivery = -7.0
                    evt_pickup = self.order_gen.emit_order_picked_up(
                        t_pickup, order_context, courier.get("courier_id", ""), actual_prep
                    )
                    evt_delivery = self.order_gen.emit_order_delivered(
                        t_delivery, order_context, courier.get("courier_id", ""),
                        actual_prep, actual_delivery, anomaly_flag=anomaly
                    )
                    order_completion_events = [evt_delivery] if missing_step else [evt_pickup, evt_delivery]
                    for oe in self._maybe_out_of_order(order_completion_events):
                        for final_evt in self._maybe_apply_edge_case(oe, t_delivery):
                            yield ("order_lifecycle", final_evt)

                elif pickup_evt and order_context:
                    # If courier flow ended without delivery, still emit pickup when available.
                    ts_pickup = pickup_evt.get("event_time", "")
                    if isinstance(ts_pickup, str):
                        t_pickup = datetime.fromisoformat(ts_pickup.replace("Z", "+00:00"))
                    else:
                        t_pickup = datetime.utcnow()
                    evt_pickup = self.order_gen.emit_order_picked_up(
                        t_pickup, order_context, courier.get("courier_id", ""), order_context.get("actual_prep_time")
                    )
                    for final_evt in self._maybe_apply_edge_case(evt_pickup, t_pickup):
                        yield ("order_lifecycle", final_evt)

                assigned.append(po)

            for po in assigned:
                pending_orders.remove(po)

            # Throttle by wall-clock at end of tick (events visible before next tick)
            if realtime:
                time.sleep(step_minutes * real_sec_per_sim_min)

        # Emit any remaining pending orders as cancelled (simulation end)
        for po in pending_orders:
            cancel_evt = self.order_gen.emit_cancellation(
                order_id=po["order_id"],
                reason="order_cancelled_system",
                cancel_time=end_time,
                order_events=po["order_events"],
            )
            if cancel_evt:
                for final_evt in self._maybe_apply_edge_case(cancel_evt, end_time):
                    yield ("order_lifecycle", final_evt)
