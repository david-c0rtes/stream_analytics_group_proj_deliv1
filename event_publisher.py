import os
import json
import queue
import threading
from io import BytesIO
from pathlib import Path
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData
import fastavro

load_dotenv()

CONN_STR = os.getenv("EVENTHUB_CONN_STR", "").strip()
ORDERS_HUB = os.getenv("EVENTHUB_ORDERS", "").strip()
COURIERS_HUB = os.getenv("EVENTHUB_COURIERS", "").strip()
PAYLOAD_FORMAT = os.getenv("EVENTHUB_PAYLOAD_FORMAT", "json").strip().lower()

_eh_enabled = bool(CONN_STR and ORDERS_HUB and COURIERS_HUB)

# Separate queues per hub so order/courier sends never block each other
_order_queue: queue.Queue = queue.Queue(maxsize=10_000)
_courier_queue: queue.Queue = queue.Queue(maxsize=10_000)
_send_errors = 0
_MAX_ERRORS = 5

_SCHEMA_BASE = Path(__file__).parent / "src" / "delivery_simulation" / "schemas"
_ORDER_SCHEMA_PATH = _SCHEMA_BASE / "order_lifecycle_schema.avsc"
_COURIER_SCHEMA_PATH = _SCHEMA_BASE / "courier_operations_schema.avsc"

_order_schema = None
_courier_schema = None
if PAYLOAD_FORMAT == "avro":
    try:
        with open(_ORDER_SCHEMA_PATH, "r", encoding="utf-8") as fh:
            _order_schema = fastavro.parse_schema(json.load(fh))
        with open(_COURIER_SCHEMA_PATH, "r", encoding="utf-8") as fh:
            _courier_schema = fastavro.parse_schema(json.load(fh))
    except Exception as exc:
        print(f"[EventHub] AVRO schema load failed, falling back to JSON: {exc}", flush=True)
        PAYLOAD_FORMAT = "json"


def _to_epoch_ms(value) -> int:
    if value is None:
        return 0
    text = str(value).replace("Z", "+00:00").replace("+00:00+00:00", "+00:00")
    try:
        from datetime import datetime
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except Exception:
        return 0


def _to_int(value):
    if value is None:
        return None
    try:
        return int(round(float(value)))
    except Exception:
        return None


def _serialize_order_avro(event: dict) -> bytes:
    record = {
        "event_id": str(event.get("event_id", "")),
        "event_type": str(event.get("event_type", "order_created")),
        "event_version": str(event.get("event_version", "1.0")),
        "event_time": _to_epoch_ms(event.get("event_time")),
        "ingestion_time": _to_epoch_ms(event.get("ingestion_time")),
        "is_late_event": bool(event.get("is_late_event", False)),
        "event_delay_seconds": _to_int(event.get("event_delay_seconds")),
        "order_id": str(event.get("order_id", "")),
        "user_id": str(event.get("user_id", "")),
        "restaurant_id": str(event.get("restaurant_id", "")),
        "courier_id": event.get("courier_id"),
        "user_zone_id": str(event.get("user_zone_id", "")),
        "restaurant_zone_id": str(event.get("restaurant_zone_id", "")),
        "estimated_prep_time": _to_int(event.get("estimated_prep_time")),
        "actual_prep_time": _to_int(event.get("actual_prep_time")),
        "estimated_delivery_time": _to_int(event.get("estimated_delivery_time")),
        "actual_delivery_time": _to_int(event.get("actual_delivery_time")),
        "promo_applied": event.get("promo_applied"),
        "restaurant_category": str(event.get("restaurant_category", "")),
        "match_day_flag": bool(event.get("match_day_flag", False)),
        "holiday_flag": bool(event.get("holiday_flag", False)),
        "anomaly_flag": bool(event.get("anomaly_flag", False)),
    }
    buf = BytesIO()
    fastavro.schemaless_writer(buf, _order_schema, record)
    return buf.getvalue()


def _serialize_courier_avro(event: dict) -> bytes:
    record = {
        "event_id": str(event.get("event_id", "")),
        "event_type": str(event.get("event_type", "courier_online")),
        "event_version": str(event.get("event_version", "1.0")),
        "event_time": _to_epoch_ms(event.get("event_time")),
        "ingestion_time": _to_epoch_ms(event.get("ingestion_time")),
        "is_late_event": bool(event.get("is_late_event", False)),
        "event_delay_seconds": _to_int(event.get("event_delay_seconds")),
        "courier_id": str(event.get("courier_id", "")),
        "courier_zone_id": str(event.get("courier_zone_id", "")),
        "order_id": event.get("order_id"),
        "restaurant_id": event.get("restaurant_id"),
        "restaurant_zone_id": event.get("restaurant_zone_id"),
        "user_zone_id": event.get("user_zone_id"),
        "shift_id": str(event.get("shift_id", "")),
        "courier_status": str(event.get("courier_status", "online")),
        "zone_hops_to_restaurant": _to_int(event.get("zone_hops_to_restaurant")),
        "zone_hops_to_user": _to_int(event.get("zone_hops_to_user")),
        "estimated_travel_time": _to_int(event.get("estimated_travel_time")),
        "actual_travel_time": _to_int(event.get("actual_travel_time")),
        "offline_reason": event.get("offline_reason"),
    }
    buf = BytesIO()
    fastavro.schemaless_writer(buf, _courier_schema, record)
    return buf.getvalue()


def _worker(q: queue.Queue, producer: EventHubProducerClient, label: str) -> None:
    """Background thread: drain queue and send batches to Event Hub."""
    global _send_errors
    while True:
        item = q.get()
        if item is None:  # shutdown signal
            q.task_done()
            break
        try:
            batch = producer.create_batch()
            batch.add(EventData(item))
            # Drain any additional items that arrived while we were waiting
            while True:
                try:
                    extra = q.get_nowait()
                    if extra is None:
                        q.task_done()
                        q.put(None)  # re-queue shutdown signal
                        break
                    try:
                        batch.add(EventData(extra))
                        q.task_done()
                    except Exception:
                        # Batch full — send what we have and start a new one
                        producer.send_batch(batch)
                        batch = producer.create_batch()
                        batch.add(EventData(extra))
                        q.task_done()
                except queue.Empty:
                    break
            producer.send_batch(batch)
            _send_errors = 0
        except Exception as exc:
            _send_errors += 1
            if _send_errors <= _MAX_ERRORS:
                print(f"[EventHub] {label} send error ({_send_errors}): {exc}", flush=True)
        finally:
            q.task_done()


if _eh_enabled:
    _orders_producer = EventHubProducerClient.from_connection_string(
        conn_str=CONN_STR, eventhub_name=ORDERS_HUB
    )
    _couriers_producer = EventHubProducerClient.from_connection_string(
        conn_str=CONN_STR, eventhub_name=COURIERS_HUB
    )
    _t_orders = threading.Thread(
        target=_worker, args=(_order_queue, _orders_producer, "orders"), daemon=True
    )
    _t_couriers = threading.Thread(
        target=_worker, args=(_courier_queue, _couriers_producer, "couriers"), daemon=True
    )
    _t_orders.start()
    _t_couriers.start()
    print(f"[EventHub] async publishers ready → {ORDERS_HUB}, {COURIERS_HUB} (payload={PAYLOAD_FORMAT})")
else:
    _orders_producer = None
    _couriers_producer = None
    print("[EventHub] WARNING: credentials missing — publishing disabled")


def send_order_event(event: dict) -> None:
    """Non-blocking: enqueue event for background sending."""
    if not _eh_enabled or _send_errors >= _MAX_ERRORS:
        return
    try:
        payload = _serialize_order_avro(event) if PAYLOAD_FORMAT == "avro" else json.dumps(event)
        _order_queue.put_nowait(payload)
    except queue.Full:
        pass  # drop rather than block the simulation


def send_courier_event(event: dict) -> None:
    """Non-blocking: enqueue event for background sending."""
    if not _eh_enabled or _send_errors >= _MAX_ERRORS:
        return
    try:
        payload = _serialize_courier_avro(event) if PAYLOAD_FORMAT == "avro" else json.dumps(event)
        _courier_queue.put_nowait(payload)
    except queue.Full:
        pass


def close_producers() -> None:
    """Flush queues then close producers gracefully."""
    if _eh_enabled:
        _order_queue.join()
        _courier_queue.join()
        _order_queue.put(None)
        _courier_queue.put(None)
        if _orders_producer:
            _orders_producer.close()
        if _couriers_producer:
            _couriers_producer.close()
