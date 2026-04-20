"""
AVRO event serializer for order lifecycle and courier operations feeds.

Converts event dicts to AVRO record format and writes using fastavro.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import fastavro


def _parse_iso_to_epoch_ms(ts: str | datetime) -> int:
    """Convert ISO8601 or datetime to epoch milliseconds."""
    if isinstance(ts, datetime):
        return int(ts.timestamp() * 1000)
    if not ts:
        return int(datetime.utcnow().timestamp() * 1000)
    s = str(ts).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return int(datetime.utcnow().timestamp() * 1000)


def _to_int(val: Any) -> int | None:
    """Convert to int or None."""
    if val is None:
        return None
    try:
        return int(round(float(val)))
    except (ValueError, TypeError):
        return None


class AvroEventSerializer:
    """Serializes events to AVRO files."""

    def __init__(
        self,
        output_dir: Path,
        order_schema_path: Path | None = None,
        courier_schema_path: Path | None = None,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        base = Path(__file__).parent.parent / "schemas"
        self.order_schema_path = order_schema_path or base / "order_lifecycle_schema.avsc"
        self.courier_schema_path = courier_schema_path or base / "courier_operations_schema.avsc"

    def _convert_order_event(self, evt: dict[str, Any]) -> dict[str, Any]:
        """Convert order event dict to AVRO record format."""
        return {
            "event_id": str(evt.get("event_id", "")),
            "event_type": str(evt.get("event_type", "order_created")),
            "event_version": str(evt.get("event_version", "1.0")),
            "event_time": _parse_iso_to_epoch_ms(evt.get("event_time", "")),
            "ingestion_time": _parse_iso_to_epoch_ms(evt.get("ingestion_time", "")),
            "is_late_event": bool(evt.get("is_late_event", False)),
            "event_delay_seconds": _to_int(evt.get("event_delay_seconds")),
            "order_id": str(evt.get("order_id", "")),
            "user_id": str(evt.get("user_id", "")),
            "restaurant_id": str(evt.get("restaurant_id", "")),
            "courier_id": evt.get("courier_id"),
            "user_zone_id": str(evt.get("user_zone_id", "")),
            "restaurant_zone_id": str(evt.get("restaurant_zone_id", "")),
            "estimated_prep_time": _to_int(evt.get("estimated_prep_time")),
            "actual_prep_time": _to_int(evt.get("actual_prep_time")),
            "estimated_delivery_time": _to_int(evt.get("estimated_delivery_time")),
            "actual_delivery_time": _to_int(evt.get("actual_delivery_time")),
            "promo_applied": evt.get("promo_applied"),
            "restaurant_category": str(evt.get("restaurant_category", "")),
            "match_day_flag": bool(evt.get("match_day_flag", False)),
            "holiday_flag": bool(evt.get("holiday_flag", False)),
            "anomaly_flag": bool(evt.get("anomaly_flag", False)),
        }

    def _convert_courier_event(self, evt: dict[str, Any]) -> dict[str, Any]:
        """Convert courier event dict to AVRO record format."""
        return {
            "event_id": str(evt.get("event_id", "")),
            "event_type": str(evt.get("event_type", "courier_online")),
            "event_version": str(evt.get("event_version", "1.0")),
            "event_time": _parse_iso_to_epoch_ms(evt.get("event_time", "")),
            "ingestion_time": _parse_iso_to_epoch_ms(evt.get("ingestion_time", "")),
            "is_late_event": bool(evt.get("is_late_event", False)),
            "event_delay_seconds": _to_int(evt.get("event_delay_seconds")),
            "courier_id": str(evt.get("courier_id", "")),
            "courier_zone_id": str(evt.get("courier_zone_id", "")),
            "order_id": evt.get("order_id"),
            "restaurant_id": evt.get("restaurant_id"),
            "restaurant_zone_id": evt.get("restaurant_zone_id"),
            "user_zone_id": evt.get("user_zone_id"),
            "shift_id": str(evt.get("shift_id", "")),
            "courier_status": str(evt.get("courier_status", "online")),
            "zone_hops_to_restaurant": _to_int(evt.get("zone_hops_to_restaurant")),
            "zone_hops_to_user": _to_int(evt.get("zone_hops_to_user")),
            "estimated_travel_time": _to_int(evt.get("estimated_travel_time")),
            "actual_travel_time": _to_int(evt.get("actual_travel_time")),
            "offline_reason": evt.get("offline_reason"),
        }

    def write_events(
        self,
        feed_name: str,
        events: Iterator[dict[str, Any]],
        file_prefix: str = "events",
        max_per_file: int = 10000,
    ) -> list[Path]:
        """
        Write events to AVRO file(s). Rotates when max_per_file reached.

        Returns list of written file paths.
        """
        written: list[Path] = []
        current_file: Path | None = None
        count = 0
        file_index = 0
        records: list[dict[str, Any]] = []

        schema_path = (
            self.order_schema_path
            if "order" in feed_name.lower()
            else self.courier_schema_path
        )
        with open(schema_path, "rb") as f:
            schema = fastavro.schema.parse_schema(json.load(f))

        converter = (
            self._convert_order_event
            if "order" in feed_name.lower()
            else self._convert_courier_event
        )

        for evt in events:
            records.append(converter(evt))
            count += 1

            if count % max_per_file == 0:
                file_index = (count - 1) // max_per_file
                current_file = self.output_dir / f"{feed_name}_{file_prefix}_{file_index:04d}.avro"
                with open(current_file, "wb") as out:
                    fastavro.writer(out, schema, records)
                written.append(current_file)
                records = []

        if records:
            current_file = self.output_dir / f"{feed_name}_{file_prefix}_{file_index + 1:04d}.avro"
            with open(current_file, "wb") as out:
                fastavro.writer(out, schema, records)
            written.append(current_file)

        return written


class AvroStreamWriter:
    """Buffers events and writes AVRO files in real-time. Flush on max_per_file or close()."""

    def __init__(
        self,
        output_dir: Path,
        feed_name: str,
        file_prefix: str = "events",
        max_per_file: int = 1000,
        order_schema_path: Path | None = None,
        courier_schema_path: Path | None = None,
    ) -> None:
        self.serializer = AvroEventSerializer(
            output_dir,
            order_schema_path=order_schema_path,
            courier_schema_path=courier_schema_path,
        )
        self.feed_name = feed_name
        self.file_prefix = file_prefix
        self.max_per_file = max_per_file
        self._buffer: list[dict[str, Any]] = []
        self._file_index = 0
        self._written: list[Path] = []

    def write(self, evt: dict[str, Any]) -> None:
        """Buffer event; flush to file when buffer reaches max_per_file."""
        self._buffer.append(evt)
        if len(self._buffer) >= self.max_per_file:
            self._flush()

    def _flush(self) -> None:
        """Write buffered records to AVRO file and clear buffer."""
        if not self._buffer:
            return
        records = self._buffer
        self._buffer = []
        schema_path = (
            self.serializer.order_schema_path
            if "order" in self.feed_name.lower()
            else self.serializer.courier_schema_path
        )
        with open(schema_path, "rb") as f:
            schema = fastavro.schema.parse_schema(json.load(f))
        converter = (
            self.serializer._convert_order_event
            if "order" in self.feed_name.lower()
            else self.serializer._convert_courier_event
        )
        converted = [converter(e) for e in records]
        out_path = self.serializer.output_dir / f"{self.feed_name}_{self.file_prefix}_{self._file_index:04d}.avro"
        with open(out_path, "wb") as out:
            fastavro.writer(out, schema, converted)
        self._written.append(out_path)
        self._file_index += 1

    def close(self) -> list[Path]:
        """Flush remaining buffer and return list of written file paths."""
        self._flush()
        return self._written
