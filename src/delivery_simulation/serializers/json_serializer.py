"""
JSON event serializer for order lifecycle and courier operations feeds.

Writes events as newline-delimited JSON (NDJSON) for streaming compatibility.
Adds event_time_epoch_ms and ingestion_time_epoch_ms for Event Hubs / Spark.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator


def _iso_to_epoch_ms(ts: str | None) -> int:
    """Convert ISO8601 string to epoch milliseconds. Returns 0 if invalid."""
    if not ts:
        return 0
    s = str(ts).replace("Z", "+00:00").replace("+00:00+00:00", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return 0


def _enrich_for_eventhub(evt: dict[str, Any]) -> dict[str, Any]:
    """Add epoch_ms fields for Event Hubs / Spark Structured Streaming."""
    out = dict(evt)
    out["event_time_epoch_ms"] = _iso_to_epoch_ms(evt.get("event_time"))
    out["ingestion_time_epoch_ms"] = _iso_to_epoch_ms(evt.get("ingestion_time"))
    return out


class JsonEventSerializer:
    """Serializes events to NDJSON files."""

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_events(
        self,
        feed_name: str,
        events: Iterator[dict[str, Any]],
        file_prefix: str = "events",
        max_per_file: int = 10000,
    ) -> list[Path]:
        """
        Write events to NDJSON file(s). Rotates to new file when max_per_file reached.

        Returns list of written file paths.
        """
        written: list[Path] = []
        current_file: Path | None = None
        writer = None
        count = 0
        file_index = 0

        for evt in events:
            if count % max_per_file == 0 or writer is None:
                if writer is not None:
                    writer.close()
                file_index = count // max_per_file
                current_file = self.output_dir / f"{feed_name}_{file_prefix}_{file_index:04d}.ndjson"
                writer = open(current_file, "w", encoding="utf-8")
                written.append(current_file)

            enriched = _enrich_for_eventhub(evt)
            writer.write(json.dumps(enriched, ensure_ascii=False) + "\n")
            count += 1

        if writer is not None:
            writer.close()

        return written

    def write_event_stream(
        self,
        event_stream: Iterator[tuple[str, dict[str, Any]]],
        order_prefix: str = "order_lifecycle",
        courier_prefix: str = "courier_operations",
        file_prefix: str = "events",
        collect_events: tuple[list[dict[str, Any]], list[dict[str, Any]]] | None = None,
    ) -> tuple[int, int]:
        """
        Stream (feed_name, event) tuples to NDJSON files as they arrive.
        If collect_events is (order_list, courier_list), appends events for AVRO etc.
        Returns (order_count, courier_count).
        """
        order_file = self.output_dir / f"{order_prefix}_{file_prefix}_stream.ndjson"
        courier_file = self.output_dir / f"{courier_prefix}_{file_prefix}_stream.ndjson"
        order_count = courier_count = 0
        order_list, courier_list = collect_events if collect_events is not None else ([], [])
        with open(order_file, "w", encoding="utf-8") as ow, open(courier_file, "w", encoding="utf-8") as cw:
            for feed_name, evt in event_stream:
                enriched = _enrich_for_eventhub(evt)
                line = json.dumps(enriched, ensure_ascii=False) + "\n"
                if feed_name == "order_lifecycle":
                    ow.write(line)
                    ow.flush()
                    if collect_events is not None:
                        order_list.append(evt)
                    order_count += 1
                else:
                    cw.write(line)
                    cw.flush()
                    if collect_events is not None:
                        courier_list.append(evt)
                    courier_count += 1
        return order_count, courier_count


class JsonStreamWriter:
    """Writes events to NDJSON in real-time. Rotates files when max_per_file reached."""

    def __init__(
        self,
        output_dir: Path,
        feed_name: str,
        file_prefix: str = "events",
        max_per_file: int = 1000,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.feed_name = feed_name
        self.file_prefix = file_prefix
        self.max_per_file = max_per_file
        self._writer = None
        self._current_file: Path | None = None
        self._count = 0
        self._file_index = 0
        self._written: list[Path] = []

    def write(self, evt: dict[str, Any]) -> None:
        """Write one event immediately, flush. Rotates file when max_per_file reached."""
        if self._count % self.max_per_file == 0 or self._writer is None:
            self._close_current()
            self._file_index = self._count // self.max_per_file
            self._current_file = (
                self.output_dir / f"{self.feed_name}_{self.file_prefix}_{self._file_index:04d}.ndjson"
            )
            self._writer = open(self._current_file, "w", encoding="utf-8")
            self._written.append(self._current_file)
        enriched = _enrich_for_eventhub(evt)
        self._writer.write(json.dumps(enriched, ensure_ascii=False) + "\n")
        self._writer.flush()
        self._count += 1

    def _close_current(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    def close(self) -> list[Path]:
        """Close writer and return list of written file paths."""
        self._close_current()
        return self._written
