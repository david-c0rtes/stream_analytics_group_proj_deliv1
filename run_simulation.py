#!/usr/bin/env python3
"""
CLI entry point for the Madrid food delivery streaming simulation.

Usage:
    python run_simulation.py                    # Full week (default config)
    python run_simulation.py --output output     # Custom output directory
    python run_simulation.py --duration 1440     # Override: 1 day instead of 1 week
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

# ANSI colors for live output (only when stdout is a TTY)
_R = "\033[0m"      # reset
_DIM = "\033[2m"    # dim
_B = "\033[1m"      # bold
_G = "\033[32m"     # green
_Y = "\033[33m"     # yellow
_C = "\033[36m"     # cyan
_M = "\033[35m"     # magenta


def _fmt_live_event(feed_name: str, event: dict, use_color: bool) -> str:
    """Format a single event for live console output. Columns: Date | Time | Event | Order | Courier | Feed."""
    et = event.get("event_type", "?")
    oid = event.get("order_id", "") or "-"
    cid = event.get("courier_id", "") or "-"
    ts_raw = event.get("event_time", "")
    date_str = "????-??-??"
    ts = "??:??"
    if ts_raw:
        s = str(ts_raw)
        dm = re.search(r"(\d{4}-\d{2}-\d{2})", s)
        tm = re.search(r"T(\d{2}:\d{2})", s)
        if dm:
            date_str = dm.group(1)
        if tm:
            ts = tm.group(1)

    labels = {
        "order_created": "Created",
        "order_confirmed": "Confirmed",
        "order_preparing": "Preparing",
        "order_ready_for_pickup": "Ready for pickup",
        "order_picked_up": "Picked up",
        "order_delivered": "Delivered",
        "courier_assigned_order": "Assigned",
        "courier_arrived_restaurant": "At restaurant",
        "courier_pickup_completed": "Pickup done",
        "courier_arrived_customer_zone": "At customer",
        "courier_delivery_completed": "Delivery done",
    }
    label = labels.get(et, et)
    feed_display = "Order" if feed_name == "order_lifecycle" else "Courier"

    if not use_color:
        return f"  {date_str}  {ts}   {label:<20}  {oid:12}   {cid:8}  {feed_display}"

    if feed_name == "order_lifecycle":
        if "created" in et:
            c = _G
        elif "delivered" in et:
            c = _B + _G
        elif "pickup" in et.lower() or "ready" in et:
            c = _Y
        else:
            c = _DIM
    else:
        if "delivery_completed" in et:
            c = _B + _G
        elif "assigned" in et:
            c = _C
        else:
            c = _M

    return f"  {_DIM}{date_str}{_R}  {_DIM}{ts}{_R}  {c}{label:20}{_R}  {_B}{oid}{_R}  {_DIM}{cid:8}{_R}  {_B}{feed_display}{_R}"


def _parse_event_time(evt: dict) -> datetime | None:
    """Parse event_time from event dict to datetime for sorting."""
    raw = evt.get("event_time")
    if not raw:
        return None
    s = str(raw).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _parse_sim_start(config) -> datetime:
    """Parse simulation_start from config (YYYY-MM-DD HH:MM) to UTC datetime."""
    s = config.simulation_start
    if not s:
        return datetime.now(timezone.utc)
    try:
        dt = datetime.strptime(s.strip(), "%Y-%m-%d %H:%M")
        return dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


from delivery_simulation.config import load_simulation_config
from delivery_simulation.loaders.data_context import DataContext
from delivery_simulation.engine import SimulationEngine
from delivery_simulation.serializers.json_serializer import JsonStreamWriter
from delivery_simulation.serializers.avro_serializer import AvroStreamWriter


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Madrid food delivery streaming simulation - generates order and courier event feeds"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).parent / "simulation_config.yaml",
        help="Path to simulation config YAML",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent / "output",
        help="Base output directory (JSON -> output/json, AVRO -> output/avro)",
    )
    parser.add_argument(
        "--logs",
        type=Path,
        default=Path(__file__).parent / "logs",
        help="Directory for terminal/log output (live event stream)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Override simulation duration in minutes (default: 10080 = 1 week)",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=["json", "avro"],
        default=["json", "avro"],
        help="Output formats to generate",
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Skip JSON output (deprecated, use --formats avro)",
    )
    parser.add_argument(
        "--no-avro",
        action="store_true",
        help="Skip AVRO output (deprecated, use --formats json)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Stream events to console as they are created (enables realtime throttling)",
    )
    parser.add_argument(
        "--live-json",
        action="store_true",
        help="With --live: print full JSON per event (default: compact summary)",
    )
    parser.add_argument(
        "--live-sort-by-event-time",
        action="store_true",
        help="With --live: replay sorted by event_time (default: preserve generation order)",
    )
    parser.add_argument(
        "--live-log",
        type=Path,
        default=None,
        help="With --live: custom path for event log (default: logs/live_events.log)",
    )
    args = parser.parse_args()

    if not args.config.exists():
        print(f"Error: Config file not found: {args.config}")
        return 1

    config = load_simulation_config(args.config)

    if args.duration is not None:
        config.raw.setdefault("simulation", {})["duration_minutes"] = args.duration

    if args.live:
        # Default live mode preserves generation order so out-of-order cases remain visible.
        _stream_by_time = args.live_sort_by_event_time and not args.live_json
        if not _stream_by_time:
            config.raw.setdefault("simulation", {})["realtime_mode"] = True
        mode_desc = "streaming by event time (one-by-one)" if _stream_by_time else "realtime throttling"
        print(f"Live mode: {mode_desc}")

    sim_dur = config.simulation_duration_minutes
    days = sim_dur / 1440
    print(f"Simulation: {sim_dur} min ({days:.1f} days), start {config.simulation_start or 'now'}")

    # Resolve data path
    project_root = Path(__file__).parent
    data_path = project_root / config.raw.get("datasets", {}).get("base_path", "data")
    if not data_path.exists():
        print(f"Error: Data directory not found: {data_path}")
        return 1

    # Load data context
    max_restaurants = config.raw.get("restaurants", {}).get("max_count")
    data_context = DataContext()
    data_context.load_from_directory(data_path, max_restaurants=max_restaurants)
    print(f"Loaded {len(data_context.zones)} zones, {len(data_context.restaurants)} restaurants")

    # Create engine and run
    engine = SimulationEngine(config, data_context)
    output_dir = Path(args.output)
    logs_dir = Path(args.logs)
    output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    json_dir = output_dir / "json"
    avro_dir = output_dir / "avro"
    json_dir.mkdir(parents=True, exist_ok=True)
    avro_dir.mkdir(parents=True, exist_ok=True)

    # Clean up previous JSON/AVRO/log outputs so each run starts fresh
    for f in json_dir.glob("*.ndjson"):
        try:
            f.unlink()
        except OSError:
            pass
    for f in avro_dir.glob("*.avro"):
        try:
            f.unlink()
        except OSError:
            pass
    log_path = args.live_log or (logs_dir / "live_events.log")
    if log_path.exists():
        try:
            log_path.write_text("", encoding="utf-8")
        except OSError:
            pass

    formats = list(args.formats)
    if args.no_json:
        formats = [f for f in formats if f != "json"]
    if args.no_avro:
        formats = [f for f in formats if f != "avro"]

    events_per_file = config.raw.get("output", {}).get("events_per_file", 1000)
    order_prefix = config.raw.get("output", {}).get("order_feed_prefix", "order_lifecycle")
    courier_prefix = config.raw.get("output", {}).get("courier_feed_prefix", "courier_operations")

    # Real-time stream writers (write as events are generated)
    json_order: JsonStreamWriter | None = None
    json_courier: JsonStreamWriter | None = None
    avro_order: AvroStreamWriter | None = None
    avro_courier: AvroStreamWriter | None = None
    if "json" in formats:
        json_order = JsonStreamWriter(json_dir, order_prefix, "events", events_per_file)
        json_courier = JsonStreamWriter(json_dir, courier_prefix, "events", events_per_file)
    if "avro" in formats:
        avro_order = AvroStreamWriter(avro_dir, order_prefix, "events", events_per_file)
        avro_courier = AvroStreamWriter(avro_dir, courier_prefix, "events", events_per_file)

    def _write_event(feed_name: str, event: dict) -> None:
        """Write event to JSON/AVRO streams in real time."""
        if feed_name == "order_lifecycle":
            if json_order:
                json_order.write(event)
            if avro_order:
                avro_order.write(event)
        else:
            if json_courier:
                json_courier.write(event)
            if avro_courier:
                avro_courier.write(event)

    def _close_writers() -> None:
        """Close all stream writers (flush AVRO buffers, close JSON files)."""
        if json_order:
            json_order.close()
        if json_courier:
            json_courier.close()
        if avro_order:
            avro_order.close()
        if avro_courier:
            avro_courier.close()

    order_events: list[dict] = []
    courier_events: list[dict] = []
    stream_by_time = args.live and args.live_sort_by_event_time and not args.live_json
    interrupted = False

    try:
        if stream_by_time:
            # Phase 1: Run fast, collect all events (no file writes yet)
            all_events: list[tuple[str, dict]] = []
            for feed_name, event in engine.run(duration_minutes=config.simulation_duration_minutes):
                if feed_name == "order_lifecycle":
                    order_events.append(event)
                else:
                    courier_events.append(event)
                all_events.append((feed_name, event))

            # Phase 2: Sort by event_time so they stream in real-time order
            def _sort_key(item: tuple[str, dict]) -> datetime:
                dt = _parse_event_time(item[1])
                return dt if dt else datetime.min.replace(tzinfo=timezone.utc)

            all_events.sort(key=_sort_key)

            # Phase 3: Replay at real-time speed — write to JSON, AVRO, log as each event "arrives"
            sim_start = _parse_sim_start(config)
            real_sec_per_sim_min = config.raw.get("simulation", {}).get("real_seconds_per_sim_minute", 1.0)
            use_color = sys.stdout.isatty()

            sep = "─" * 78
            h = f"  {'Date':12}  {'Time':6}  {'Event':20}  {'Order':12}  {'Courier':8}  Feed"
            banner = "Two Feeds: Order Lifecycle | Courier Operations (interleaved by event_time)"

            print(banner if not use_color else f"{_DIM}{banner}{_R}", flush=True)
            print(sep if not use_color else f"{_DIM}{sep}{_R}", flush=True)
            print(h if not use_color else f"{_DIM}{h}{_R}", flush=True)
            print(sep if not use_color else f"{_DIM}{sep}{_R}", flush=True)

            with open(log_path, "w", encoding="utf-8") as log_file:
                log_file.write(banner + "\n" + sep + "\n" + h + "\n" + sep + "\n")
                log_file.flush()

                replay_start = time.monotonic()
                for feed_name, event in all_events:
                    evt_time = _parse_event_time(event)
                    if evt_time:
                        delay_sec = (evt_time - sim_start).total_seconds() * (real_sec_per_sim_min / 60.0)
                        elapsed = time.monotonic() - replay_start
                        if delay_sec > elapsed and delay_sec > 0:
                            time.sleep(min(delay_sec - elapsed, 3600))  # cap sleep at 1h
                    # Write to JSON, AVRO, log at same speed as console display
                    _write_event(feed_name, event)
                    log_file.write(_fmt_live_event(feed_name, event, use_color=False) + "\n")
                    log_file.flush()
                    line = _fmt_live_event(feed_name, event, use_color)
                    print(line, flush=True)
        else:
            # Normal or live-json: collect, write in real time, optionally print
            if args.live and args.live_json:
                for feed_name, event in engine.run(duration_minutes=config.simulation_duration_minutes):
                    if feed_name == "order_lifecycle":
                        order_events.append(event)
                    else:
                        courier_events.append(event)
                    _write_event(feed_name, event)
                    out = {"feed": feed_name, **event}
                    print(json.dumps(out), flush=True)
            else:
                for feed_name, event in engine.run(duration_minutes=config.simulation_duration_minutes):
                    if feed_name == "order_lifecycle":
                        order_events.append(event)
                    else:
                        courier_events.append(event)
                    _write_event(feed_name, event)
                    if args.live:
                        use_color = sys.stdout.isatty()
                        line = _fmt_live_event(feed_name, event, use_color)
                        print(line, flush=True)
    except KeyboardInterrupt:
        interrupted = True
        print("\nInterrupted (Ctrl+C). Saving collected data...", flush=True)

    _close_writers()
    order_n, courier_n = len(order_events), len(courier_events)
    if interrupted:
        print(f"Saved {order_n} order events, {courier_n} courier events -> {output_dir}", flush=True)
    else:
        print(f"Generated {order_n} order events, {courier_n} courier events", flush=True)
    if "json" in formats and json_order:
        print(f"JSON: {json_dir}", flush=True)
    if "avro" in formats and avro_order:
        print(f"AVRO: {avro_dir}", flush=True)
    if stream_by_time:
        print(f"Log: {logs_dir.absolute()}", flush=True)
    print(f"Output: {output_dir.absolute()}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
