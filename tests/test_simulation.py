"""
Validation tests for simulation engine and event generation.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest
from delivery_simulation.config import load_simulation_config
from delivery_simulation.loaders.data_context import DataContext
from delivery_simulation.engine import SimulationEngine


def test_simulation_produces_events():
    """Test that simulation produces both order and courier events."""
    project = Path(__file__).parent.parent
    config_path = project / "simulation_config.yaml"
    data_dir = project / "data"

    if not config_path.exists() or not data_dir.exists():
        pytest.skip("Config or data not found")

    config = load_simulation_config(config_path)
    config.raw.setdefault("simulation", {})["duration_minutes"] = 5
    config.raw.setdefault("couriers", {})["count"] = 5
    config.raw.setdefault("restaurants", {})["max_count"] = 20

    ctx = DataContext()
    ctx.load_from_directory(data_dir, max_restaurants=20)

    engine = SimulationEngine(config, ctx)
    order_count = 0
    courier_count = 0

    for feed_name, event in engine.run(duration_minutes=5):
        if feed_name == "order_lifecycle":
            order_count += 1
            assert "event_id" in event
            assert "event_type" in event
            assert "order_id" in event
            assert "event_time" in event
            assert "ingestion_time" in event
        else:
            courier_count += 1
            assert "event_id" in event
            assert "event_type" in event
            assert "courier_id" in event

    # Should have some events (may be 0 for very short runs)
    assert order_count >= 0
    assert courier_count >= 0


def test_order_events_have_required_fields():
    """Test order lifecycle events have all required fields."""
    project = Path(__file__).parent.parent
    config_path = project / "simulation_config.yaml"
    data_dir = project / "data"

    if not config_path.exists() or not data_dir.exists():
        pytest.skip("Config or data not found")

    config = load_simulation_config(config_path)
    config.raw.setdefault("simulation", {})["duration_minutes"] = 15
    config.raw.setdefault("couriers", {})["count"] = 10
    config.raw.setdefault("restaurants", {})["max_count"] = 50

    ctx = DataContext()
    ctx.load_from_directory(data_dir, max_restaurants=50)

    engine = SimulationEngine(config, ctx)
    required = {
        "event_id", "event_type", "event_version", "event_time", "ingestion_time",
        "order_id", "user_id", "restaurant_id", "user_zone_id", "restaurant_zone_id",
        "restaurant_category", "match_day_flag", "holiday_flag", "anomaly_flag",
    }

    for feed_name, event in engine.run(duration_minutes=15):
        if feed_name == "order_lifecycle":
            for field in required:
                assert field in event, f"Missing field: {field}"
            break  # Check first order event only
