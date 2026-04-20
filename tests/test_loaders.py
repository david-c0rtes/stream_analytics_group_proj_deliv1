"""
Validation tests for Madrid dataset loaders.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest
from delivery_simulation.loaders.data_context import DataContext


def test_load_data_context():
    """Test that all Madrid datasets load correctly."""
    data_dir = Path(__file__).parent.parent / "data"
    if not data_dir.exists():
        pytest.skip("Data directory not found")

    ctx = DataContext()
    ctx.load_from_directory(data_dir, max_restaurants=100)

    assert len(ctx.zones) == 21, "Should have 21 Madrid districts"
    assert len(ctx.restaurants) <= 100
    assert ctx.travel_matrix is not None
    assert len(ctx.football_effects) > 0

    # Check zone structure
    z = ctx.zones[0]
    assert hasattr(z, "zone_id")
    assert hasattr(z, "district_name")
    assert hasattr(z, "spawn_probability")

    # Check spawn probabilities sum to ~1
    total_prob = sum(ctx.spawn_probabilities)
    assert 0.99 <= total_prob <= 1.01

    # Check travel matrix has routes
    route = ctx.get_travel_row("Z01", "Z02")
    assert route is not None
    assert "mean_time_min" in route
    assert "hop_count" in route

    # Check football multipliers
    mult = ctx.get_football_multiplier("Z05", "burger")  # Chamartín, burger
    assert mult >= 1.0


def test_get_restaurants_for_zone():
    """Test restaurant lookup by zone."""
    data_dir = Path(__file__).parent.parent / "data"
    if not data_dir.exists():
        pytest.skip("Data directory not found")

    ctx = DataContext()
    ctx.load_from_directory(data_dir, max_restaurants=200)

    restaurants = ctx.get_restaurants_for_zone("Z01")
    assert isinstance(restaurants, list)
    for r in restaurants:
        assert r["zone_id"] == "Z01"
        assert "restaurant_id" in r
        assert "category" in r
