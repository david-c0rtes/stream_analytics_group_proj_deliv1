"""
Restaurants dataset loader.

Loads restaurant master data from madrid_restaurants.csv.
Restaurant distribution and category demand follow this dataset.
"""

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class Restaurant:
    """Restaurant with zone, category, prep time, and promo attributes."""

    restaurant_id: str
    name: str
    zone_id: str
    district: str
    category: str
    price_level: int
    base_prep_time_mean_min: float
    base_prep_time_std_min: float
    capacity_tier: str
    has_consistent_promo: bool
    promo_pattern: str
    matchday_sensitivity: str
    latency_pressure_factor: float

    @property
    def avg_prep_time(self) -> float:
        """Alias for base_prep_time_mean_min."""
        return self.base_prep_time_mean_min

    @property
    def promo_type(self) -> str:
        """Alias for promo_pattern."""
        return self.promo_pattern

    @property
    def match_day_sensitive(self) -> bool:
        """Whether restaurant demand is sensitive to match days."""
        return self.matchday_sensitivity.lower() == "high"


def _parse_bool(value: str) -> bool:
    """Parse string to boolean."""
    return str(value).strip().lower() in ("true", "1", "yes")


def load_restaurants(
    csv_path: Path,
    zone_ids: Optional[List[str]] = None,
    categories: Optional[List[str]] = None,
    max_restaurants: Optional[int] = None,
) -> List[Restaurant]:
    """
    Load restaurants from CSV.

    Args:
        csv_path: Path to madrid_restaurants.csv
        zone_ids: Optional filter by zone IDs
        categories: Optional filter by categories
        max_restaurants: Optional limit on number of restaurants

    Returns:
        List of Restaurant objects
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"Restaurants file not found: {csv_path}")

    restaurants: List[Restaurant] = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if zone_ids and row["zone_id"].strip() not in zone_ids:
                continue
            if categories and row["category"].strip() not in categories:
                continue

            r = Restaurant(
                restaurant_id=row["restaurant_id"].strip(),
                name=row["name"].strip(),
                zone_id=row["zone_id"].strip(),
                district=row["district"].strip(),
                category=row["category"].strip(),
                price_level=int(row["price_level"]),
                base_prep_time_mean_min=float(row["base_prep_time_mean_min"]),
                base_prep_time_std_min=float(row["base_prep_time_std_min"]),
                capacity_tier=row["capacity_tier"].strip(),
                has_consistent_promo=_parse_bool(row.get("has_consistent_promo", "false")),
                promo_pattern=row.get("promo_pattern", "none").strip(),
                matchday_sensitivity=row.get("matchday_sensitivity", "medium").strip(),
                latency_pressure_factor=float(
                    row.get("latency_pressure_factor", "1.0")
                ),
            )
            restaurants.append(r)

            if max_restaurants and len(restaurants) >= max_restaurants:
                break

    return restaurants


def get_restaurants_by_zone(restaurants: List[Restaurant]) -> Dict[str, List[Restaurant]]:
    """Group restaurants by zone_id."""
    by_zone: Dict[str, List[Restaurant]] = {}
    for r in restaurants:
        by_zone.setdefault(r.zone_id, []).append(r)
    return by_zone


def get_restaurants_by_category(
    restaurants: List[Restaurant],
) -> Dict[str, List[Restaurant]]:
    """Group restaurants by category."""
    by_cat: Dict[str, List[Restaurant]] = {}
    for r in restaurants:
        by_cat.setdefault(r.category, []).append(r)
    return by_cat
