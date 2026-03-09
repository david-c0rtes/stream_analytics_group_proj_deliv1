"""
Unified data context that loads and holds all Madrid datasets.

Provides a single entry point for simulation to access zones, restaurants,
travel matrix, and football effects.
"""

from pathlib import Path
from typing import Dict, List, Optional

from .zones import Zone, load_zones
from .restaurants import Restaurant, load_restaurants, get_restaurants_by_zone
from .travel_matrix import TravelMatrix, load_travel_matrix
from .football import (
    FootballEffect,
    load_football_effects,
    build_football_multiplier_lookup,
)


class DataContext:
    """
    Holds all loaded Madrid datasets for simulation.

    Use load_from_directory() to populate from a data directory.
    """

    def __init__(self) -> None:
        self.zones: List[Zone] = []
        self.restaurants: List[Restaurant] = []
        self.travel_matrix: Optional[TravelMatrix] = None
        self.football_effects: List[FootballEffect] = []
        self._district_to_zone: Dict[str, str] = {}
        self._restaurants_by_zone: Dict[str, List[Restaurant]] = {}
        self._football_multipliers: Dict[tuple, float] = {}

    def load_from_directory(
        self,
        data_dir: Path,
        max_restaurants: Optional[int] = None,
    ) -> None:
        """
        Load all datasets from a directory.

        Expected files:
        - madrid_zones_population.csv
        - zone_adjacency.csv (loaded but not yet used in simulation logic)
        - zone_travel_matrix.csv
        - madrid_restaurants.csv
        - football_effects_reference.csv

        Args:
            data_dir: Path to directory containing CSV files
            max_restaurants: Optional limit on restaurants to load
        """
        self.zones = load_zones(data_dir / "madrid_zones_population.csv")
        self._district_to_zone = {z.district_name: z.zone_id for z in self.zones}

        self.restaurants = load_restaurants(
            data_dir / "madrid_restaurants.csv",
            zone_ids=[z.zone_id for z in self.zones],
            max_restaurants=max_restaurants,
        )
        self._restaurants_by_zone = get_restaurants_by_zone(self.restaurants)

        self.travel_matrix = load_travel_matrix(data_dir / "zone_travel_matrix.csv")
        self.football_effects = load_football_effects(
            data_dir / "football_effects_reference.csv"
        )
        self._football_multipliers = build_football_multiplier_lookup(
            self.football_effects, self._district_to_zone
        )

    @property
    def zone_ids(self) -> List[str]:
        """List of zone IDs."""
        return [z.zone_id for z in self.zones]

    @property
    def spawn_probabilities(self) -> List[float]:
        """Spawn probability per zone (same order as zone_ids)."""
        return [z.spawn_probability for z in self.zones]

    def district_to_zone(self, district: str) -> Optional[str]:
        """Map district name to zone_id."""
        return self._district_to_zone.get(district)

    def zone_to_district(self, zone_id: str) -> Optional[str]:
        """Map zone_id to district name."""
        for z in self.zones:
            if z.zone_id == zone_id:
                return z.district_name
        return None

    def get_restaurants_in_zone(self, zone_id: str) -> List[Restaurant]:
        """Get restaurants in a zone."""
        return self._restaurants_by_zone.get(zone_id, [])

    def get_football_multiplier(self, zone_id: str, category: str) -> float:
        """Get demand multiplier for zone+category during match day. 1.0 if none."""
        return self._football_multipliers.get((zone_id, category), 1.0)

    def get_travel_params(
        self, origin_zone: str, destination_zone: str
    ) -> Optional[tuple]:
        """Get travel parameters for zone pair."""
        if self.travel_matrix is None:
            return None
        return self.travel_matrix.get_travel_params(origin_zone, destination_zone)

    def get_zone(self, zone_id: str) -> Optional[dict]:
        """Get zone as dict for simulation models."""
        for z in self.zones:
            if z.zone_id == zone_id:
                return {
                    "zone_id": z.zone_id,
                    "district_name": z.district_name,
                    "population": z.population,
                    "spawn_probability": z.spawn_probability,
                }
        return None

    def get_restaurants_for_zone(self, zone_id: str) -> List[dict]:
        """
        Get restaurants in zone (and optionally adjacent zones) as dicts.
        For now returns restaurants in zone only.
        """
        restaurants = self._restaurants_by_zone.get(zone_id, [])
        return [_restaurant_to_dict(r) for r in restaurants]

    def get_travel_row(
        self, origin_zone: str, destination_zone: str
    ) -> Optional[dict]:
        """Get travel row as dict for TravelModel."""
        if self.travel_matrix is None:
            return None
        route = self.travel_matrix.get_route(origin_zone, destination_zone)
        if route is None:
            return None
        return {
            "mean_time_min": route.mean_time_min,
            "std_dev_min": route.std_dev_min,
            "hop_count": route.hop_count,
            "base_delay_probability": route.base_delay_probability,
            "pickup_cancel_factor": route.pickup_cancel_factor,
            "user_cancel_sensitivity": route.user_cancel_sensitivity,
        }

    def get_football_demand_multiplier(
        self,
        zone_id: str,
        category: str,
        match_primary_zones: set,
        match_secondary_zones: set,
    ) -> float:
        """Get demand multiplier for zone+category during match day."""
        return self._football_multipliers.get((zone_id, category), 1.0)


def _restaurant_to_dict(r: Restaurant) -> dict:
    """Convert Restaurant to dict for simulation."""
    return {
        "restaurant_id": r.restaurant_id,
        "name": r.name,
        "zone_id": r.zone_id,
        "district": r.district,
        "category": r.category,
        "price_level": r.price_level,
        "base_prep_time_mean_min": r.base_prep_time_mean_min,
        "base_prep_time_std_min": r.base_prep_time_std_min,
        "capacity_tier": r.capacity_tier,
        "has_consistent_promo": r.has_consistent_promo,
        "promo_pattern": r.promo_pattern,
        "matchday_sensitivity": r.matchday_sensitivity,
        "latency_pressure_factor": r.latency_pressure_factor,
    }
