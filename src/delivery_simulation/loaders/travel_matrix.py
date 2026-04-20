"""
Zone-to-zone travel matrix loader.

Loads travel times, zone hops, delay probability, and cancellation factors
from zone_travel_matrix.csv.
"""

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple


@dataclass
class TravelRoute:
    """Travel route between two zones."""

    origin_zone: str
    destination_zone: str
    district_from: str
    district_to: str
    hop_count: int
    distance_index: float
    mean_time_min: float
    std_dev_min: float
    base_delay_probability: float
    pickup_cancel_factor: float
    user_cancel_sensitivity: float
    same_zone: bool


class TravelMatrix:
    """
    Zone-to-zone travel matrix for delivery simulation.

    Provides lookup by (origin_zone, destination_zone).
    Column names in CSV: zone_from, zone_to, mean_time_min, std_dev_min,
    hop_count, base_delay_probability, pickup_cancel_factor, user_cancel_sensitivity.
    """

    def __init__(self) -> None:
        self._routes: Dict[Tuple[str, str], TravelRoute] = {}

    def add_route(self, route: TravelRoute) -> None:
        """Add a travel route."""
        key = (route.origin_zone, route.destination_zone)
        self._routes[key] = route

    def get_route(
        self, origin_zone: str, destination_zone: str
    ) -> Optional[TravelRoute]:
        """Get travel route for zone pair."""
        return self._routes.get((origin_zone, destination_zone))

    def get_travel_params(
        self, origin_zone: str, destination_zone: str
    ) -> Optional[
        Tuple[float, float, int, float, float, float]
    ]:
        """
        Get (mean_time_min, std_dev_min, hop_count, base_delay_prob,
        pickup_cancel_factor, user_cancel_sensitivity) for zone pair.
        """
        r = self.get_route(origin_zone, destination_zone)
        if r is None:
            return None
        return (
            r.mean_time_min,
            r.std_dev_min,
            r.hop_count,
            r.base_delay_probability,
            r.pickup_cancel_factor,
            r.user_cancel_sensitivity,
        )

    def __len__(self) -> int:
        return len(self._routes)


def _parse_bool(value: str) -> bool:
    return str(value).strip().lower() in ("true", "1", "yes")


def load_travel_matrix(csv_path: Path) -> TravelMatrix:
    """
    Load zone-to-zone travel matrix from CSV.

    Args:
        csv_path: Path to zone_travel_matrix.csv

    Returns:
        TravelMatrix instance
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"Travel matrix file not found: {csv_path}")

    matrix = TravelMatrix()

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            route = TravelRoute(
                origin_zone=row["zone_from"].strip(),
                destination_zone=row["zone_to"].strip(),
                district_from=row["district_from"].strip(),
                district_to=row["district_to"].strip(),
                hop_count=int(row["hop_count"]),
                distance_index=float(row.get("distance_index", 0)),
                mean_time_min=float(row["mean_time_min"]),
                std_dev_min=float(row["std_dev_min"]),
                base_delay_probability=float(row["base_delay_probability"]),
                pickup_cancel_factor=float(row["pickup_cancel_factor"]),
                user_cancel_sensitivity=float(row["user_cancel_sensitivity"]),
                same_zone=_parse_bool(row.get("same_zone", "false")),
            )
            matrix.add_route(route)

    return matrix
