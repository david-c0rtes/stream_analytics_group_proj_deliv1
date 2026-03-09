"""
Madrid zones and population loader.

Loads zone data from madrid_zones_population.csv.
Spawn probability is derived from population (or used directly if present).
"""

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass
class Zone:
    """Madrid district zone with population and spawn probability."""

    zone_id: str
    district_name: str
    population: int
    spawn_probability: float
    centroid_x: float = 0.0
    centroid_y: float = 0.0
    centrality_weight: float = 1.0

    def __post_init__(self) -> None:
        if self.spawn_probability < 0 or self.spawn_probability > 1:
            raise ValueError(
                f"Invalid spawn_probability {self.spawn_probability} for zone {self.zone_id}"
            )


def _parse_bool(value: str) -> bool:
    """Parse string to boolean."""
    return str(value).strip().lower() in ("true", "1", "yes")


def load_zones(csv_path: Path) -> List[Zone]:
    """
    Load Madrid zones from CSV.

    Args:
        csv_path: Path to madrid_zones_population.csv

    Returns:
        List of Zone objects

    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If data is invalid
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"Zones file not found: {csv_path}")

    zones: List[Zone] = []
    total_pop = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            zone_id = row["zone_id"].strip()
            district_name = row["district"].strip()
            population = int(row["population"])
            spawn_probability = float(row["spawn_probability"])
            centroid_x = float(row.get("centroid_x", 0))
            centroid_y = float(row.get("centroid_y", 0))
            centrality_weight = float(row.get("centrality_weight", 1.0))

            zones.append(
                Zone(
                    zone_id=zone_id,
                    district_name=district_name,
                    population=population,
                    spawn_probability=spawn_probability,
                    centroid_x=centroid_x,
                    centroid_y=centroid_y,
                    centrality_weight=centrality_weight,
                )
            )
            total_pop += population

    if not zones:
        raise ValueError("No zones loaded from file")

    # Normalize spawn probabilities to sum to 1.0 if they don't
    prob_sum = sum(z.spawn_probability for z in zones)
    if abs(prob_sum - 1.0) > 0.01:
        for z in zones:
            z.spawn_probability = z.spawn_probability / prob_sum

    return zones
