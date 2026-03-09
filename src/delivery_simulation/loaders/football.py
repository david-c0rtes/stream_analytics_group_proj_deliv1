"""
Football match demand effects loader.

Loads demand multipliers for Real Madrid (Chamartín) and
Atlético Madrid (San Blas-Canillejas) match days from football_effects_reference.csv.
"""

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


@dataclass
class FootballEffect:
    """Demand multiplier for a category in a district during match day."""

    event_type: str
    home_team: str
    primary_district: str
    secondary_district: str
    category: str
    demand_multiplier: float


# Stadium zones: Real Madrid = Chamartín (Z05), Atlético = San Blas-Canillejas (Z20)
REAL_MADRID_STADIUM_ZONE = "Z05"  # Chamartín
ATLETICO_STADIUM_ZONE = "Z20"     # San Blas-Canillejas


def load_football_effects(csv_path: Path) -> List[FootballEffect]:
    """
    Load football match demand effects from CSV.

    Args:
        csv_path: Path to football_effects_reference.csv

    Returns:
        List of FootballEffect objects
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"Football effects file not found: {csv_path}")

    effects: List[FootballEffect] = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            effects.append(
                FootballEffect(
                    event_type=row["event_type"].strip(),
                    home_team=row["home_team"].strip(),
                    primary_district=row["primary_district"].strip(),
                    secondary_district=row["secondary_district"].strip(),
                    category=row["category"].strip(),
                    demand_multiplier=float(row["demand_multiplier"]),
                )
            )

    return effects


def build_football_multiplier_lookup(
    effects: List[FootballEffect],
    district_to_zone: Dict[str, str],
) -> Dict[Tuple[str, str], float]:
    """
    Build lookup: (zone_id, category) -> demand_multiplier.

    For primary district zones, use the multiplier directly.
    For secondary district zones, use a reduced multiplier (e.g. 0.7 * primary).
    """
    lookup: Dict[Tuple[str, str], float] = {}

    for e in effects:
        primary_zone = district_to_zone.get(e.primary_district)
        secondary_zone = district_to_zone.get(e.secondary_district)

        if primary_zone:
            key = (primary_zone, e.category)
            lookup[key] = max(lookup.get(key, 1.0), e.demand_multiplier)
        if secondary_zone:
            key = (secondary_zone, e.category)
            # Secondary gets slightly lower multiplier
            mult = e.demand_multiplier * 0.85
            lookup[key] = max(lookup.get(key, 1.0), mult)

    return lookup
