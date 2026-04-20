"""
Travel model for zone-to-zone delivery times.

Uses the provided zone travel matrix for:
- Mean travel time and std dev
- Zone hops
- Delay probability
- Pickup cancel factor (for courier cancellation logic)
"""

from __future__ import annotations

import math
import random
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ..loaders.data_context import DataContext


class TravelModel:
    """
    Samples travel times and zone hops from the zone-to-zone matrix.
    Travel time is sampled from a log-normal or normal distribution
    using mean and std from the matrix.
    """

    def __init__(self, data_context: DataContext, rng: random.Random):
        self._ctx = data_context
        self._rng = rng

    def sample_travel_time(
        self, origin_zone_id: str, destination_zone_id: str
    ) -> tuple[float, int, float, float]:
        """
        Sample travel time from matrix distribution.
        Returns: (actual_minutes, zone_hops, delay_probability, pickup_cancel_factor)
        """
        row = self._ctx.get_travel_row(origin_zone_id, destination_zone_id)
        if not row:
            return 8.0, 0, 0.06, 1.0  # Default same-zone values

        mean_min = float(row["mean_time_min"])
        std_min = float(row["std_dev_min"])
        hops = int(row["hop_count"])
        delay_prob = float(row["base_delay_probability"])
        cancel_factor = float(row["pickup_cancel_factor"])

        # Sample from truncated normal (no negative times)
        raw = self._rng.gauss(mean_min, std_min)
        actual = max(1.0, raw)

        return actual, hops, delay_prob, cancel_factor

    def get_user_cancel_sensitivity(
        self, restaurant_zone_id: str, user_zone_id: str
    ) -> float:
        """Get user cancellation sensitivity for this OD pair."""
        row = self._ctx.get_travel_row(restaurant_zone_id, user_zone_id)
        if not row:
            return 0.9
        return float(row["user_cancel_sensitivity"])

    def get_zone_hops(self, origin_zone_id: str, destination_zone_id: str) -> int:
        """Get zone hops for OD pair."""
        row = self._ctx.get_travel_row(origin_zone_id, destination_zone_id)
        if not row:
            return 0
        return int(row["hop_count"])

    def get_route(
        self, origin_zone_id: str, destination_zone_id: str
    ) -> Optional[dict]:
        """Get travel route as dict (mean_time_min, etc.) for order generator."""
        return self._ctx.get_travel_row(origin_zone_id, destination_zone_id)

    def apply_delay(self, base_minutes: float, delay_probability: float) -> float:
        """
        With delay_probability, add 20-50% delay.
        Returns delayed minutes.
        """
        if self._rng.random() >= delay_probability:
            return base_minutes
        delay_pct = 0.2 + self._rng.random() * 0.3
        return base_minutes * (1 + delay_pct)
