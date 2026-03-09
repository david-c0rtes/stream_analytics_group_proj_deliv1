"""
Cancellation logic for user, restaurant, and courier.

User: probability increases with wait time and delivery ETA.
Courier: probability increases with zone hops and travel time to restaurant.
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..loaders.data_context import DataContext


class CancellationModel:
    """
    Models cancellation probabilities for users, restaurants, and couriers.
    """

    # Base cancellation rates (per minute of excess wait)
    USER_BASE_RATE = 0.002
    USER_ETA_SENSITIVITY = 0.0015  # per minute of ETA
    COURIER_BASE_RATE = 0.001
    COURIER_HOP_SENSITIVITY = 0.02  # per zone hop
    COURIER_TIME_SENSITIVITY = 0.008  # per minute of travel to restaurant
    RESTAURANT_BASE_RATE = 0.005  # low baseline

    def __init__(self, data_context: DataContext, rng: random.Random):
        self._ctx = data_context
        self._rng = rng

    def user_cancels(
        self,
        wait_time_minutes: float,
        estimated_delivery_minutes: float,
        user_cancel_sensitivity: float = 1.0,
    ) -> bool:
        """
        User cancellation increases with wait time and delivery ETA.
        """
        prob = (
            self.USER_BASE_RATE * wait_time_minutes * user_cancel_sensitivity
            + self.USER_ETA_SENSITIVITY * estimated_delivery_minutes * user_cancel_sensitivity
        )
        return self._rng.random() < min(0.3, prob)

    def courier_cancels(
        self,
        zone_hops_to_restaurant: int,
        travel_time_to_restaurant_minutes: float,
        pickup_cancel_factor: float = 1.0,
    ) -> bool:
        """
        Courier cancellation increases with zone hops and travel time.
        """
        prob = (
            self.COURIER_BASE_RATE
            + self.COURIER_HOP_SENSITIVITY * zone_hops_to_restaurant * pickup_cancel_factor
            + self.COURIER_TIME_SENSITIVITY
            * travel_time_to_restaurant_minutes
            * pickup_cancel_factor
        )
        return self._rng.random() < min(0.25, prob)

    def restaurant_cancels(self, order_age_minutes: float) -> bool:
        """
        Restaurant cancellation (capacity, etc.) - low baseline.
        """
        prob = self.RESTAURANT_BASE_RATE + 0.001 * order_age_minutes
        return self._rng.random() < min(0.05, prob)
