"""
Demand model for order generation.

Handles:
- Lunch and dinner peaks
- Weekday vs weekend differences
- Holiday demand shifts
- Zone-based spawn probabilities (from population)
- Restaurant category distributions
- Football match demand surges
- Promotion effects
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime, time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..loaders.data_context import DataContext


@dataclass
class DemandContext:
    """Context for demand calculation at a point in time."""

    dt: datetime
    zone_id: str
    restaurant_category: str
    match_day: bool
    match_zone_primary: bool
    match_zone_secondary: bool
    holiday: bool
    promo_active: bool
    promo_category_match: bool
    match_category_multiplier: float = 1.0


class DemandModel:
    """
    Computes order spawn rates and category selection based on time, zone,
    and external factors (football, holidays, promotions).
    """

    # Lunch peak: 12:00-14:30, Dinner peak: 19:30-22:00
    LUNCH_START = time(12, 0)
    LUNCH_END = time(14, 30)
    DINNER_START = time(19, 30)
    DINNER_END = time(22, 0)

    # Base hourly rates (orders per hour per 1000 population)
    BASE_RATE = 0.8
    LUNCH_PEAK_MULT = 2.5
    DINNER_PEAK_MULT = 2.2
    WEEKEND_MULT = 1.3
    HOLIDAY_MULT = 1.4
    PROMO_MULT = 1.25

    def __init__(self, data_context: DataContext, config: dict, rng: random.Random):
        self._ctx = data_context
        self._config = config
        self._rng = rng
        demand_cfg = config.get("demand", {})
        self._demand_multiplier = demand_cfg.get("base_orders_per_minute", 2.0) / 2.0
        # Multiplier ranges: [min, max] sampled uniformly each call
        self._lunch_range = tuple(demand_cfg.get("lunch_peak_multiplier_range", [2.0, 3.0]))
        self._dinner_range = tuple(demand_cfg.get("dinner_peak_multiplier_range", [1.8, 2.6]))
        self._weekend_range = tuple(demand_cfg.get("weekend_multiplier_range", [1.1, 1.5]))
        self._holiday_range = tuple(demand_cfg.get("holiday_multiplier_range", [1.2, 1.7]))
        self._spawn_jitter = demand_cfg.get("spawn_jitter", 0.12)
        self._smooth_time_curve = demand_cfg.get("smooth_time_curve", True)
        # Parse holidays: support "holidays" (YYYY-MM-DD) or "holiday_calendar" ((month, day))
        holidays = set()
        for d in config.get("holidays", []) or config.get("holiday_calendar", []):
            if isinstance(d, str) and len(d.split("-")) >= 3:
                parts = d.split("-")
                holidays.add((int(parts[1]), int(parts[2])))
            elif isinstance(d, (list, tuple)) and len(d) >= 2:
                holidays.add((int(d[0]), int(d[1])))
        self._holidays = holidays

    def _gaussian(self, x: float, peak: float, sigma: float) -> float:
        """Gaussian bump centered at peak with width sigma."""
        return math.exp(-0.5 * ((x - peak) / sigma) ** 2)

    def _get_smooth_peak_multiplier(self, dt: datetime) -> float:
        """
        Smooth time-of-day curve: Gaussian bumps at lunch (~13:15) and dinner (~20:45).
        No hard boundaries; demand ramps up and down gradually.
        """
        minutes_from_midnight = dt.hour * 60 + dt.minute
        lunch_peak_min = 13 * 60 + 15  # 13:15
        dinner_peak_min = 20 * 60 + 45  # 20:45
        sigma = 75  # minutes; controls width of each peak (~2.5h effective)
        lunch_mult = self._rng.uniform(*self._lunch_range)
        dinner_mult = self._rng.uniform(*self._dinner_range)
        lunch_contrib = (lunch_mult - 1.0) * self._gaussian(minutes_from_midnight, lunch_peak_min, sigma)
        dinner_contrib = (dinner_mult - 1.0) * self._gaussian(minutes_from_midnight, dinner_peak_min, sigma)
        return 1.0 + lunch_contrib + dinner_contrib

    def _get_step_peak_multiplier(self, dt: datetime) -> float:
        """Legacy step-based peaks: fixed multiplier inside lunch/dinner windows."""
        t = dt.time()
        if self.LUNCH_START <= t <= self.LUNCH_END:
            return self._rng.uniform(*self._lunch_range)
        if self.DINNER_START <= t <= self.DINNER_END:
            return self._rng.uniform(*self._dinner_range)
        return 1.0

    def is_peak_hour(self, dt: datetime) -> tuple[bool, float]:
        """
        Returns (is_peak, multiplier). Kept for compatibility.
        Uses smooth or step curve based on config.
        """
        mult = self._get_smooth_peak_multiplier(dt) if self._smooth_time_curve else self._get_step_peak_multiplier(dt)
        is_peak = mult > 1.1
        return is_peak, mult

    def is_weekend(self, dt: datetime) -> bool:
        """Saturday=5, Sunday=6."""
        return dt.weekday() >= 5

    def is_holiday(self, dt: datetime) -> bool:
        """Check if date is in holiday calendar."""
        return (dt.month, dt.day) in self._holidays

    def get_time_multiplier(self, dt: datetime) -> float:
        """Combined time-based demand multiplier. Samples from config ranges."""
        _, peak_mult = self.is_peak_hour(dt)
        weekend_mult = self._rng.uniform(*self._weekend_range) if self.is_weekend(dt) else 1.0
        holiday_mult = self._rng.uniform(*self._holiday_range) if self.is_holiday(dt) else 1.0
        return peak_mult * weekend_mult * holiday_mult * self._demand_multiplier

    def should_spawn_order(self, dt: datetime, zone_id: str) -> bool:
        """
        Decide if an order should spawn in this zone at this time.
        Uses zone spawn_probability from population data.
        """
        zone = self._ctx.get_zone(zone_id)
        if not zone:
            return False
        prob = float(zone["spawn_probability"])
        time_mult = self.get_time_multiplier(dt)
        # Scale probability by time (higher during peaks)
        spawn_prob = min(0.99, prob * time_mult * 0.15)
        # Per-minute jitter: multiply by uniform(1 - jitter, 1 + jitter)
        jitter = self._rng.uniform(1.0 - self._spawn_jitter, 1.0 + self._spawn_jitter)
        spawn_prob = min(0.99, spawn_prob * jitter)
        return self._rng.random() < spawn_prob

    def select_restaurant(
        self,
        zone_id: str,
        dt: datetime,
        match_day: bool,
        match_primary_zones: set[str],
        match_secondary_zones: set[str],
        active_promos: dict[str, set[str]],
    ) -> dict | None:
        """
        Select a restaurant for an order. Considers:
        - Restaurants in zone and adjacent zones
        - Category distribution
        - Football match demand (burger, pizza, fast_food, mexican surge)
        - Promotion effects
        """
        restaurants = self._ctx.get_restaurants_for_zone(zone_id)
        if not restaurants:
            return None

        # Build weights per restaurant
        weights = []
        for r in restaurants:
            w = 1.0
            cat = r["category"]

            # Football match surge for specific categories
            if match_day:
                mult = self._ctx.get_football_demand_multiplier(
                    zone_id, cat, match_primary_zones, match_secondary_zones
                )
                w *= mult

            # Promotion boost
            for promo_cats in active_promos.values():
                if cat in promo_cats:
                    w *= self.PROMO_MULT
                    break

            # Match-day sensitivity (restaurants with high sensitivity get more orders)
            sens = r.get("matchday_sensitivity", "medium")
            if match_day and sens == "high":
                w *= 1.2
            elif match_day and sens == "low":
                w *= 0.9

            weights.append(max(0.01, w))

        total = sum(weights)
        if total <= 0:
            return self._rng.choice(restaurants)

        rnd = self._rng.random() * total
        for r, w in zip(restaurants, weights):
            rnd -= w
            if rnd <= 0:
                return r
        return restaurants[-1]

    def get_active_promos(self, dt: datetime) -> dict[str, set[str]]:
        """
        Return active promotions: {promo_name: set of categories}.
        Includes recurring (e.g. Taco Tuesday) and random promotions.
        """
        promos = {}
        promo_cfg = self._config.get("promotions", {})
        config_promos = promo_cfg.get("recurring", []) or self._config.get("promo_periods", []) or self._config.get("promotions", {}).get("recurring", [])

        for p in config_promos:
            name = p.get("name", "")
            categories = set(p.get("categories", []))
            if not categories:
                continue
            # Recurring by weekday: support weekdays list or day_of_week single value
            weekdays = p.get("weekdays", [])
            if not weekdays and "day_of_week" in p:
                weekdays = [p["day_of_week"]]
            if weekdays and dt.weekday() not in weekdays:
                continue
            # Time window
            start = p.get("start_hour", 0)
            end = p.get("end_hour", 24)
            if not (start <= dt.hour < end):
                continue
            promos[name] = categories

        return promos
