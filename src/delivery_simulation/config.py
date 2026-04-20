"""
Configuration loader for the delivery simulation.

Loads YAML configuration and provides typed access to all simulation parameters.
Nothing is hardcoded; all values come from config or datasets.
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import yaml


@dataclass
class SimulationConfig:
    """Typed simulation configuration built from YAML."""

    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def simulation_duration_minutes(self) -> int:
        return self.raw.get("simulation", {}).get("duration_minutes", 1440)

    @property
    def tick_interval_minutes(self) -> int:
        return self.raw.get("simulation", {}).get("tick_interval_minutes", 1)

    @property
    def seed(self) -> int:
        return self.raw.get("simulation", {}).get("seed", 42)

    @property
    def num_couriers(self) -> int:
        return self.raw.get("couriers", {}).get("count", 150)

    def _get_distribution(self, key: str, default_mean: float, default_std: float) -> tuple[float, float]:
        """Get (mean, std) from config. Supports scalar (use as mean, std=0) or {mean, std} dict."""
        val = self.raw.get("streaming_edge_cases", {}).get(key)
        if isinstance(val, (int, float)):
            return float(val), 0.0
        if isinstance(val, dict):
            return float(val.get("mean", default_mean)), float(val.get("std", default_std))
        return default_mean, default_std

    def _sample_probability(
        self,
        key: str,
        rng: Any,
        default_mean: float,
        default_std: float,
        default_alpha: float,
        default_beta: float,
    ) -> float:
        cfg = self.raw.get("streaming_edge_cases", {}).get(key)
        if isinstance(cfg, (int, float)):
            return max(0.0, min(1.0, float(cfg)))
        if isinstance(cfg, dict):
            dist_type = (cfg.get("type") or "normal").lower()
            if dist_type == "beta":
                alpha = float(cfg.get("alpha", default_alpha))
                beta = float(cfg.get("beta", default_beta))
                return max(0.0, min(1.0, rng.betavariate(max(0.001, alpha), max(0.001, beta))))
            mean = float(cfg.get("mean", default_mean))
            std = float(cfg.get("std", default_std))
            if std <= 0:
                return max(0.0, min(1.0, mean))
            return max(0.0, min(1.0, rng.gauss(mean, std)))
        return default_mean

    @property
    def late_event_probability(self) -> float:
        """Legacy: returns mean. Use sample_late_event_probability() for distribution sampling."""
        mean, _ = self._get_distribution("late_event_probability", 0.02, 0.006)
        return mean

    def sample_late_event_probability(self, rng: Any) -> float:
        """Sample probability. Supports: beta (alpha, beta), normal (mean, std), or scalar."""
        return self._sample_probability("late_event_probability", rng, 0.02, 0.006, 2, 98)

    def sample_late_event_delay_seconds(self, rng: Any) -> int:
        """Sample delay. Supports: exponential (mean), normal (mean, std), or legacy min/max."""
        cfg = self.raw.get("streaming_edge_cases", {}).get("late_event_delay_seconds")
        if isinstance(cfg, dict):
            dist_type = (cfg.get("type") or "normal").lower()
            min_val = int(cfg.get("min", 10))
            max_val = int(cfg.get("max", 300))
            if dist_type == "exponential":
                mean = float(cfg.get("mean", 120))
                rate = 1.0 / max(1.0, mean)
                sample = rng.expovariate(rate)
                return int(max(min_val, min(max_val, sample)))
            # normal
            mean = float(cfg.get("mean", 120))
            std = float(cfg.get("std", 60))
            if std <= 0:
                return int(max(min_val, min(max_val, mean)))
            return int(max(min_val, min(max_val, rng.gauss(mean, std))))
        # Legacy: uniform between min and max
        ec = self.raw.get("streaming_edge_cases", {})
        min_val = ec.get("late_event_min_delay_seconds", 60)
        max_val = ec.get("late_event_max_delay_seconds", 300)
        return rng.randint(min_val, max_val)

    @property
    def late_event_min_delay_seconds(self) -> int:
        return self.raw.get("streaming_edge_cases", {}).get("late_event_delay_seconds", {}).get("min", 10)

    @property
    def late_event_max_delay_seconds(self) -> int:
        return self.raw.get("streaming_edge_cases", {}).get("late_event_delay_seconds", {}).get("max", 300)

    @property
    def duplicate_event_probability(self) -> float:
        """Legacy: returns mean. Use sample_duplicate_event_probability() for distribution sampling."""
        mean, _ = self._get_distribution("duplicate_event_probability", 0.005, 0.002)
        return mean

    def sample_duplicate_event_probability(self, rng: Any) -> float:
        """Sample probability. Supports: beta (alpha, beta), normal (mean, std), or scalar."""
        return self._sample_probability("duplicate_event_probability", rng, 0.005, 0.002, 1, 199)

    @property
    def anomaly_injection_probability(self) -> float:
        mean, _ = self._get_distribution("anomaly_injection_probability", 0.002, 0.001)
        return mean

    def sample_anomaly_injection_probability(self, rng: Any) -> float:
        return self._sample_probability("anomaly_injection_probability", rng, 0.002, 0.001, 1, 499)

    @property
    def out_of_order_probability(self) -> float:
        mean, _ = self._get_distribution("out_of_order_probability", 0.01, 0.005)
        return mean

    def sample_out_of_order_probability(self, rng: Any) -> float:
        return self._sample_probability("out_of_order_probability", rng, 0.01, 0.005, 1, 99)

    @property
    def courier_offline_mid_delivery_probability(self) -> float:
        mean, _ = self._get_distribution("courier_offline_mid_delivery_probability", 0.01, 0.005)
        return mean

    def sample_courier_offline_mid_delivery_probability(self, rng: Any) -> float:
        return self._sample_probability("courier_offline_mid_delivery_probability", rng, 0.01, 0.005, 1, 99)

    @property
    def missing_step_probability(self) -> float:
        mean, _ = self._get_distribution("missing_step_probability", 0.01, 0.005)
        return mean

    def sample_missing_step_probability(self, rng: Any) -> float:
        return self._sample_probability("missing_step_probability", rng, 0.01, 0.005, 1, 99)

    @property
    def realtime_mode(self) -> bool:
        """If True, throttle simulation to wall-clock so events arrive in real time."""
        return self.raw.get("simulation", {}).get("realtime_mode", False)

    @property
    def real_seconds_per_sim_minute(self) -> float:
        """Wall-clock seconds per simulated minute. 0.0417 = 1 real min = 1 sim day."""
        return self.raw.get("simulation", {}).get("real_seconds_per_sim_minute", 0.04)

    @property
    def simulation_start(self) -> str | None:
        """Start datetime (YYYY-MM-DD HH:MM). Enables weekday/weekend and lunch/dinner distribution."""
        return self.raw.get("simulation", {}).get("simulation_start")


def generate_random_football_schedule(
    config: dict,
    simulation_start: datetime,
    rng: Any,
) -> list[dict[str, Any]]:
    """
    Generate one random match per team per week when random_schedule is True.

    Each match gets a random day_of_week (0=Mon, 6=Sun) and random start_time
    within start_time_range. Returns list of match dicts with home_team,
    primary_zone, day_of_week, start_time, duration_minutes.
    """
    fm = config.get("football_matches", {})
    if not fm.get("random_schedule"):
        return config.get("football_matches", []) if isinstance(
            config.get("football_matches"), list
        ) else []

    teams = fm.get("teams", [])
    if not teams:
        return []

    time_range = fm.get("start_time_range", ["16:00", "21:00"])
    duration = fm.get("duration_minutes", 120)

    def parse_time(s: str) -> int:
        parts = str(s).strip().split(":")
        h = int(parts[0]) if parts else 16
        m = int(parts[1]) if len(parts) > 1 else 0
        return h * 60 + m

    try:
        start_min = parse_time(time_range[0] if isinstance(time_range, (list, tuple)) else "16:00")
        end_min = parse_time(time_range[1] if isinstance(time_range, (list, tuple)) and len(time_range) > 1 else "21:00")
    except (ValueError, TypeError):
        start_min, end_min = 16 * 60, 21 * 60

    matches = []
    for t in teams:
        day_of_week = rng.randint(0, 6)
        mins = rng.randint(start_min, end_min) if end_min >= start_min else start_min
        h, m = divmod(mins, 60)
        start_time = f"{h:02d}:{m:02d}"
        matches.append({
            "home_team": t.get("home_team", ""),
            "primary_zone": t.get("primary_zone", ""),
            "day_of_week": day_of_week,
            "start_time": start_time,
            "duration_minutes": duration,
        })
    return matches


def load_config(config_path: Path) -> dict[str, Any]:
    """Load simulation configuration from YAML file."""
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config or {}


def resolve_dataset_path(base_path: str, filename: str, project_root: Optional[Path] = None) -> Path:
    """Resolve dataset file path. Supports relative and absolute paths."""
    path = Path(base_path) / filename
    if not path.is_absolute() and project_root:
        path = project_root / path
    return path


def get_config_value(config: dict, *keys: str, default: Any = None) -> Any:
    """Safely get nested config value."""
    value = config
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default
    return value


def load_simulation_config(config_path: Path) -> SimulationConfig:
    """Load YAML and return SimulationConfig."""
    raw = load_config(config_path)
    return SimulationConfig(raw=raw)
