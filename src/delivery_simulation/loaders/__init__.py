"""
Dataset loaders for Madrid food delivery simulation.

Loads and validates:
- Madrid zones and population
- Zone adjacency graph
- Zone-to-zone travel matrix
- Restaurants dataset
- Football demand effects
"""

from .zones import load_zones, Zone
from .restaurants import load_restaurants, Restaurant
from .travel_matrix import load_travel_matrix, TravelMatrix
from .football import load_football_effects, FootballEffect
from .data_context import DataContext

__all__ = [
    "load_zones",
    "load_restaurants",
    "load_travel_matrix",
    "load_football_effects",
    "DataContext",
    "Zone",
    "Restaurant",
    "TravelMatrix",
    "FootballEffect",
]
