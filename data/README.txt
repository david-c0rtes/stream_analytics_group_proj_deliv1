# Madrid food-delivery reference datasets

This package contains simulation-ready CSV files for a Madrid food-delivery streaming project.

## Files
- madrid_zones_population.csv — 21 Madrid districts used as delivery zones
- zone_adjacency.csv — district adjacency graph
- zone_travel_matrix.csv — 21x21 origin-destination matrix with hop count, mean travel time, delay probability and cancellation sensitivity modifiers
- madrid_restaurants.csv — synthetic but realistic restaurant master dataset (2000 rows)
- football_effects_reference.csv — starter multipliers for Real Madrid and Atlético de Madrid match-day demand

## Notes
- Zones are based on Madrid's 21 official districts.
- Population values are rounded district-level counts designed for simulation and aligned to recent Madrid open-data magnitudes.
- Restaurant data is synthetic. It is not an official registry and should be used as master/reference data for your generator.
- The travel matrix is a realistic engineering approximation based on district adjacency, relative position and congestion penalties.

