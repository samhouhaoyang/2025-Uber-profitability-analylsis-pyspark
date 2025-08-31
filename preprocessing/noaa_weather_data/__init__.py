"""
NOAA NYC Weather Data Processing Module

Contains optimized functions for processing NOAA weather data with reduced feature set.
"""

from .process_noaa_weather_data_optimized_v2 import (
    assess_noaa_weather_data_quality,
    process_noaa_weather_data_optimized_v2,
    save_optimized_weather_to_external_features
)

from .simple_weather_processor import (
    simple_process_weather_data,
    save_simple_weather_data
)

from .copy_and_optimize_weather import (
    copy_and_optimize_existing_weather_data,
    save_optimized_copy
)

__version__ = "1.0.0"
__author__ = "Weather Data Pipeline"

__all__ = [
    'assess_noaa_weather_data_quality',
    'process_noaa_weather_data_optimized_v2',
    'save_optimized_weather_to_external_features',
    'simple_process_weather_data',
    'save_simple_weather_data'
]
