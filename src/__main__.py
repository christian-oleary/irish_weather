"""Application entry point for CLI."""

import sys

from src.fetch_weather_data import WeatherDataCollector, DATA_FORMATS

# Get the command line arguments
try:
    formats = sys.argv[1:]
except IndexError as e:
    raise ValueError(
        'Please provide at least one data format: "hourly", "daily", or "monthly"') from e

for data_format in formats:
    if data_format not in DATA_FORMATS:
        raise ValueError(f'Invalid data format: {data_format}. Options are: {DATA_FORMATS}')

# Call the fetch_data function and pass the data_formats argument
collector = WeatherDataCollector(data_formats=formats)
collector.fetch_data()
