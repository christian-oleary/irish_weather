"""Application entry point for CLI."""

import sys

from loguru import logger

from src.fetch_weather_data import WeatherDataCollector, DATA_FORMATS
from src.logs import Logs

if __name__ == '__main__':
    logger.remove()
    Logs.log_to_file(
        sink='logs/irish_weather.log',
        level='DEBUG',
        backtrace=True,
        diagnose=True,
        rotation='30 MB',
        retention='14 days',
        compression='zip',
        enqueue=True,
    )

    # Get the command line arguments
    try:
        formats = sys.argv[1:]
    except IndexError as e:
        raise ValueError(
            'Please provide at least one data format: "hourly", "daily", or "monthly"'
        ) from e

    for data_format in formats:
        if data_format not in DATA_FORMATS:
            raise ValueError(f'Invalid data format: {data_format}. Options are: {DATA_FORMATS}')

    # Call the fetch_data function and pass the data_formats argument
    collector = WeatherDataCollector(data_formats=formats, enable_logging=True)
    collector.fetch_data()
