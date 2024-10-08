"""Test fetch_weather_data.py"""

from datetime import datetime
import unittest
from pathlib import Path

import pandas as pd
from loguru import logger

from src.fetch_weather_data import WeatherDataCollector, STATION_DATA_URL
from src.logs import Logs


class TestFetchWeatherData(unittest.TestCase):
    """Test fetch_weather_data"""

    MAX_STATIONS = 3

    def setUp(self):
        """Set up test fixtures"""
        self.data_dir = 'tests/data'
        self.station_url = 'tests/data/stations_test.csv'
        self.data_formats = ['monthly', 'daily', 'hourly']
        self.sleep_delay = 1
        self.overwrite_files = True
        Logs.log_to_stderr()
        Logs.log_to_file()

        # Download station data to retrieve some test IDs
        logger.info('Downloading station ID data...')
        df_stations = pd.read_csv(STATION_DATA_URL)
        df_stations = df_stations.head(self.MAX_STATIONS)
        df_stations.drop('get_data', axis=1, errors='ignore', inplace=True)
        Path(self.data_dir).mkdir(exist_ok=True)
        df_stations.to_csv(Path(self.station_url), index=False)

    def test_fetch_data(self):
        """Test fetch_data function"""
        logger.info('Testing if station data file is downloaded...')
        self.assertTrue(Path(self.station_url).exists())

        logger.info('Testing fetch_data function...')
        collector = WeatherDataCollector(
            data_dir=self.data_dir,
            station_url=self.station_url,
            data_formats=self.data_formats,
            sleep_delay=self.sleep_delay,
            overwrite_files=self.overwrite_files,
        )
        collector.fetch_data()

        logger.info('Testing if data directory is created...')
        self.assertTrue(Path(self.data_dir).exists())

        if int(str(collector.df_all_stations.index[-1]).split('-')[0]) != datetime.now().year:
            raise ValueError('Data not fetched for the current year')

    def test_logs_initialization(self):
        """Test logs initialization"""
        Logs.log_to_stderr()
        Logs.log_to_file()
