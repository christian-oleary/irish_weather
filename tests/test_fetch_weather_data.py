"""Test fetch_weather_data.py"""

import unittest
from pathlib import Path

import pandas as pd

from src.fetch_weather_data import fetch_data, STATION_DATA_URL


class TestFetchWeatherData(unittest.TestCase):
    """Test fetch_weather_data"""

    MAX_STATIONS = 3

    def setUp(self):
        """Set up test fixtures"""
        self.data_dir = Path('tests/data')
        self.station_url = 'tests/data/stations_test.csv'
        self.data_formats = ['hourly', 'daily', 'monthly']
        self.sleep_delay = 1
        self.overwrite_files = True

        # Download station data to retrieve some test IDs
        df_stations = pd.read_csv(STATION_DATA_URL)
        df_stations = df_stations.head(self.MAX_STATIONS)
        df_stations.drop('get_data', axis=1, errors='ignore', inplace=True)
        df_stations.to_csv(Path(self.station_url), index=False)

    def test_fetch_data(self):
        """Test fetch_data function"""
        # Test if the data directory is created
        fetch_data(self.data_dir, self.station_url, self.data_formats, self.sleep_delay, self.overwrite_files)
        self.assertTrue(self.data_dir.exists())

        # Test if the station data file is downloaded
        station_data_file = self.data_dir / 'stations.csv'
        self.assertTrue(station_data_file.exists())
