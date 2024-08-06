"""Fetch public weather data for Ireland"""

from __future__ import annotations
from pathlib import Path
from io import BytesIO
import shutil
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from zipfile import ZipFile

from loguru import logger
import pandas as pd

# Data
DATA_DIR = 'data'
SLEEP_DELAY = 5
STATION_DATA_URL = 'https://cli.fusio.net/cli/climate_data/stations.csv'

# Formats
MONTHLY = 'monthly'
DAILY = 'daily'
HOURLY = 'hourly'
DATA_FORMATS = [MONTHLY, DAILY, HOURLY]


class WeatherDataCollector:
    """Collects Met Eireann data"""

    def __init__(
        self,
        data_dir: str = DATA_DIR,
        station_url: str = STATION_DATA_URL,
        data_formats: list[str] | None = None,
        sleep_delay: int = SLEEP_DELAY,
        overwrite_files: bool = False
    ):
        if data_formats is None:
            data_formats = DATA_FORMATS

        self.data_dir = data_dir
        self.station_url = station_url
        self.data_formats = data_formats
        self.sleep_delay = sleep_delay
        self.overwrite_files = overwrite_files

        self.df_all_stations = pd.DataFrame()

    def fetch_data(self):
        """Fetch Met Eireann weather data and save to CSV files

        :param str | Path data_dir: Output data directory, defaults to 'data'
        :param str station_url: URL to Met Eireann stations data
        :param list[str] | None data_formats: Data formats to fetch
        :param int sleep_delay: Delay between requests, defaults to 3
        :param bool overwrite_files: Overwrite existing files, defaults to False
        """
        logger.info('Fetching data from Met Eireann')
        logger.debug(f'data_dir: {self.data_dir}')
        logger.debug(f'station_url: {self.station_url}')
        logger.debug(f'sleep_delay: {self.sleep_delay}')

        Path(self.data_dir).mkdir(exist_ok=True)

        # Download station data to retrieve IDs
        logger.info('Downloading station ID data...')
        df_stations = pd.read_csv(self.station_url)
        df_stations.drop('get_data', axis=1, errors='ignore', inplace=True)
        df_stations.to_csv(Path(self.data_dir, 'stations.csv'), index=False)

        # Fetch data by time format
        for data_format in self.data_formats:
            logger.info(f'Downloading {data_format} zip files...')
            # Fetch data by station
            for row in df_stations.itertuples():
                self.fetch_station_data(row, data_format)
            logger.debug(f'Data shape: {self.df_all_stations.shape}')

            # Save data to CSV
            output_path = Path(self.data_dir, f'{data_format}_all_stations.csv')
            logger.info(f'Saving data to {output_path}')
            self.df_all_stations.to_csv(output_path)

    def fetch_station_data(
        self,
        data: pd._PandasNamedTuple,
        data_format: str,
    ) -> pd.DataFrame:
        """Fetch data for a single weather station

        :param pd._PandasNamedTuple data: Station data
        :param str data_format: Data format ('hourly', 'daily', 'monthly')
        :param str | Path data_dir: Output data directory
        :param bool overwrite_files: Overwrite existing CSV files
        :param pd.DataFrame df_all_stations: Dataframe of all stations
        :raises ValueError: If invalid data format
        :return pd.DataFrame: Dataframe of all stations
        """
        data_types = [s.strip().lower() for s in data.data_types.split('|')]

        if data_format not in data_types:
            logger.debug(f'Station {data.stno} ({data.Name}) does not have {data_format} data')
            return self.df_all_stations

        name = (
            f'{data.stno}__{data.county}__{data.Name}'.strip()
            .replace(' ', '_')
            .replace('(', '_')
            .replace(')', '_')
        )
        output_dir = Path(self.data_dir, data_format, name)

        zip_url = 'https://cli.fusio.net/cli/climate_data/webdata/'
        zip_url += f'{data_format[0]}ly{data.stno}.zip'
        if self.overwrite_files or not output_dir.exists():
            self.download_zip_file(zip_url, name, data_format, output_dir)
        else:
            logger.debug(f'Skipped {name} {data_format}')

        if output_dir.exists():
            station_path = Path(output_dir, f'{data_format[0]}ly{data.stno}.csv')
            df_path = Path(str(station_path).replace('.csv', '_DATA_.csv'))
            if df_path.exists() and not self.overwrite_files:
                df_station = pd.read_csv(df_path, index_col=0)
            else:
                df_station = self.parse_csv_data(station_path, data.stno, data_format, df_path)

            if len(self.df_all_stations) == 0:
                df_all_stations = df_station.sort_index()
            else:
                df_all_stations = pd.concat([df_all_stations, df_station], axis=1).sort_index()

        return df_all_stations

    def download_zip_file(
        self, zip_url: str, name: str, data_format: str, output_dir: Path
    ) -> None:
        """Download and extract a zip file

        :param str zip_url: URL to zip file
        :param str name: Name of station
        :param str data_format: Data format ('hourly', 'daily', 'monthly')
        :param Path output_dir: Output directory
        :raises ValueError: If invalid URL
        """
        try:
            # Sleep to avoid overloading server/blacklisting/etc.
            time.sleep(SLEEP_DELAY)

            # Download and extract zip file
            if zip_url.startswith('http'):
                with urlopen(Request(zip_url)) as response:  # nosec
                    with ZipFile(BytesIO(response.read())) as zip_file:
                        zip_file.extractall(output_dir)
            else:
                raise ValueError(f'Invalid URL: {zip_url}')

            logger.debug(f'Fetched {name} {data_format} data')
        except HTTPError as error:
            logger.warning(f'Error fetching {name} ({data_format}): {error}')
            shutil.rmtree(output_dir, ignore_errors=True)

    def parse_csv_data(
        self, station_path: Path, station_id: int, data_format: str, output_path: Path
    ) -> pd.DataFrame:
        """Parse CSV data from a Met Eireann weather station CSV file

        :param Path | str station_path: Input file path
        :param str station_id: Station ID
        :param str data_format: Data format ('hourly', 'daily', 'monthly')
        :param Path | str output_path: Output file path
        :raises ValueError: If headers not found in file
        :return pd.DataFrame: Formatted dataframe
        """
        # Read CSV file
        with open(station_path, 'r', encoding='utf-8') as csv_file:
            lines = csv_file.readlines()

        # Find line where data headers start
        date_header = 'date,ind,'
        month_header = 'year,month,'
        headers_line = self.find_headers_line(lines, station_path, date_header, month_header)

        # Write data to new file
        with open(output_path, 'w', encoding='utf-8') as output_file:
            output_file.writelines(lines[headers_line:])

        # Read data to dataframe
        df = pd.read_csv(output_path)

        # Create time index
        if lines[headers_line].startswith(month_header):
            df['day'] = 1
            df['hour'] = 0
            df['minute'] = 0
            time_cols = ['year', 'month', 'day', 'hour', 'minute']
            df.index = pd.to_datetime(df[time_cols])
            df.drop(time_cols, axis=1, inplace=True)
        else:
            df = self.parse_date_col(df, data_format)

        # Sort by index, drop duplicates
        df = df.sort_index()
        df = df[~df.index.duplicated(keep='first')]

        # Rename columns and index, and save to CSV
        df.columns = [f'{station_id}__{col}' for col in df.columns]
        df.index.name = 'time'
        df.to_csv(output_path, index_label='time')
        df = pd.read_csv(output_path, index_col='time')
        return df

    def find_headers_line(self, lines, station_path, *headers):
        """Find line in file where headers are located

        :param list[str] lines: List of lines in file
        :param str station_path: Path to station file
        :param str headers: Headers to search for
        :return int: Line number where headers are located
        :raises ValueError: If headers not found in file
        """
        headers_line = -1
        for i, line in enumerate(lines):
            if line.startswith(headers):
                headers_line = i
                break

        if headers_line == -1:
            raise ValueError(f'Headers not found in {station_path}\n' + '\n'.join(lines[:40]))
        return headers_line

    def parse_date_col(self, df, data_format):
        """Attempt to parse date column in dataframe

        :param pd.DataFrame df: Input dataframe
        :param str data_format: Data format ('hourly', 'daily', 'monthly')
        :return pd.DataFrame: Formatted dataframe
        """
        df = df.set_index('date', drop=True)

        if data_format in ['daily', 'monthly']:
            format_ = '%d-%b-%Y'
        elif data_format == 'hourly':
            format_ = '%d-%b-%Y %H:%M'

        df.index = pd.date_range(start=df.index[0], periods=len(df), freq=data_format[0].upper())
        df.index = pd.to_datetime(df.index, format=format_)
        return df


if __name__ == '__main__':
    collector = WeatherDataCollector(
        data_dir=DATA_DIR,
        station_url=STATION_DATA_URL,
        data_formats=DATA_FORMATS,
        sleep_delay=SLEEP_DELAY,
        overwrite_files=False,
    )
    collector.fetch_data()
