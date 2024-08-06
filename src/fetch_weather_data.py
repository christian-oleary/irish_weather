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
HOURLY = 'hourly'
DAILY = 'daily'
MONTHLY = 'monthly'
data_formats = [HOURLY, DAILY, MONTHLY]


def fetch_data(
    data_dir: str | Path = DATA_DIR,
    station_url: str = STATION_DATA_URL,
    data_formats: list[str] = data_formats,
    sleep_delay: int = SLEEP_DELAY,
    overwrite_files: bool = False,
):
    """Fetch Met Eireann weather data and save to CSV files

    :param str | Path data_dir: Output data directory, defaults to 'data'
    :param str station_url: URL to Met Eireann stations data
    :param list[str] data_formats: Data formats to fetch, defaults to ['hourly', 'daily', 'monthly']
    :param int sleep_delay: Delay between requests, defaults to 3
    :param bool overwrite_files: Overwrite existing files, defaults to False
    """
    logger.info('Fetching data from Met Eireann')
    logger.debug(f'data_dir: {data_dir}')
    logger.debug(f'station_url: {station_url}')
    logger.debug(f'sleep_delay: {sleep_delay}')

    Path(data_dir).mkdir(exist_ok=True)

    # Download station data to retrieve IDs
    logger.info('Downloading station ID data...')
    df_stations = pd.read_csv(station_url)
    df_stations.drop('get_data', axis=1).to_csv(Path(data_dir, 'stations.csv'), index=False)

    # Fetch data by time format
    for data_format in data_formats:
        logger.info(f'Downloading {data_format} zip files...')
        dataframes = []
        df_all = None

        # Fetch data by station
        for i, row in enumerate(df_stations.itertuples()):
            station_id = row.stno
            county = row.county
            data_types = [s.strip().lower() for s in row.data_types.split('|')]
            name = row.Name

            if data_format not in data_types:
                logger.debug(f'Station {station_id} ({name}) does not have {data_format} data')
                continue

            name = (
                f'{station_id}__{county}__{name}'.strip()
                .replace(' ', '_')
                .replace('(', '_')
                .replace(')', '_')
            )
            output_dir = Path(data_dir, data_format, name)

            zip_url = 'https://cli.fusio.net/cli/climate_data/webdata/'
            zip_url += f'{data_format[0]}ly{station_id}.zip'
            if overwrite_files or not output_dir.exists():
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
                except HTTPError as e:
                    logger.warning(f'Error fetching {name} ({data_format}): {e}')
                    shutil.rmtree(output_dir, ignore_errors=True)
            else:
                logger.debug(f'Skipped {name} {data_format}')

            if overwrite_files or not output_dir.exists():
                station_path = Path(output_dir, f'{data_format[0]}ly{station_id}.csv')
                df_path = Path(str(station_path).replace('.csv', '_DATA_.csv'))
                if df_path.exists() and not overwrite_files:
                    df = pd.read_csv(df_path, index_col=0)
                else:
                    df = parse_csv_data(station_path, station_id, data_format, df_path)

                dataframes.append(df)

                if df_all is None:
                    df_all = df.sort_index()
                else:
                    df_all = pd.concat([df_all, df], axis=1).sort_index()

        # Concatenate dataframes
        logger.debug(f'Finished downloading {data_format} data. Concatenating dataframes...')
        df = pd.concat(dataframes, axis=1).sort_index()
        logger.debug(f'Data shape: {df.shape}')

        # Save data to CSV
        output_path = Path(data_dir, f'{data_format}_all_stations.csv')
        df.to_csv(output_path)
        logger.info(f'Data saved to {output_path}')


def parse_csv_data(station_path: Path, station_id: int, data_format: str, output_path: Path) -> pd.DataFrame:
    """Parse CSV data from a Met Eireann weather station CSV file

    :param Path | str station_path: Input file path
    :param str station_id: Station ID
    :param str data_format: Data format ('hourly', 'daily', 'monthly')
    :param Path | str output_path: Output file path
    :raises ValueError: If headers not found in file
    :return pd.DataFrame: Formatted dataframe
    """
    # Read CSV file
    with open(station_path) as f:
        lines = f.readlines()

    # Find line where data headers start
    DATE_HEADER = 'date,ind,'
    MONTH_HEADER = 'year,month,'
    headers_line = find_headers_line(lines, station_path, DATE_HEADER, MONTH_HEADER)

    # Write data to new file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.writelines(lines[headers_line:])

    # Read data to dataframe
    df = pd.read_csv(output_path)

    # Create time index
    if lines[headers_line].startswith(MONTH_HEADER):
        df['day'] = 1
        df['hour'] = 0
        df['minute'] = 0
        time_cols = ['year', 'month', 'day', 'hour', 'minute']
        df.index = pd.to_datetime(df[time_cols])
        df.drop(time_cols, axis=1, inplace=True)
    else:
        df = parse_date_col(df, data_format)

    # Sort by index, drop duplicates
    df = df.sort_index()
    df = df[~df.index.duplicated(keep='first')]

    # Rename columns and index, and save to CSV
    df.columns = [f'{station_id}__{col}' for col in df.columns]
    df.index.name = 'time'
    df.to_csv(output_path, index_label='time')
    df = pd.read_csv(output_path, index_col='time')
    return df


def find_headers_line(lines, station_path, *headers):
    """Find line in file where headers are located

    :param list[str] lines: List of lines in file
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


def parse_date_col(df, data_format):
    """Attempt to parse date column in dataframe

    :param pd.DataFrame df: Input dataframe
    :param str data_format: Data format ('hourly', 'daily', 'monthly')
    :return pd.DataFrame: Formatted dataframe
    """
    df = df.set_index('date', drop=True)

    if data_format in ['daily', 'monthly']:
        format = '%d-%b-%Y'
    elif data_format == 'hourly':
        format = '%d-%b-%Y %H:%M'

    df.index = pd.date_range(start=df.index[0], periods=len(df), freq=data_format[0].upper())
    df.index = pd.to_datetime(df.index, format=format)
    return df


if __name__ == '__main__':
    fetch_data(DATA_DIR, STATION_DATA_URL, overwrite_files=False)
