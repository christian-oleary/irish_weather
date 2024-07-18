"""Fetch public weather data for Ireland"""

from pathlib import Path

import pandas as pd

DATA_DIR = 'data'
STATION_DATA_URL = 'https://cli.fusio.net/cli/climate_data/stations.csv'


def fetch_data(
    data_dir: str | Path,
    station_url: str,
):
    """Fetch Met Eireann weather data and save to CSV files

    :param str | Path data_dir: _description_, defaults to 'data'
    :param _type_ station_url: _description_, defaults to 'https://cli.fusio.net/cli/climate_data/stations.csv'
    """

    # Read station data to retrieve IDs
    df_stations = pd.read_csv(station_url)
    df_stations.to_csv(Path(data_dir, 'stations.csv'), index=False)

    df_stations = pd.read_csv(Path(data_dir, 'stations.csv'))

    print(df_stations)
    print(df_stations.columns)
    print(df_stations.shape)

    station_ids = dict(
        zip(
            df_stations['stno'],
            df_stations['county'] + '__' + df_stations['Name']
        )
    )

    for station_id, name in station_ids.items():
        print(station_id, name)

    # TODO:
    # declare -a levels=("h" "d" "m")
    # for i in {1000..9999}; do
    #     for l in "${levels[@]}"; do
    #         if [ -e $l'ly'$i'.zip' ]; then
    #             echo $l'ly'$i'.zip already exists'
    #         else
    #             echo 'Attemping download of '$l'ly'$i'.zip'
    #             curl -s -f 'https://cli.fusio.net/cli/climate_data/webdata/'$l'ly'$i'.zip' --output $l'ly'$i'.zip'
    #             sleep 5  # Just in case of rate limiting/automatic blacklisting
    #         fi
    #     done
    # done


if __name__ == '__main__':
    fetch_data(DATA_DIR, STATION_DATA_URL)
