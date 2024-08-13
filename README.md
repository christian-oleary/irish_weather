# Irish Weather

Irish weather data from Met Eireann (Creative Commons Attribution 4.0 International (CC BY 4.0) License)

## Data Download Page

### Using Command Line Interface

Run the following command in a terminal (on Windows, use WSL or Git Bash) to get a link to the most recent data:

```bash
echo -e "\nFetching link to data...\n" && curl -s -f -L https://github.com/christian-oleary/irish_weather/actions/workflows/fetch_data.yml?query=is%3Asuccess | grep -Eo "/\S+?\"" | grep -m 1 actions/runs | sed 's/"//g' | xargs -I {} echo https://github.com'{}'
```

### Using a Browser

Alternatively, you can use the web interface on GitHub:

- Navigate to the Actions page of the repository: [https://github.com/christian-oleary/irish_weather/actions/workflows/fetch_data.yml?query=is%3Asuccess](https://github.com/christian-oleary/irish_weather/actions/workflows/fetch_data.yml?query=is%3Asuccess)
- Select first (most recent) action in the resulting table.
- The following page contains a link to the zip file.

## Installation

```bash
conda create -n env_weather -y python=3.10
conda activate env_weather
pip install -r requirements.txt
```

## Downloading Data from CLI

```bash
python -m src monthly # To fetch monthly data
python -m src daily   # To fetch daily data (slow)
python -m src hourly  # To fetch hourly data (very slow)
```

## Configuration in Python

See code in [src/fetch_weather_data.py](./src/fetch_weather_data.py):

```python
from src.fetch_weather_data import WeatherDataCollector, STATION_DATA_URL

collector = WeatherDataCollector(
    data_dir='data',              # Output directory
    data_formats=['monthly'],     # monthly, daily and/or hourly
    max_rows=50000,               # Threshold to start dropping old data (-1 = no limit)
    min_date='1990-01-01',        # Earliest allowed data if max_rows is reached
    station_url=STATION_DATA_URL,
    sleep_delay=5,                # Delay between requests
    overwrite_files=False,        # Set to True if you need to update data
    enable_logging=True,
)

# Download data and combine into CSV files
collector.fetch_data()
```

## Development

Setup:

```bash
conda activate env_weather
pip install -r ./tests/requirements.txt
conda install pre-commit
pre-commit run --all-files
pytest
pylint src
```
