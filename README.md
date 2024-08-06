# Irish Weather

Irish weather data from Met Eireann (Creative Commons Attribution 4.0 International (CC BY 4.0) License)

## Downloading Data

Installation:

```bash
conda create -n env_weather -y python=3.10
conda activate env_weather
pip install -r requirements.txt
python -m src monthly            # To fetch monthly data
python -m src daily              # To fetch daily data (slow)
python -m src hourly             # To fetch hourly data (very slow)
python src/fetch_weather_data.py # To fetch all data (slowest)
```

## Development

```bash
# Follow installation commands above, then:

# For pre-commit
conda activate env_weather
conda install pre-commit
pre-commit run --all-files

# For tests
pytest

# For linting
pylint
```
