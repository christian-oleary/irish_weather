# Irish Weather

Irish weather data from Met Eireann (Creative Commons Attribution 4.0 International (CC BY 4.0) License)

## Downloading Data

Installation:

```bash
conda create -n env_weather -y python=3.10
conda activate env_weather
pip install -r requirements.txt
python -m src monthly
python -m src daily
python -m src hourly
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
