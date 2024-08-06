import sys

from src.fetch_weather_data import fetch_data, data_formats

# Get the command line arguments
try:
    formats = sys.argv[1:]
except IndexError:
    raise ValueError('Please provide at least one data format: "hourly", "daily", or "monthly"')

for data_format in formats:
    if data_format not in data_formats:
        raise ValueError(f'Invalid data format: {data_format}. Options are: {data_formats}')

# Call the fetch_data function and pass the data_formats argument
fetch_data(data_formats=formats)
