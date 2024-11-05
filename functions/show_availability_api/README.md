## Google Cloud Run Function - TodayTix API show availability

This folder contains the Python scripts to deploy and run a Google Cloud Run function to query the TodayTix API for show availability, parse the API response into a Polars DataFrame, and save the DataFrame into Google Cloud Storage.

The top-level Python script has to be called `main.py`, and the entry point function has to be decorared with `@functions_framework.http`.

Note that the functions in `utils.py` are also used by the "local" Python script to query the API -- see the `scripts\` folder.