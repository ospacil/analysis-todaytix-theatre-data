from datetime import datetime, date
import gcsfs
import functions_framework
import polars as pl
from utils import *

@functions_framework.http
def get_show_availability(request):
    # Create a test DataFrame
    show_list = pl.DataFrame({
        "show_name": ["A Christmas Carol(ish)", "Oedipus (by Robert Icke)"],
        "show_id": [42462, 41707],
        "date_start": ["20241101", "20241101"],
        "date_end": ["20241231", "20241231"]
    })

    # Get API responses for the list of shows
    api_responses = [query_api_for_show_availability(row["show_id"], row["date_start"], row["date_end"], "TodayTix") for row in show_list.iter_rows(named=True)]

    # Parse API responses into Polars DataFrames
    parsed_responses = [parse_api_response(api_response) for api_response in api_responses if api_response is not None]

    # If not empty, save the parsed results into a single dataframe
    if parsed_responses == []:
        return "No API response parsed. Nothing to save."
    else:
        # Concatenate into a single DataFrame
        dat_parsed = pl.concat(parsed_responses, how="vertical")

        # Configure connection to Cloud Storage
        fs = gcsfs.GCSFileSystem(project="todaytix-theatre-data")
        file_name = f"gs://raw-todaytix-api-show-availability/show-availability-query-date-{str(date.today())}.parquet"
        
        # Write into Cloud Storage
        with fs.open(file_name, "wb") as f:
            dat_parsed.write_parquet(f)

        return 'Success!'
