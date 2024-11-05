import polars as pl
from datetime import datetime, date
from google.cloud import storage
from functions.show_availability_api.utils import *

def save_to_google_storage(dat, bucket):
    # First save locally in parquet format
    """
    Save a Polars DataFrame to Google Cloud Storage, with the current date included in the file name.

    Inputs:
    - dat: polars.DataFrame - The dataframe to be saved
    - bucket: str - The Google Cloud Storage bucket to save to

    Returns: None
    """
    
    file_name = f"show-availability-query-date-{str(date.today())}.parquet"
    path_to_file = f"data/{file_name}"
    dat.write_parquet(path_to_file)

    # Then upload to Google Cloud Storage from local
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(path_to_file)
        print(f"Saved {file_name} to Google Cloud Storage")
    except Exception as e:
        print(f"Failed to save {file_name} to Google Cloud Storage")
        print(e)

# ==============================================================================

if __name__ == "__main__":
    # Load secrets from CSV file
    secrets = pl.read_csv("secrets.csv")
    affiliate_id = secrets.filter(pl.col("secretName") == "affiliateId").get_column("secretValue")[0]
    
    # We shall query the API for the following list of shows
    show_list = pl.DataFrame({
        "show_name": ["A Christmas Carol(ish)", "Oedipus (by Robert Icke)"],
        "show_id": [42462, 41707],
        "date_start": ["20241101", "20241101"],
        "date_end": ["20241231", "20241231"]
    })

    # Get API responses for the list of shows
    api_responses = [query_api_for_show_availability(row["show_id"], row["date_start"], row["date_end"], affiliate_id) for row in show_list.iter_rows(named=True)]

    # Parse API responses into Polars DataFrames
    parsed_responses = [parse_api_response(api_response) for api_response in api_responses if api_response is not None]

    # If not empty, save the parsed results into a single dataframe
    if parsed_responses == []:
        print("No API response parsed. Nothing to save.")
    else: 
        # Concatenate into a single DataFrame
        dat_parsed = pl.concat(parsed_responses, how="vertical")
        
        print("Head of the resulting DataFrame:")
        print(dat_parsed.glimpse())

        # Save the dataframe in parquet format locally, and also upload to google cloud storage
        save_to_google_storage(dat_parsed, "raw-todaytix-api-show-availability")
