import polars as pl
import requests
from datetime import date
from google.cloud import storage

# Load secrets from CSV file
secrets = pl.read_csv("secrets.csv")
affiliate_id = secrets.filter(pl.col("secretName") == "affiliateId").get_column("secretValue")[0]

# Define function to query API for show availability information
def query_api_for_show_availability(show_id, date_start, date_end, affiliate_id=affiliate_id):
    url = f"https://inventory-service.tixuk.io/api/v4/availability/products/{show_id}/quantity/1/from/{date_start}/to/{date_end}/detailed"
    headers = {"affiliateId": affiliate_id}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print(f"Successfully queried API for show ID {show_id}")
        return response
    else:
        print(f"Failed to query API for show ID {show_id}")
        return None

# Define function to parse API response into Polars DataFrame
def parse_api_response(api_response):
    if api_response is not None:
        api_response_json = api_response.json()
        response_show_availability = api_response_json["response"]["availability"]
        df_parsed = pl.DataFrame({
            "performanceTime": [x["datetime"] for x in response_show_availability],
            "performanceType": [x["performanceType"] for x in response_show_availability],
            "availableSeatCount": [x["availableSeatCount"] for x in response_show_availability],
            "largestLumpOfTickets": [x["largestLumpOfTickets"] for x in response_show_availability],
            "minPrice": [x["minPrice"] for x in response_show_availability],
            "maxPrice": [x["maxPrice"] for x in response_show_availability],
            "currency": [x["currency"] for x in response_show_availability],
            "discountAvailable": [x["discount"] for x in response_show_availability]
        })

        # Also extract the requested show ID as recorded in the response
        response_show_id = api_response_json["response"]["show"]
        # Also extract the request timestamp (as recorded in the response)
        if "X-Client-Request-DateTime" in api_response.headers:
            response_request_timestamp = api_response.headers["X-Client-Request-DateTime"]
        else:
            response_request_timestamp = None
        # And add these as constant columns
        df_parsed = df_parsed.with_columns(
            pl.lit(response_request_timestamp).alias("requestTime"),
            pl.lit(response_show_id).alias("showId")
        )

        # Enforce correct data types
        # TODO: handle casting errors
        df = df_parsed.select(
            pl.col("performanceTime").str.to_datetime("%+"),
            pl.col("performanceType").cast(pl.String),
            pl.col("availableSeatCount").cast(pl.Int64),
            pl.col("largestLumpOfTickets").cast(pl.Int64),
            pl.col("minPrice").cast(pl.Int64),
            pl.col("maxPrice").cast(pl.Int64),
            pl.col("currency").cast(pl.String),
            pl.col("discountAvailable").cast(pl.Boolean),
            pl.col("requestTime").str.to_datetime("%+"),
            pl.col("showId").cast(pl.String)
        )
        
        print(f"Successfully parsed API response for show ID {response_show_id}")
        return df
    else:
        return None

def save_to_google_storage(dat, bucket):
    # First save locally in parquet format
    # TODO: Using a the gcsfs Python library, I should be able to save directly to Storage in parquet format
    file_name = f"show-availability-query-date-{str(date.today())}.parquet"
    path_to_file = f"data/{file_name}"
    dat.write_parquet(path_to_file)

    try:
        # Then upload to Google Cloud Storage from local
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
    # We shall query the API for the followinf list of shows
    show_list = pl.DataFrame({
        "show_name": ["A Christmas Carol(ish)", "Oedipus (by Robert Icke)"],
        "show_id": [42462, 41707],
        "date_start": ["20241101", "20241101"],
        "date_end": ["20241231", "20241231"]
    })

    # Get API responses for the list of shows
    api_responses = [query_api_for_show_availability(row["show_id"], row["date_start"], row["date_end"]) for row in show_list.iter_rows(named=True)]

    # Parse API responses into Polars DataFrames
    parsed_responses = [parse_api_response(api_response) for api_response in api_responses if api_response is not None]

    # Concatenate into a single DataFrame
    dat_parsed = pl.concat(parsed_responses, how="vertical")

    # Save the dataframe in parquet format locally, and also upload to google cloud storage
    save_to_google_storage(dat_parsed, "raw-todaytix-api-show-availability")