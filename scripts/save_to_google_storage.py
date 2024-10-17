# Imports the Google Cloud client library
from google.cloud import storage

# Instantiates a client
storage_client = storage.Client()
bucket = storage_client.bucket('irbis-analytics-todaytix-theatre-data')

file_name = 'product-id-42462-availability-2024-10-17.parquet'
path_to_local_file = 'data/' + file_name

blob = bucket.blob(file_name)
blob.upload_from_filename(path_to_local_file)
