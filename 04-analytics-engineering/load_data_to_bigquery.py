import dlt
import requests
import pandas as pd

from io import BytesIO

# Create a dlt pipeline that will load
# chess player data to the BigQuery destination
# via a GCS bucket.
# pipeline = dlt.pipeline(
#     pipeline_name='stg_to_bq',
#     destination='bigquery',
#     staging='filesystem', # Add this to activate the staging location.
#     dataset_name='mod04_bq_dataset'
# )

# Define a dlt resource to download and process Parquet files as single table
@dlt.resource(name="rides", write_disposition="replace")
def download_parquet():
    for month in range(1,7):
        url = f"https://storage.cloud.google.com/mod04-bucket/fhv/fhv_tripdata_2019-{month}.parquet"
        response = requests.get(url)
        # print(response.content)

        # df = pd.read_parquet(BytesIO(response.content))
        df = pd.read_parquet(response.content)

        # Return the dataframe as a dlt resource for ingestion
        yield df

# Initialize the pipeline
pipeline = dlt.pipeline(
    pipeline_name="rides_pipeline",
    destination="duckdb",  # Use DuckDB for testing
    # destination="bigquery",  # Use BigQuery for production
    dataset_name="mod04_bq_dataset"
)

# Run the pipeline to load Parquet data into DuckDB
info = pipeline.run(download_parquet)

# Print the results
# print(info)

     
