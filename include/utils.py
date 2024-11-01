# include/utils.py

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime


def log_gov_file_info(file_name, file_location):
    print(f"Processing file: {file_name}")
    print(f"File location: {file_location}")


def download_and_upload_to_s3(file_name, file_location):
    # Download the file from the given location
    response = requests.get(file_location)
    response.raise_for_status()  # Ensure the request was successful

    # Format the S3 key with a date-stamped directory
    date_str = datetime.now().strftime("%Y-%m-%d")
    s3_key = f"raw_downloads/{date_str}/{file_name}"

    # Use the S3Hook with the same connection ID as in the test DAG
    s3_hook = S3Hook(aws_conn_id="s3mock")  # Use "s3mock" as in your test DAG

    # Upload the file content to the specified bucket and key
    s3_hook.load_bytes(
        bytes_data=response.content,
        key=s3_key,
        bucket_name="my-bucket",  # Use the same bucket name as in your test DAG
        replace=True
    )
    print(f"Uploaded {file_name} to s3://my-bucket/{s3_key}")