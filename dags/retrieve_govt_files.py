import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from include.retrieve_gov_file import retrieve_gov_file

gov_files = "include/gov_files.json"
BUCKET = "civic-data-warehouse-lz"
# Tasks currently fail if run on all gov_docs at once, but confirmed to work in smaller batches. Worker resource constraints?
with DAG(
    "govt_file_download",
    description="Downloads public civic data and saves to s3 for processing",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 24),
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
) as dag:
    with open(gov_files, "r") as read_file:
        gov_file_data = json.load(read_file)
        for file in gov_file_data["gov_files"]:
            upload_file = PythonOperator(
                task_id="files_to_s3_" + file["file_name"],
                python_callable=retrieve_gov_file,
                op_kwargs={
                    "filename": file["file_name"],
                    "file_url": file["file_location"],
                    "bucket": BUCKET,
                    "s3_conn_id": "s3_datalake",
                },
            )
