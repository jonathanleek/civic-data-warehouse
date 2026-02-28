doc_md_DAG = """
### govt_file_download

This dag downloads files listed in include/gov_files.json to the worker, where they are recursively unzipped, converted to csvs, and pushed to the s3 bucket.
Python script being run can be found at include/retrieve_gov_file.py
Currently converts the following file formats. All others are loaded to s3 after unzipping.
- .mdb


"""


import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from include.retrieve_gov_file import retrieve_gov_file, clear_files_and_subdirs

gov_files = "include/gov_files.json"
BUCKET = "civic-data-warehouse-lz"
prep_directory = "/tmp/prep/" 

# Tasks currently fail if run on all gov_docs at once, but confirmed to work in smaller batches. Worker resource constraints?
with DAG(
    "govt_file_download",
    description="Downloads public civic data and saves to s3 for processing",
    schedule_interval=None,
    start_date=datetime(2022, 6, 24),
    catchup=False,
    doc_md=doc_md_DAG,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
) as dag:
    
    @task_group()
    def upload_all_files():
        with open(gov_files, "r") as read_file:
            gov_file_data = json.load(read_file)
            for file in gov_file_data["gov_files"]:
                task_id="files_to_s3_" + file["file_name"]
                upload_file = PythonOperator(
                    task_id=task_id,
                    python_callable=retrieve_gov_file,
                    op_kwargs={
                        "filename": file["file_name"],
                        "file_url": file["file_location"],
                        "bucket": BUCKET,
                        "s3_conn_id": "s3_datalake",
                        "task_id": task_id,
                        "base_prep_dir": prep_directory
                    },
                )
    
    cleanup_task = PythonOperator(
        task_id='clear_tmp_directory',
        python_callable=clear_files_and_subdirs,
        op_kwargs={
            "dir_to_clear": prep_directory
        },
    )

    upload_all_files() >> cleanup_task