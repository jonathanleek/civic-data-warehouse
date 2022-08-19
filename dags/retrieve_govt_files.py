from airflow.models import Variable
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

from include.retrieve_gov_file import retrieve_gov_file

# TODO change s3 connection to use environment variables
AWS_S3_CONN_ID = Variable.get("AIRFLOW_CONN_S3_DATALAKE")
gov_files = "gov_files.json"
BUCKET = "civic-data-warehouse-lz"
# TODO Test this dag
with DAG(
    "govt_file_download",
    description="Downloads public civic data and saves to s3 for processing",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 24),
    catchup=False,
) as dag:
    with open(gov_files, "r") as read_file:
        gov_file_data = json.load(read_file)
    for file in gov_file_data.gov_file:
        for file in gov_file_data:
            upload_file = PythonOperator(
                task_id="files_to_s3",
                python_callable=retrieve_gov_file,
                op_kwargs={
                    "file_name": file.file_name,
                    "file_url": file.file_url,
                    "bucket": BUCKET,
                    "s3_conn_id": AWS_S3_CONN_ID,
                },
            )
