from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator, S3ToSqlOperator

with DAG(
    "s3_to_postgres_ingest",
    start_date=datetime(2022, 12, 30),
    max_active_runs=1,
    schedule=None
) as dag:

# TODO test
# https://stackoverflow.com/questions/2829158/truncating-all-tables-in-a-postgres-database
    truncate_staging = PostgresOperator(
        task_id= 'truncate_staging',
        postgres_conn_id= 'cdw-dev',
        sql= "include/sql/truncate_schema.sql",
        params={'schema': 'staging'}
    )

# TODO get list of files in s3
    list_s3_objects = S3ListOperator(
        bucket = s3_datalake
    )

#  TODO requires testing, likely broken
    with list_s3_objects as govt_files:
        for file in gov_files:
            file_to_postgres = S3ToSqlOperator(
                task_id="files_to_PostGres_" + file,
                s3_key = file,
                s3_bucket = s3_datalake,
                table = "staging.{{file}}",
                parser = def parse_csv(filepath):import csv with open(filepath, newline=””) as file:yield from csv.reader(file),
                sql_conn_id = "cdw-dev"
            )
