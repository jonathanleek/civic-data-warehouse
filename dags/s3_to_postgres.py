from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator


with DAG(
    "s3_to_postgres_ingest",
    start_date=datetime(2022, 12, 30),
    max_active_runs=1,
    schedule=None
) as dag:


# truncate tables in staging_2
# copy staging_1 tables to staging_2
# truncate staging_1 tables
# get list of files in s3
# for each file in s3, create table if not exist and import csv
