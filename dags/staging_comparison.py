from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

with DAG(
    "staging_comparison",
    start_date=datetime(2022, 12, 30),
    max_active_runs=1,
    schedule=None
) as dag:

