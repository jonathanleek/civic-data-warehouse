from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import csv
import os

base_dir = os.path.dirname(os.path.realpath(__file__))
sql_dir = os.path.join(base_dir, 'sql')

def parse_csv_to_list(filepath):

    with open(filepath, newline="") as file:
        return [row for row in csv.reader(file)]

def create_key_table_pairs(key):
    key_table_pair = {
        "key": f"{key}",
        "table": f"{key}"
    }
    return key_table_pair

with DAG(
    "s3_to_postgres_ingest",
    start_date=datetime(2022, 12, 30),
    max_active_runs=1,
    schedule=None,
    template_searchpath=[sql_dir, 'include/sql']
) as dag:
    # https://stackoverflow.com/questions/2829158/truncating-all-tables-in-a-postgres-database
    truncate_staging = PostgresOperator(
        task_id="truncate_staging",
        postgres_conn_id="cdw-dev",
        sql=f"truncate_schema.sql",
        params={"schema": "staging"},
    )

# get list of files in s3
    list_s3_objects = S3ListOperator(
        bucket = "civic-data-warehouse-lz",
        task_id= "S3_List",
        aws_conn_id="s3_datalake"
    )

# TODO Iterate through files and ingest into Postgres
# https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
    with TaskGroup('file_ingest_task_group',
                   prefix_group_id=False,
                   ):
            transfer_s3_to_sql = S3ToSqlOperator.partial(
                task_id="transfer_s3_to_sql",
                s3_bucket='civic-data-warehouse-lz',
                parser=parse_csv_to_list,
                sql_conn_id='cdw-dev',
                schema= 'staging',
            ).expand(s3_key=list_s3_objects.output, table = list_s3_objects.output)

truncate_staging >> list_s3_objects >> transfer_s3_to_sql