from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.operators.python import PythonOperator
import os

base_dir = os.path.dirname(os.path.realpath(__file__))
sql_dir = os.path.join(base_dir, 'sql')

def parse_csv_to_list(filepath):
    import csv

    with open(filepath, newline="") as file:
        return [row for row in csv.reader(file)]


def test_dag(file):
    print(file)


with DAG(
    "s3_to_postgres_ingest",
    start_date=datetime(2022, 12, 30),
    max_active_runs=1,
    schedule=None,
    template_searchpath=[sql_dir, 'include/sql']
) as dag:
    # TODO test
    # https://stackoverflow.com/questions/2829158/truncating-all-tables-in-a-postgres-database
    truncate_staging = PostgresOperator(
        task_id="truncate_staging",
        postgres_conn_id="cdw-dev",
        sql=f"truncate_schema.sql",
        params={"schema": "staging"},
    )

# # TODO get list of files in s3
#     list_s3_objects = S3ListOperator(
#         bucket = "s3_datalake",
#         task_id= "S3_List",
#         aws_conn_id="s3_datalake"
#     )

# #  TODO requires testing, likely broken
#     with list_s3_objects as govt_files:
#         for file in govt_files:
#             test_file = PythonOperator(
#                 task_id="test_operator" + file,
#                 python_callable= test_dag(),
#                 op_kwargs=file
#             )
#
#             # transfer_s3_to_sql = S3ToSqlOperator(
#             #     task_id="transfer_s3_to_sql" + file,
#             #     s3_bucket="s3_datalake",
#             #     s3_key=file,
#             #     table=file,
#             #     parser=parse_csv_to_list,
#             #     sql_conn_id='cdw-dev',
#             # )
