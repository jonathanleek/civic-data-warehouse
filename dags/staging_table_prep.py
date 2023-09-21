doc_md_DAG = """
### govt_file_download

This dag truncates any existing tables in the staging schema, the creates a table for any file found in the s3 bucket.
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import os
from include.staging_table_prep import create_staging_tables

base_dir = os.path.dirname(os.path.realpath(__file__))
sql_dir = os.path.join(base_dir, "sql")
BUCKET = "civic-data-warehouse-lz"


def prepare_s3_list(unprepared_list):
    file_dict = []
    for i in unprepared_list:
        file_dict.append({"key": i})
    return file_dict


with DAG(
    "staging_table_prep",
    start_date=datetime(2022, 12, 30),
    max_active_runs=1,
    schedule=None,
    doc_md=doc_md_DAG,
    template_searchpath=[sql_dir, "include/sql"],
) as dag:
    truncate_staging = PostgresOperator(
        task_id="truncate_staging",
        postgres_conn_id="cdw-dev",
        sql=f"truncate_schema.sql",
        params={"schema": "staging"},
    )

    # get list of files in s3
    list_s3_objects = S3ListOperator(
        bucket="civic-data-warehouse-lz", task_id="S3_List", aws_conn_id="s3_datalake"
    )

    prepare_list = PythonOperator(
        task_id="prepare_list",
        python_callable=prepare_s3_list,
        op_args=[list_s3_objects.output],
    )

    with TaskGroup(
        "table_creation_task_group",
        prefix_group_id=False,
    ):
        create_staging_tables = PythonOperator.partial(
            task_id="create_staging_tables",
            python_callable=create_staging_tables,
            op_args=["civic-data-warehouse-lz", "s3_datalake", "cdw-dev"],
        ).expand(op_kwargs=prepare_list.output)

truncate_staging >> list_s3_objects >> prepare_list >> create_staging_tables
