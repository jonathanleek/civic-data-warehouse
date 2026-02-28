doc_md_DAG = """
### govt_file_download

This dag truncates any existing tables in the staging schema, the creates a table for any file found in the s3 bucket.
"""

from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.sdk import TaskGroup
import os
from include.staging_table_prep import create_staging_table, populate_staging_table
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


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
    truncate_staging = SQLExecuteQueryOperator(
        task_id="truncate_staging",
        conn_id="cdw-dev",
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

    create_staging_tables = PythonOperator.partial(
        task_id="create_staging_tables",
        python_callable=create_staging_table,
        op_args=["civic-data-warehouse-lz", "s3_datalake", "cdw-dev"],
    ).expand(op_kwargs=prepare_list.output)

    # TODO 'forestry_maintenance_properties' is failing to populate.
    # column "category" of relation "forestry_maintenance_properties" does not exist
    # column names for forestry_maintenance_properties are in "" for some reason? Not seeing that in other tables

    populate_staging_tables = PythonOperator.partial(
        task_id="populate_staging_tables",
        python_callable=populate_staging_table,
        op_args=["civic-data-warehouse-lz", "s3_datalake", "cdw-dev"],
        trigger_rule="all_done",
    ).expand(op_kwargs=prepare_list.output)

(
    truncate_staging
    >> list_s3_objects
    >> prepare_list
    >> create_staging_tables
    >> populate_staging_tables
)
