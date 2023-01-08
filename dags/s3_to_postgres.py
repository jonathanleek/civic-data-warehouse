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
# https://stackoverflow.com/questions/2829158/truncating-all-tables-in-a-postgres-database
# add function creation dag to create appropriate function
    truncate_stating_2= PostgresOperator(
        task_id = 'truncate_staging_2',
        postgres_conn_id = "cdw-dev",
        sql = "include/sql/truncate_staging_2.sql"
    )

# copy staging_1 tables to staging_2
# need to create or replace tables
    copy_staging_1_to_staging_2 = PostgresOperator(
        task_id= 'copy_staging_1_to_staging_2',
        postgres_conn_id= "cdw_dev",
        sql = "include/sql/staging_1_to_staging_2.sql"
    )

# get list of files in s3
# https://registry.astronomer.io/providers/amazon/modules/s3listoperator

# for each file in s3, create table if not exist and import csv
# use dynamic task generation https://newt-tan.medium.com/airflow-dynamic-generation-for-tasks-6959735b01b
# see retrieve_gov_files.py for example
    create_staging_1_tables = PostgresOperator(
        task_id= 'create_staging_1_tables',
        postgres_conn_id= "cdw-dev",
        sql= "include/sql/create_staging_1_tables.sql"
    )
