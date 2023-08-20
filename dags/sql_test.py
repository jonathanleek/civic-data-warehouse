from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    "sql_test", start_date=datetime(2022, 12, 30), max_active_runs=1, schedule=None
) as dag:
    truncate_staging = PostgresOperator(
        task_id="sql_test",
        postgres_conn_id="cdw-dev",
        sql="SELECT * FROM CDW.Staging.test_table",
        params={"schema": "staging"},
    )
