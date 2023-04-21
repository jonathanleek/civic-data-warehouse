from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    "cdw_creation",
    start_date=datetime(2022, 12, 30),
    max_active_runs=1,
    schedule_interval=None
) as dag:

    # Create schema "staging" in cdw database
    create_staging = PostgresOperator(
        task_id = 'create_staging',
        postgres_conn_id = "cdw-dev",
        sql = "include/sql/create_staging.sql"
    )

    # Create schema "data_prep" in cdw database
    create_data_prep = PostgresOperator(
        task_id = 'create_data_prep',
        postgres_conn_id = "cdw-dev",
        sql = "include/sql/create_data_prep.sql"
    )

    # Create schema "history" in cdw database
    create_history = PostgresOperator(
        task_id="create_history",
        postgres_conn_id = "cdw-dev",
        sql= "include/sql/create_history.sql"
    )

create_staging >> create_data_prep >> create_history