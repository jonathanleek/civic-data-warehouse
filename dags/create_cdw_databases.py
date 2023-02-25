from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    "cdw_creation",
    start_date=datetime(2022, 12, 30),
    max_active_runs=1,
    schedule_interval=None
) as dag:

    # Create schema "staging_1" in cdw database
    create_staging_1 = PostgresOperator(
        task_id = 'create_staging_1',
        postgres_conn_id = "cdw-dev",
        sql = "include/sql/create_staging_1.sql"
    )
    # Create schema "staging_2" in cdw database
    create_staging_2 = PostgresOperator(
        task_id = 'create_staging_2',
        postgres_conn_id = "cdw-dev",
        sql = "include/sql/create_staging_2.sql"
    )

    # Create schema "data_prep" in cdw database
    create_dead_records = PostgresOperator(
        task_id = 'create_dead_records',
        postgres_conn_id = "cdw-dev",
        sql = "include/sql/create_dead_records.sql"
    )

    create_updated_records = PostgresOperator(
        task_id = 'create_updated_records',
        postgres_conn_id = "cdw-dev",
        sql = "include/sql/create_updated_records.sql"
    )

create_staging_1 >> create_staging_2 >> create_data_prep