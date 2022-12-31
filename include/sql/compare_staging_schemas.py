from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
        "compare_staging_schemas",
        start_date=datetime(2022, 12, 30),
        max_active_runs=1,
        schedule=None
) as dag:

# truncate dead_records and updated_records tables
# populate dead_records table with records in staging_2 but not staging_1
# populate updated_records table with records in staging_1 but not staging 2 and records that exist in both staging schema, but with differences
