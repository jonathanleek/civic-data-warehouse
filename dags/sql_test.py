from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG

with DAG(
    "sql_test", start_date=datetime(2022, 12, 30), max_active_runs=1, schedule=None
) as dag:
    truncate_staging = SQLExecuteQueryOperator(
        task_id="sql_test",
        conn_id="cdw-dev",
        sql="SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('staging', 'current', 'history')",
    )
