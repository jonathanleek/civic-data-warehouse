from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import os
from airflow.sdk import DAG

base_dir = os.path.dirname(os.path.realpath(__file__))
sql_dir = os.path.join(base_dir, "sql")

with DAG(
    "cdw_creation",
    start_date=datetime(2022, 12, 30),
    max_active_runs=1,
    schedule=None,
    template_searchpath=[sql_dir, "include/sql"],
) as dag:
    # Create schema "staging" in cdw database
    create_staging = SQLExecuteQueryOperator(
        task_id="create_staging",
        conn_id="cdw-dev",
        sql="create_staging.sql",
    )

    # Create schema "current" in cdw database
    create_current = SQLExecuteQueryOperator(
        task_id="create_data_prep",
        conn_id="cdw-dev",
        sql=f"create_current.sql",
    )

    # Create schema "history" in cdw database
    create_history = SQLExecuteQueryOperator(
        task_id="create_history",
        conn_id="cdw-dev",
        sql=f"create_history.sql",
    )

    create_truncate_tables_function = SQLExecuteQueryOperator(
        task_id="create_truncate_tables_function",
        conn_id="cdw-dev",
        sql=f"create_truncate_tables_function.sql",
    )
create_staging >> create_current >> create_history >> create_truncate_tables_function
