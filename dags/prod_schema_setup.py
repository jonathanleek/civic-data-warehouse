doc_md_DAG = """
### prod_schema_setup

Creates the production schema and tables in the `current` schema.
"""

from datetime import datetime
import os

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG


base_dir = os.path.dirname(os.path.realpath(__file__))
sql_dir = os.path.join(base_dir, "sql")


with DAG(
    "prod_schema_setup",
    start_date=datetime(2022, 12, 30),
    max_active_runs=1,
    schedule=None,
    catchup=False,
    doc_md=doc_md_DAG,
    template_searchpath=[sql_dir, "include/sql"],
) as dag:
    create_current = SQLExecuteQueryOperator(
        task_id="create_current",
        conn_id="cdw-dev",
        sql="create_current.sql",
    )

    create_history = SQLExecuteQueryOperator(
        task_id="create_history",
        conn_id="cdw-dev",
        sql="create_history.sql",
    )

    # Run in parallel
