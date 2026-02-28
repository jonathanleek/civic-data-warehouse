doc_md_DAG = """
### dbt_run_after_staging

This DAG waits for the `staging_table_prep` DAG to succeed, then runs dbt to
build production tables in the `current` schema from `staging`.
"""

from datetime import datetime
import os

from airflow.sdk import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState
from airflow.providers.standard.operators.bash import BashOperator


BASE_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
DBT_DIR = os.path.join(PROJECT_ROOT, "dbt")


with DAG(
    "dbt_run_after_staging",
    start_date=datetime(2022, 12, 30),
    schedule=None,
    catchup=False,
    doc_md=doc_md_DAG,
) as dag:
    wait_for_staging = ExternalTaskSensor(
        task_id="wait_for_staging_table_prep",
        external_dag_id="staging_table_prep",
        external_task_id=None,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="cd {{ params.project_root }} && dbt run --project-dir dbt",
        params={"project_root": PROJECT_ROOT},
        env={"DBT_PROFILES_DIR": DBT_DIR},
    )

    wait_for_staging >> run_dbt
