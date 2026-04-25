from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import DAG  # noqa: F401 - required for Airflow's DAG discovery heuristic
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode, InvocationMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_PATH = Path(__file__).parent.parent / "dbt"

profile_config = ProfileConfig(
    profile_name="cdw",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="cdw-dev",
        profile_args={"schema": "public"},
    ),
)

dbt_transform = DbtDag(
    dag_id="dbt_transform",
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        invocation_mode=InvocationMode.SUBPROCESS,
        dbt_executable_path="/usr/local/bin/dbt",
    ),
    schedule=None,
    start_date=datetime(2022, 12, 30),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
)
