from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import DAG  # noqa: F401 - required for Airflow's DAG discovery heuristic
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode, RenderConfig
from cosmos.constants import LoadMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_PATH = Path(__file__).parent.parent / "dbt"
DBT_MANIFEST_PATH = DBT_PROJECT_PATH / "target" / "manifest.json"

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
        manifest_path=DBT_MANIFEST_PATH,
    ),
    render_config=RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.WATCHER,
    ),
    schedule=None,
    start_date=datetime(2022, 12, 30),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
)
