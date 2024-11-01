import os
import dagfactory
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator

# Set the project root to 'cdw'
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))

# Path to the YAML configuration file
CONFIG_FILE = os.path.join(PROJECT_ROOT, "dags", "config", "cdw_setup_dag.yaml")

# Raise an error if the config file is not found
if not os.path.isfile(CONFIG_FILE):
    raise FileNotFoundError(f"Config file not found at {CONFIG_FILE}")

# Initialize the DagFactory with the YAML path
dag_factory = dagfactory.DagFactory(CONFIG_FILE)

# Generate DAGs and register them in globals()
try:
    dag_factory.generate_dags(globals())
except Exception as e:
    raise RuntimeError(f"Error generating DAGs: {e}")

# Retrieve the generated DAG
cdw_setup_dag = globals().get("cdw_setup_dag")
if not cdw_setup_dag:
    raise ValueError("DAG 'cdw_setup_dag' was not found.")

# Set the template search path to include the SQL directory
cdw_setup_dag.template_searchpath = [os.path.join(PROJECT_ROOT, "include", "sql")]

# Ensure the start task exists or create a default EmptyOperator
start_task = cdw_setup_dag.task_dict.get("start_task") or EmptyOperator(
    task_id="start_task", dag=cdw_setup_dag
)

# Path to SQL scripts inside the project
SQL_SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "include", "sql", "cdw_setup")

# Raise an error if the SQL directory is missing
if not os.path.isdir(SQL_SCRIPTS_DIR):
    raise FileNotFoundError(f"SQL scripts directory not found at {SQL_SCRIPTS_DIR}")

# Loop through SQL files and create tasks
for sql_file in sorted(os.listdir(SQL_SCRIPTS_DIR)):
    if sql_file.endswith(".sql"):
        task_id = f"execute_{os.path.splitext(sql_file)[0]}"
        sql_path = f"cdw_setup/{sql_file}"  # Relative path

        # Create a PostgresOperator task for each SQL script
        sql_task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="civic_data_warehouse",  # Ensure this connection exists in Airflow
            sql=sql_path,
            dag=cdw_setup_dag,
        )

        # Set dependencies: start_task -> sql_task
        start_task >> sql_task