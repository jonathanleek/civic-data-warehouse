import os
import sys
import json
import yaml
import dagfactory
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Set project root and add the include directory to the Python path
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
INCLUDE_DIR = os.path.join(PROJECT_ROOT, "include")
sys.path.append(INCLUDE_DIR)

# Paths to configuration files and directories
CONFIG_DIR = os.path.join(PROJECT_ROOT, "dags", "config")
CDW_DAG_CONFIG_FILE = os.path.join(CONFIG_DIR, "cdw_setup_dag.yaml")
GOV_FILE_DAG_CONFIG_FILE = os.path.join(CONFIG_DIR, "gov_file_prep.yaml")
GOV_FILES_JSON = os.path.join(INCLUDE_DIR, "gov_files.json")

# Ensure config directory exists
os.makedirs(CONFIG_DIR, exist_ok=True)

# Load gov_files.json to use in gov_file_prep.yaml generation
if not os.path.isfile(GOV_FILES_JSON):
    raise FileNotFoundError(f"gov_files.json not found at {GOV_FILES_JSON}")

with open(GOV_FILES_JSON, 'r') as json_file:
    gov_files = json.load(json_file).get("gov_files", [])

# ---- Part 1: Generate cdw_setup_dag.yaml configuration ----
cdw_dag_config = {
    "default": {
        "default_args": {
            "owner": "airflow",
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "start_date": datetime(2023, 1, 1)  # Set a default start_date
        },
        "schedule_interval": "@daily",
        "catchup": False,
        "dagrun_timeout_sec": 300,
        "default_view": "graph"
    }
}

# Save cdw_setup_dag.yaml configuration
with open(CDW_DAG_CONFIG_FILE, 'w') as yaml_file:
    yaml.dump(cdw_dag_config, yaml_file, default_flow_style=False)

# ---- Part 2: Generate gov_file_prep.yaml configuration from gov_files.json ----
gov_file_dag_config = {
    "default": {
        "default_args": {
            "owner": "airflow",
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "start_date": datetime(2023, 1, 1)  # Required start_date for gov_file_prep.yaml
        },
        "schedule_interval": "@daily",
        "catchup": False,
        "dagrun_timeout_sec": 300,
        "default_view": "graph"
    }
}

# Create a DAG configuration for each file in gov_files.json
for file in gov_files:
    dag_id = f"prep_{file['file_name'].split('.')[0]}"
    gov_file_dag_config[dag_id] = {
        "dag_id": dag_id,
        "default_args": {
            "file_name": file["file_name"],
            "file_location": file["file_location"]
        },
        "tasks": {
            "start": {
                "operator": "airflow.operators.empty.EmptyOperator",
                "task_id": "start"
            },
            "log_info": {
                "operator": "airflow.operators.python.PythonOperator",
                "task_id": "log_info",
                "python_callable": "utils.log_gov_file_info",
                "op_args": [file["file_name"], file["file_location"]]
            },
            "upload_to_s3": {
                "operator": "airflow.operators.python.PythonOperator",
                "task_id": "upload_to_s3",
                "python_callable": "utils.download_and_upload_to_s3",
                "op_args": [file["file_name"], file["file_location"]]
            }
        }
    }

# Save gov_file_prep.yaml configuration
with open(GOV_FILE_DAG_CONFIG_FILE, 'w') as yaml_file:
    yaml.dump(gov_file_dag_config, yaml_file, default_flow_style=False)

# ---- Load and Generate DAGs using DagFactory ----
# Initialize the DagFactory with the YAML paths
for config_file in [CDW_DAG_CONFIG_FILE, GOV_FILE_DAG_CONFIG_FILE]:
    if not os.path.isfile(config_file):
        raise FileNotFoundError(f"Config file not found at {config_file}")
    dag_factory = dagfactory.DagFactory(config_file)

    # Generate DAGs and register them in globals()
    try:
        dag_factory.generate_dags(globals())
    except Exception as e:
        raise RuntimeError(f"Error generating DAGs from {config_file}: {e}")

# Set up task dependencies
dag_ids = [dag_id for dag_id, dag in globals().items() if isinstance(dag, DAG)]

for dag_id in dag_ids:
    dag = globals()[dag_id]
    start_task = dag.get_task("start")
    log_info_task = dag.get_task("log_info")
    upload_to_s3_task = dag.get_task("upload_to_s3")

    # Set task dependencies
    start_task >> log_info_task >> upload_to_s3_task