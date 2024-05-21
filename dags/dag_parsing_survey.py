from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagBag
from datetime import datetime, timedelta
import time


def survey_dag_parsing_times(**kwargs):
    """
    Surveys the parsing time of DAGs using dagbag.process_file(dag.fileloc).

    Args:
    kwargs (dict): Airflow's context passed by the PythonOperator.

    Returns:
    dict: A dictionary with the DAG names as keys and their parsing times as values.
    """
    dagbag = DagBag()
    parsing_times = {}

    for dag_id, dag in dagbag.dags.items():
        start_time = time.time()
        dagbag.process_file(dag.fileloc)
        end_time = time.time()

        parsing_time = end_time - start_time
        parsing_times[dag_id] = parsing_time

    # Log the parsing times
    for dag_id, parsing_time in parsing_times.items():
        print(f"DAG: {dag_id} parsed in {parsing_time:.4f} seconds")

    return parsing_times


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "survey_dag_parsing_times",
    default_args=default_args,
    description="A DAG to survey DAG parsing times",
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    survey_task = PythonOperator(
        task_id="survey_dag_parsing_times_task",
        python_callable=survey_dag_parsing_times,
        provide_context=True,
    )

    survey_task
