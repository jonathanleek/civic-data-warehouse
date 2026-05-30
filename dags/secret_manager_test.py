from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, BaseHook, Variable


def print_var():
    my_var = Variable.get("my-test-variable")
    print(f"My variable is: {my_var}")

    conn = BaseHook.get_connection(conn_id="cdw-dev")
    print(conn.get_uri())


with DAG(
    "example_secrets_dag",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["utility"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
) as dag:
    test_task = PythonOperator(task_id="test-task", python_callable=print_var)
