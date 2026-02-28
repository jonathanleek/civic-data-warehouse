from datetime import datetime
from airflow.sdk import DAG
from airflow.sdk import BaseHook
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator


def print_var():
    my_var = Variable.get("my-test-variable")
    print(f"My variable is: {my_var}")

    conn = BaseHook.get_connection(conn_id="cdw-dev")
    print(conn.get_uri())


with DAG(
    "example_secrets_dag", start_date=datetime(2022, 1, 1), schedule=None
) as dag:
    test_task = PythonOperator(task_id="test-task", python_callable=print_var)
