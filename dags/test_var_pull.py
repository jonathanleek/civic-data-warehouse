import logging
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

doc_md_DAG = """
### test_var_pull

This is just here to show that the airflow variables are working.
This DAG should probably be removed before final merge to master.
"""


def log_airflow_vars(**kwargs):
    logger = logging.getLogger()
    logger.info("Testing variables...")
    try:
        logger.info(f"Country code : {Variable.get('CDW-COUNTRY-ID')}")
        logger.info(f"Region code : {Variable.get('CDW-REGION-ID')}")
    except Exception:
        logger.error("Exception! Somethings up.")
    logger.info("Test complete.")


with DAG(
    "test_var_pull",
    description="Simple logging of variables",
    schedule=None,
    start_date=datetime(2022, 6, 24),
    catchup=False,
    tags=["development_only"],
    default_args={"retries": 2},
) as dag:
    log_op = PythonOperator(
        task_id="log_airflow_vars",
        python_callable=log_airflow_vars,
    )

    (log_op)
