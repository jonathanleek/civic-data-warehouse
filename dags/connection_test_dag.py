from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

with (DAG(
    dag_id='test_connections_dag',
    default_args=default_args,
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False,
) as dag):

    # Test Postgres connection using the new SQLExecuteQueryOperator
    test_postgres = SQLExecuteQueryOperator(
        task_id='test_postgres',
        conn_id='civic_data_warehouse',
        sql='SELECT 1;',
        hook_params={'schema':'civic_data_warehouse'},
    )

    # Test S3 mock connection
    test_s3 = S3ListOperator(
        task_id='test_s3',
        aws_conn_id='s3mock',
        bucket='my-bucket',
    )

    # test_postgres >>
    test_s3
