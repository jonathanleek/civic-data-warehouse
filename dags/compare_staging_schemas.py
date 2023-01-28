from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
        "compare_staging_schemas",
        start_date=datetime(2022, 12, 30),
        max_active_runs=1,
        schedule=None
) as dag:

# truncate dead_records and updated_records tables
    truncate_dead_records= PostgresOperator(
        task_id = 'truncate dead_records',
        postgres_conn_id = "cdw-dev",
        sql = "include/sql/truncate_dead_records.sql"
    )

    truncate_updated_records = PostgresOperator(
        task_id='truncate updated_records',
        postgres_conn_id="cdw-dev",
        sql="include/sql/truncate_updated_records.sql"
    )
# populate dead_records table with records in staging_2 but not staging_1
# TODO Write SQL. staging_2 LEFT JOIN staging_1 on primary keys
    populate_dead_records = PostgresOperator(
        task_id='populate dead_records',
        postgres_conn_id="cdw-dev",
        sql="include/sql/populate_dead_records"
    )

# populate updated_records table with records in staging_1 but not staging 2 and records that exist in both staging schema, but with differences
# TODO Write SQL. staging_1 LEFT JOIN staging_2 on primary keys UNION (staging_1 INNER JOIN staging_2 on primary keys WHERE  staging_1.non-keys != staging_2.non-keys
populate_updated_records = PostgresOperator(
    task_id='populate updated_records',
    postgres_conn_id="cdw-dev",
    sql="include/sql/populate_updated_records"
)