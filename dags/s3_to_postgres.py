from airflow.models import Variable
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from include.retrieve_gov_file import retrieve_gov_file
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator



# get most recent date from staging schema
# get list of files updated/uploaded since that date from s3
s3_file_list = S3ListOperator(
    task_id='list_s3_objects',
    aws_conn_id='s3_datalake',
    bucket='civic-data-warehouse-lz',
    prefix='unpacked/',
    delimiter='/'
)

# for each file in list, create/recreate table in staging_1