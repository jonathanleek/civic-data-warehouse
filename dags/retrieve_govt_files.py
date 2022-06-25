from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import python_operator, bash_operator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

govt_file_dict=[
    {
    'file_name': 'prcl.zip',
    'file_location': 'https://www.stlouis-mo.gov/data/upload/data-files/prcl.zip'
    },
    {
        'file_name': 'building_permit_codes.zip',
        'file_location': 'https://www.stlouis-mo.gov/data/upload/data-files/codes.zip'
    },
    {
        'file_name': 'building_permits_emp.zip',
        'file_location': 'https://www.stlouis-mo.gov/data/upload/data-files/prmemp.zip'
    },
    {
        'file_name': 'building_permits_bdo.zip',
        'file_location': 'https://www.stlouis-mo.gov/data/upload/data-files/prmbdo.zip'
    },
    {
        'file_name': 'historical_conservation.zip',
        'file_location': 'https://www.stlouis-mo.gov/data/upload/data-files/hcd.zip'
    },
    {
        'file_name': 'property_sales.zip',
        'file_location': 'https://www.stlouis-mo.gov/data/upload/data-files/prclsale.zip'
    },
    {
        'file_name': 'inspections_condemnations_vacancies.zip',
        'file_location': 'https://www.stlouis-mo.gov/data/upload/data-files/condemn.zip'
    },
    {
        'file_name': 'lra_public.zip',
        'file_location': 'https://www.stlouis-mo.gov/data/upload/data-files/lra_public.zip'
    },
    {
        'file_name': 'forestry_maintenance.zip',
        'file_location': 'https://www.stlouis-mo.gov/data/upload/data-files/forestry-maintenance-properties.csv'
    }
]

with DAG('govt_file_download',
         description = 'Downloads public civic data and saves to s3 for processing',
         schedule_interval= timedelta(days=1),
         start_date=datetime(2022, 6, 24),
         catchup=False
         ) as dag:
    for file in govt_file_dict:
        download_file = BashOperator(
            task_id='download_file_' + file['file_name'],
            bash_command='wget -P /home/zips/' + file['file_name'] + ' ' + file['file_location']
        )

        upload_file = S3CreateObjectOperator(
            task_id="upload_file_" + file['file_name'],
            aws_conn_id='civic-data-warehouse-lz',
            s3_bucket='civic-data-warehouse-lz',
            s3_key="raw_zips/" + file['file_name'],
            data=file['file_name'],
            replace=True,
        )
    download_file >> upload_file