CDW REQUIREMENTS
- Secret Manager
  - We are using AWS ParameterStore. Example of how to setup connection is included in Dockerfile_Example
  - secret_manager_test.py can be used to confirm you have the secret manager connection configured correctly
- SQL Database
  - We are using  postgreSQL hosted in AWS Aurora.  Connection information is being stored as 'cdw-dev' in secret manager
- S3 Bucket
  - We are using AWS S3. Connection information being stored as 's3_datalake'

CDW START UP PROCESS
- Manually trigger the create_cdw_databases.py dag through the Airflow UI to create the various schemas that the CDW uses
- Update start date of all dags to today's date. Failure to do so will spawn numerous dag runs of each dag
- Turn on all dags in the order they appear in the run order. For initial run, allow each dag to successfully complete before turning the next one on

DAG RUN ORDER
- retrieve_govt_files.py
- s3_to_postgres.py
- compare_staging_schemas.py