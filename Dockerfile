FROM astrocrpublic.azurecr.io/runtime:3.1-13

ENV AWS_DEFAULT_REGION=us-east-1

# Pre-generate the dbt manifest at image build time so Cosmos can use
# LoadMode.DBT_MANIFEST instead of shelling out to `dbt ls` on every parse cycle.
# dbt parse does not make database connections, so no credentials are required here.
COPY dbt/ /usr/local/airflow/dbt/
RUN cd /usr/local/airflow/dbt && dbt parse --profiles-dir . --project-dir .