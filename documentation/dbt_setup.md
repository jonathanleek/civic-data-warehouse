DBT Setup Guide
===============

Goal
----
Use dbt to transform data from the `staging` schema into production-ready tables in the `current` schema.

What was added
--------------
- `dbt/dbt_project.yml`: dbt project configuration.
- `dbt/profiles.yml`: connection profile that uses environment variables with sensible defaults for local Postgres.
- `dbt/models/examples/*`: example models and sources (disabled by default).

How it works
------------
1. Staging tables are created and populated by Airflow (see `dags/staging_table_prep.py`).
2. dbt reads those staging tables as `sources`.
3. dbt models clean, type-cast, and reshape the data into production tables in the `current` schema.

Local quick start
-----------------
1. Install dbt: `pip install dbt-postgres`
2. Set the profile directory: `$env:DBT_PROFILES_DIR = "dbt"`
3. Validate the connection: `dbt debug --project-dir dbt`
4. Run models: `dbt run --project-dir dbt`

Adjusting for real tables
-------------------------
- Add source definitions for each staging table in `dbt/models/staging/schema.yml` (or update the example file).
- Create one dbt model per target table in `dbt/models/current/`.
- Use `{{ source('staging', 'table_name') }}` to read from staging.
- Use `{{ ref('model_name') }}` to build dependencies between models.

Airflow integration (next step)
-------------------------------
Once the dbt project is stable, add an Airflow task that runs dbt after the staging load.
