DBT Setup
=========

Quick start (Windows PowerShell):
1. Install dbt: `pip install dbt-postgres`
2. Point dbt at this repo profile: `$env:DBT_PROFILES_DIR = "dbt"`
3. Validate the connection: `dbt debug --project-dir dbt`
4. Run models: `dbt run --project-dir dbt`

Notes:
- Sources are expected in the `staging` schema.
- Models materialize to the `current` schema by default.
- Example models live in `dbt/models/examples` and are disabled by default.
