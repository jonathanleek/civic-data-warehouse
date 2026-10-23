CDW Process
===========

Requirements
------------
- **Secrets / connections.** Locally, connections come from `docker-compose.override.yml`
  (`cdw-dev` → the CDW Postgres, `s3_datalake` → LocalStack). In a hosted deployment they come
  from the secrets backend (AWS Parameter Store); `dags/secret_manager_test.py` checks that the
  backend is wired up.
- **Variables.** `CDW-COUNTRY-ID` (ISO 3166-1 numeric, `840`) and `CDW-REGION-ID` (county FIPS,
  `29510`) set the CDW_ID namespace (`documentation/schema/cdw_id.md`). `CDW-REGION-NAME`
  (default `St. Louis City`) names the municipality row. Locally the first two come from
  `.airflow.vars.stl.env.shared`.

Start-up (once per database)
----------------------------
1. Trigger `cdw_creation`. It creates the `staging`, `crosswalk`, `etl`, `current` and `history`
   schemas. `crosswalk` and `history` hold persistent state and are never dropped by later runs.

DAG run order
-------------
All DAGs are `schedule=None` except the transform layers after the first, which run on Airflow
Assets.

| # | DAG | Trigger | What it does |
|---|-----|---------|--------------|
| 1 | `govt_file_download` | manual | Download city files → S3 landing zone |
| 2 | `staging_table_prep` | manual | S3 CSVs → `staging.*` (all TEXT) |
| 3 | `cdw_transform_reference` | manual | Lookups → `current` |
| 4 | `cdw_transform_entities` | Asset from 3 | address, legal_entity |
| 5 | `cdw_transform_spine` | Asset from 4 | parcel, building, unit, secondary_owner |
| 6 | `cdw_transform_events` | Asset from 5 | assessment, tax_delinquency, permit, inspection, condemnation, service |
| 7 | `cdw_transform_history` | Asset from 6 | version `current` into `history` |
| 8 | `cdw_transform_qa` | Asset from 7 | data-quality checks; fails on errors |

So a refresh is: run 1, then 2, then trigger 3. Steps 4–8 follow on their own.

When a transform fails
----------------------
- **`source_views` task** — the city changed a staging table or column the transforms need. The
  error names it. Fix the mapping in `include/sql/current/source_views.sql`.
- **A transform task with "Capture-all guard failed"** — rows went missing. Compare
  `etl.feed_reconciliation` with `current.exclusion_log` for that feed.
- **`cdw_transform_qa`** — see `SELECT * FROM etl.qa_result WHERE failing_rows > 0`.

Design reference: `documentation/pipeline_architecture/staging_to_current.md`.
