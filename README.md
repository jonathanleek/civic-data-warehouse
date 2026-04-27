Civic Data Warehouse
====================

The Civic Data Warehouse (CDW) is an open-source data warehousing project that ingests, normalizes, and stores public civic data from the City of St. Louis. The goal is to build a clean, queryable real estate database from raw government data sources — parcel records, building attributes, permits, property sales, inspections, and more.

The project uses [Apache Airflow](https://airflow.apache.org/) (via the [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/overview)) to orchestrate ETL pipelines that download public data files, stage them into a local PostgreSQL data warehouse, and transform them into a normalized schema. Documentation specific to the data model and legacy ETL logic can be found in the `documentation/` directory.

Requirements
============

- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) — the Astronomer command-line tool for running Airflow locally
- [Docker Desktop](https://docs.docker.com/get-docker/) `>= v18.09`

Getting Started
===============

The Astro CLI wraps Docker Compose to provide a one-command local Airflow environment. Key commands:

| Command | Description |
|---------|-------------|
| `astro dev start` | Start all containers (Airflow + CDW services) |
| `astro dev stop` | Stop all containers |
| `astro dev restart` | Restart the environment |
| `astro dev logs` | Tail logs from Airflow containers |
| `astro dev bash` | Open a shell in the Airflow scheduler container |
| `astro dev run` | Run a single Airflow CLI command |

For full CLI reference, see the [Astro CLI documentation](https://www.astronomer.io/docs/astro/cli/reference).

1. Install the Astro CLI and Docker Desktop using the links above.
2. Copy the example settings file: `cp airflow_settings.yaml.example airflow_settings.yaml`
3. Start the project: `astro dev start`
4. Open the Airflow UI at http://localhost:8080 (username: `admin`, password: `admin`).

Infrastructure Services
=======================

Beyond the standard Airflow containers (scheduler, API server, DAG processor, triggerer, and Airflow metadata Postgres), the `docker-compose.override.yml` defines two additional services that make up the CDW data platform:

LocalStack (S3 Emulator)
-------------------------

A local AWS S3 emulator that serves as the **landing zone** for raw data files. The `govt_file_download` DAG downloads public data and uploads it here. The `staging_table_prep` DAG reads from here to populate staging tables.

- **Image:** `gresau/localstack-persist:latest` (data persists across restarts via a Docker volume)
- **Service:** S3 only
- **Bucket:** `civic-data-warehouse-lz` (auto-created on startup by the `localstack-init` container)

| | Connection Details |
|---|---|
| **From Airflow (conn_id: `s3_datalake`)** | Endpoint: `http://localstack:4566`, Region: `us-east-1`, Access Key: `test`, Secret Key: `test` |
| **Direct access from host** | Endpoint: `http://localhost:4566`, e.g. `aws --endpoint-url=http://localhost:4566 s3 ls s3://civic-data-warehouse-lz/` (use `AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test`) |

CDW PostgreSQL (Data Warehouse)
-------------------------------

A dedicated Postgres 15 instance that serves as the **data warehouse** itself. This is separate from Airflow's internal metadata database. It contains three schemas:

- **`staging`** — raw data loaded directly from S3 CSVs
- **`current`** — normalized tables (parcel, building, unit, address, legal_entity, etc.)
- **`history`** — historical snapshots of the current schema

| | Connection Details |
|---|---|
| **From Airflow (conn_id: `cdw-dev`)** | Host: `cdw-postgres`, Port: `5432`, Database: `cdw`, User: `cdw_user`, Password: `cdw_password` |
| **Direct access from host** | `psql -h localhost -p 5433 -U cdw_user -d cdw` (password: `cdw_password`). Note: mapped to host port **5433** to avoid conflict with Airflow's Postgres on 5432. |

DAGs
====

All DAGs are manually triggered (`schedule=None`) unless noted otherwise. They should be run in the order listed below for initial setup.

### 1. `cdw_creation` — Database Schema Setup

**File:** `dags/create_cdw_databases.py`

Creates the three CDW schemas (`staging`, `current`, `history`) and a utility function for truncating tables. Run this once before any other DAG.

**Task order:** `create_staging` > `create_data_prep` > `create_history` > `create_truncate_tables_function`

### 2. `govt_file_download` — Data Ingestion

**File:** `dags/govt_file_download.py`

Downloads public civic data files from stlouis-mo.gov, unzips archives, converts `.mdb` (Microsoft Access) files to CSVs, and uploads everything to the `civic-data-warehouse-lz` S3 bucket. Sources include:

- Parcel/assessor data (`prcl.zip`)
- Building permits (last 30 days)
- Demolition, electrical, mechanical, occupancy, and plumbing permits (last 30 days)
- Historical conservation data
- Property sales
- Inspections, condemnations, and vacancies
- LRA public data
- Forestry maintenance properties

**Task order:** `upload_all_files` (parallel per file) > `clear_tmp_directory`

### 3. `staging_table_prep` — Staging Table Population

**File:** `dags/staging_table_prep.py`

Truncates the `staging` schema, lists all files in the S3 landing zone, then dynamically creates and populates a staging table for each file using dynamic task mapping.

**Known issue:** The `forestry_maintenance_properties` table fails to populate due to quoted column names in the source CSV.

**Task order:** `truncate_staging` > `S3_List` > `prepare_list` > `create_staging_tables` > `populate_staging_tables`

### 4. `prcl_owner_parsing` - Owner Address Parsing

**File:** `dags/prcl_owner_parsing.py`

Rebuilds `current.int_prcl_owners` from distinct owner fields in `staging.prcl_prcl`. The DAG uses `usaddress` to parse `owneraddr` into mailing address components, then uses Splink to write probabilistic duplicate-owner candidates to `current.int_prcl_owner_match_candidates`.

High-confidence matches with `match_weight >= 42.0` are written to `current.parcel_owner_mart`, with the matched owner records and parcel JSON arrays for each side of the match. Possible matches with `30.0 <= match_weight < 42.0` are exposed in `current.prcl_owner_match_review_queue`. Human decisions are stored in `current.prcl_owner_match_reviews`; accepted decisions are included in the mart as `splink_matched_human_reviewed`.

To accept a possible match for the next mart rebuild:

```sql
insert into current.prcl_owner_match_reviews (match_id, review_decision, reviewer, review_notes)
values ('<match_id>', 'accepted', '<name>', '<notes>')
on conflict (match_id) do update
set review_decision = excluded.review_decision,
    reviewer = excluded.reviewer,
    review_notes = excluded.review_notes,
    reviewed_at = now();
```

The possible-match review app can be run locally with:

```bash
streamlit run apps/owner_match_review.py
```

It connects to the local CDW Postgres service by default (`localhost:5433`, database `cdw`) and writes review decisions to `current.prcl_owner_match_reviews`.

**Task order:** `create_int_prcl_owners` > `create_int_prcl_owner_match_candidates` > `create_parcel_owner_mart`

### Utility DAGs

| DAG | File | Purpose |
|-----|------|---------|
| `sql_test` | `dags/sql_test.py` | Connectivity check — verifies the `staging`, `current`, and `history` schemas exist in the CDW database. |
| `example_secrets_dag` | `dags/secret_manager_test.py` | Tests secrets backend integration by retrieving an Airflow Variable and the `cdw-dev` connection. |
| `survey_dag_parsing_times` | `dags/dag_parsing_survey.py` | Profiles DAG parse times. **Only DAG with a schedule** (daily). |

Project Structure
=================

| Path | Description |
|------|-------------|
| `dags/` | Airflow DAG definitions |
| `include/` | Supporting Python modules, SQL scripts, and config files (e.g., `gov_files.json`, `create_current.sql`) |
| `documentation/` | CDW data model schema docs and legacy ETL logic analysis |
| `plugins/` | Custom Airflow plugins |
| `tests/` | Tests |
| `Dockerfile` | Astro Runtime image (currently `3.1-13`, based on Airflow 3.x) |
| `docker-compose.override.yml` | CDW-specific services (LocalStack, CDW Postgres) and Airflow connection env vars |
| `requirements.txt` | Python dependencies |
| `packages.txt` | OS-level packages |
| `airflow_settings.yaml.example` | Example Airflow connections and variables config |

Deployment
==========

For deploying to Astronomer Cloud, refer to the [Astronomer deployment documentation](https://www.astronomer.io/docs/astro/deploy-code).
