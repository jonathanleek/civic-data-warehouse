# Changes — Local CDW Postgres for Development

## Problem

The core DAGs (`cdw_creation`, `staging_table_prep`) require a `cdw-dev` Postgres connection. That connection was sourced from an AWS SSM Parameter Store backend, meaning developers needed live AWS credentials just to run anything locally.

## What Changed

### New files

**`docker-compose.override.yml`**
Adds a `cdw-postgres` service (Postgres 14) to the Astro Docker stack. Astro CLI automatically merges this file on `astro dev start`, so no extra commands are needed. The container joins Astro's internal `airflow` network so all Airflow services can reach it by hostname.

Two non-obvious settings were required:
- `POSTGRES_HOST_AUTH_METHOD: md5` — Postgres 14 defaults to SCRAM-SHA-256 authentication, which the version of libpq bundled with Airflow 2.5 cannot handle. Forcing `md5` restores compatibility.
- `networks: [airflow]` — without this, the container lands on Docker Compose's `default` network, which is separate from the `airflow` network that Astro's containers use. Adding it here causes the service name `cdw-postgres` to be registered as a DNS alias on the right network automatically.

**`airflow_settings.yaml.example`**
Template developers copy to `airflow_settings.yaml` (gitignored) on first setup. Pre-configured with the `cdw-dev` connection pointed at `cdw-postgres:5432`.

### Modified files

**`.env`** (gitignored, created locally)
Disables the AWS SSM secrets backend for local development:
```
AIRFLOW__SECRETS__BACKEND=
AIRFLOW__SECRETS__BACKEND_KWARGS=
```
This was necessary because the SSM backend takes precedence over the Airflow metastore when resolving connections. Even with a local `cdw-dev` connection added to the metastore DB, SSM would win and return the Aurora endpoint. Clearing the backend lets the metastore connection win.

Note: Airflow 2.5's environment-variable connection override (`AIRFLOW_CONN_*`) does not normalize hyphens in connection IDs, so `AIRFLOW_CONN_CDW_DEV` does not map to `cdw-dev`. The `.env` approach (disabling SSM) is the reliable workaround.

**`README.md`**
Replaced the placeholder Local Development Setup section with accurate step-by-step instructions reflecting all of the above.

## Port Map

| Service | Host port | Internal hostname |
|---|---|---|
| Airflow UI | 8080 | — |
| Airflow metadata Postgres | 5432 | `postgres` |
| CDW Postgres | 5433 | `cdw-postgres` |
