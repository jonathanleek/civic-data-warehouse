Overview
========

Welcome to Civic Data Warehouse! This project was generated using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine. Documentation specific to the civic data warehouse can be found in the documentation directory.

Project Contents
================

This Astro project contains the following files and folders:

File/Folder | Description 
--- | ---
dags | This folder contains the Python files for your Airflow DAGs. By default, this directory includes an example DAG that runs every 30 minutes and simply prints the current date. It also includes an empty 'my_custom_function' that you can fill out to execute Python code.
documenation | contains documentation about the Civic Data Warehouse and the proposed Real Estate Data Specification.
Dockerfile | This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
include | This folder contains any additional files that you want to include as part of your project. It is empty by default.
packages.txt | Install OS-level packages needed for your project by adding them to this file. It is empty by default.
requirements.txt | Install Python packages needed for your project by adding them to this file. It is empty by default.
plugins | Add custom or community plugins for your project to this file. It is empty by default.
airflow_settings.yaml | Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Requirements
===========================

- [astro cli](https://docs.astronomer.io/astro/cli/install-cli)
- [docker desktop](https://docs.docker.com/get-docker/) `>= v18.09`


Local Development Setup
=======================

The `cdw_creation` and `staging_table_prep` DAGs require a `cdw-dev` Postgres connection. In production this points to AWS Aurora. For local development, a self-contained Postgres container is provided — no AWS credentials required.

### Prerequisites

- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)
- [Docker Desktop](https://docs.docker.com/get-docker/) >= v18.09

### First-time setup

**1. Copy the Airflow settings template**

```
cp airflow_settings.yaml.example airflow_settings.yaml
```

`airflow_settings.yaml` is gitignored. The example file pre-configures the `cdw-dev` connection to point at the local Postgres container.

**2. Disable the AWS secrets backend**

Create a `.env` file in the project root (also gitignored):

```
AIRFLOW__SECRETS__BACKEND=
AIRFLOW__SECRETS__BACKEND_KWARGS=
```

This stops Airflow from querying AWS SSM for connections. Without it, the SSM backend overrides the local `cdw-dev` connection even when AWS credentials are absent.

**3. Start the stack**

```
astro dev start
```

This starts five containers: Airflow webserver, scheduler, triggerer, metadata Postgres, and the local CDW Postgres. Verify with:

```
docker ps
```

You should see a container whose name ends in `cdw-postgres` mapped to host port `5433`.

**4. Register the cdw-dev connection**

Astro reads `airflow_settings.yaml` on start, but if it fails (e.g. in a non-interactive shell), add the connection manually:

```
docker exec <project>-webserver-1 airflow connections add cdw-dev \
  --conn-type postgres \
  --conn-host cdw-postgres \
  --conn-schema postgres \
  --conn-login postgres \
  --conn-password postgres \
  --conn-port 5432
```

Replace `<project>-webserver-1` with the actual container name shown by `docker ps`.

**5. Initialize CDW schemas**

Open the Airflow UI at http://localhost:8080 (login: `admin` / `admin`) and trigger the `cdw_creation` DAG manually. All tasks should succeed. This creates the `staging`, `current`, and `history` schemas in the local CDW database.

**6. (Optional) Connect with DBeaver or another SQL client**

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5433` |
| Database | `postgres` |
| User | `postgres` |
| Password | `postgres` |

### Port reference

| Service | Host port | Internal hostname | Notes |
|---|---|---|---|
| Airflow UI | 8080 | — | http://localhost:8080 |
| Airflow metadata Postgres | 5432 | `postgres` | Internal Airflow use only |
| CDW Postgres | 5433 | `cdw-postgres` | `localhost:5433` externally; `cdw-postgres:5432` inside Docker |

### Subsequent starts

`astro dev start` and `astro dev restart` will bring everything up automatically. The CDW Postgres data persists in a named Docker volume (`cdw-postgres-data`) between restarts. To wipe and reinitialize:

```
astro dev stop
docker volume rm <project>_cdw-postgres-data
astro dev start
```

Then re-trigger `cdw_creation` to recreate the schemas.


Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://docs.astronomer.io/cloud/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support team: https://support.astronomer.io/
