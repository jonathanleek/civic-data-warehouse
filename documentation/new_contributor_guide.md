## Setting Up Your Local Environment

### Prerequisites
1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) (>= v18.09)
2. Install the [Astro CLI](https://docs.astronomer.io/astro/cli/overview)
3. Clone this repository

### Local Setup (no AWS required)

1. Use the checked-in local Dockerfile at the repository root.
   No copy step is required unless you want to customize it.

2. Create your Airflow settings file:
   ```
   cp airflow_settings.yaml.example airflow_settings.yaml
   ```

3. Start the environment:
   ```
   astro dev start
   ```

This brings up five containers:
- **Airflow Webserver** — http://localhost:8080 (admin/admin)
- **Airflow Scheduler**
- **Airflow Metadata Postgres** — localhost:5444
- **MinIO (local S3)** — http://localhost:9001 (minioadmin/minioadmin)
- **CDW Postgres** — localhost:5433 (cdw_user/cdw_password, database: cdw)

### Running the Pipeline

Trigger DAGs in this order from the Airflow UI:

1. **`cdw_creation`** — creates the staging/current/history schemas in CDW Postgres
2. **`govt_file_download`** — downloads civic data from stlouis-mo.gov, converts to CSV, uploads to MinIO
3. **`staging_table_prep`** — reads CSVs from MinIO, creates and populates staging tables in CDW Postgres

You can browse uploaded files in the MinIO console at http://localhost:9001.

### Cloud Setup (AWS required)

If you need to connect to the production AWS environment instead:

1. Start from the checked-in `Dockerfile`.
2. Fill in your AWS credentials in the Dockerfile as needed for your environment.
3. Remove or skip creating `airflow_settings.yaml` (connections come from AWS SSM)
4. Run `astro dev start`

### Useful Commands

- `astro dev start` — start the local environment
- `astro dev stop` — stop containers (data is preserved)
- `astro dev restart` — rebuild and restart (use after changing Dockerfile or requirements.txt)
- `astro dev kill` — stop and remove all containers and volumes (resets all data)
