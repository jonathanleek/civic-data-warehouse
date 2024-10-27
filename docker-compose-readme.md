### How to Use the Docker Compose File from the Git Repository

#### Prerequisites:

- **Docker**: Ensure Docker is installed on your system. [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose**: Ensure Docker Compose is installed. [Install Docker Compose](https://docs.docker.com/compose/install/)

#### Steps to Run the Docker Compose File:

1. **Clone the Repository**:

   - Open your terminal.
   - Clone the repository using the following command:

     ```bash
     git clone <repository_url>
     ```

     Replace `<repository_url>` with the actual URL of your Git repository.

2. **Navigate to the Project Directory**:

   - Change your current directory to the cloned repository:

     ```bash
     cd <repository_directory>
     ```

     Replace `<repository_directory>` with the name of the cloned directory.

3. **Start the Containers**:

   - Run the following command to start all services defined in the `docker-compose.yml` file:

     ```bash
     docker-compose up -d
     ```

     - The `-d` flag runs the containers in detached mode (in the background).

4. **Verify the Containers are Running**:

   - To check the status of the containers, use:

     ```bash
     docker-compose ps
     ```

     - This command lists all the containers along with their current status and exposed ports.

5. **Access the Services**:

   - **pgAdmin**:
     - URL: `http://localhost:5050`
     - Login Credentials:
       - **Email**: `admin@admin.com`
       - **Password**: `admin`
     - Use pgAdmin to manage your PostgreSQL databases via a web interface.

   - **Apache Superset**:
     - URL: `http://localhost:8088`
     - Login Credentials:
       - **Username**: `admin`
       - **Password**: `admin`
     - Superset is a data exploration and visualization platform.

   - **S3Mock**:
     - HTTP URL: `http://localhost:9090`
     - HTTPS URL: `https://localhost:9191`
     - Use S3Mock to simulate interactions with Amazon S3 locally.

   - **Civic Data Warehouse PostgreSQL**:
     - **Host**: `localhost`
     - **Port**: `6432`
     - **Database**: `civic_data_warehouse`
     - **User**: `civic_user`
     - **Password**: `civic_password`

   - **Superset PostgreSQL**:
     - **Host**: `localhost`
     - **Port**: `7432`
     - **Database**: `superset`
     - **User**: `superset_user`
     - **Password**: `superset_password`

6. **Stopping the Containers**:

   - When you're done and want to stop all the services, run:

     ```bash
     docker-compose down
     ```

     - This command stops and removes the containers but preserves the volumes, so your data remains intact.

#### Troubleshooting Tips:

- If you encounter any issues, you can check the logs of a specific service using:

  ```bash
  docker-compose logs <service_name>
  ```

  Replace `<service_name>` with the name of the service (e.g., `superset`, `pgadmin`, etc.).

- Ensure no other services are running on the ports used by the containers to avoid port conflicts.

#### What the Docker Compose File Does:

The `docker-compose.yml` file orchestrates multiple services to create a comprehensive development environment. Here's a breakdown of what each service does:

1. **`civic-data-warehouse`**:

   - **Purpose**: Runs a PostgreSQL database serving as the Civic Data Warehouse.
   - **Configuration**:
     - **Image**: `postgres:latest`
     - **Ports**: Exposes port `6432` on the host, mapping to the container's PostgreSQL default port `5432`.
     - **Environment Variables**:
       - `POSTGRES_USER`: `civic_user`
       - `POSTGRES_PASSWORD`: `civic_password`
       - `POSTGRES_DB`: `civic_data_warehouse`
     - **Data Persistence**: Uses the volume `cdw_civic_data_warehouse_data_volume` to store database data persistently.

2. **`superset-postgres`**:

   - **Purpose**: Runs a PostgreSQL database for Apache Superset's metadata storage.
   - **Configuration**:
     - **Image**: `postgres:latest`
     - **Ports**: Exposes port `7432` on the host.
     - **Environment Variables**:
       - `POSTGRES_USER`: `superset_user`
       - `POSTGRES_PASSWORD`: `superset_password`
       - `POSTGRES_DB`: `superset`
     - **Data Persistence**: Uses the volume `cdw_superset_postgres_data_volume`.

3. **`s3mock`**:

   - **Purpose**: Provides a mock S3 service for local development and testing.
   - **Configuration**:
     - **Image**: `adobe/s3mock`
     - **Ports**: Exposes ports `9090` (HTTP) and `9191` (HTTPS).
     - **Environment Variables**:
       - `initialBuckets`: `my-bucket` (automatically creates this bucket on startup)
     - **Data Persistence**: Uses the volume `cdw_s3mock_data_volume`.

4. **`superset`**:

   - **Purpose**: Runs Apache Superset for data exploration and visualization.
   - **Configuration**:
     - **Image**: `apache/superset:latest`
     - **Ports**: Exposes port `8088` on the host.
     - **Environment Variables**:
       - `SUPERSET_ENV`: `development`
       - `SUPERSET_CONFIG_PATH`: `/app/pythonpath/superset_config.py`
       - **Database Connection**:
         - `POSTGRES_USER`: `superset_user`
         - `POSTGRES_PASSWORD`: `superset_password`
         - `POSTGRES_DB`: `superset`
         - `POSTGRES_HOST`: `superset_postgres`
         - `POSTGRES_PORT`: `5432` (internal to Docker network)
     - **Dependencies**: Waits for `superset-postgres` to be ready before starting.
     - **Data Persistence**: Uses the volume `cdw_superset_home_data_volume`.
     - **Initialization Command**: Sets up the Superset database, creates an admin user, loads example data, and starts the web server.

5. **`pgadmin`**:

   - **Purpose**: Provides a web-based interface for managing PostgreSQL databases.
   - **Configuration**:
     - **Image**: `dpage/pgadmin4:latest`
     - **Ports**: Exposes port `5050` on the host.
     - **Environment Variables**:
       - `PGADMIN_DEFAULT_EMAIL`: `admin@admin.com`
       - `PGADMIN_DEFAULT_PASSWORD`: `admin`
     - **Data Persistence**: Uses the volume `cdw_pgadmin_data_volume`.

#### Volumes:

- **Purpose**: Volumes are used to persist data outside of the containers, so your data isn't lost when containers stop or are removed.
- **Defined Volumes**:
  - `cdw_civic_data_warehouse_data_volume`
  - `cdw_superset_postgres_data_volume`
  - `cdw_s3mock_data_volume`
  - `cdw_superset_home_data_volume`
  - `cdw_pgadmin_data_volume`

#### Networking:

- Docker Compose sets up a default network for all the services, allowing them to communicate with each other using their service names as hostnames.
- **External Access**:
  - Services are mapped to specific ports on `localhost` for external access (e.g., `pgAdmin` on port `5050`).

#### Summary:

By cloning the repository and running `docker-compose up -d`, you set up a full development environment with:

- **PostgreSQL Databases**: For both the Civic Data Warehouse and Superset metadata.
- **Apache Superset**: For data visualization and exploration.
- **pgAdmin**: For database management.
- **S3Mock**: For simulating Amazon S3 interactions locally.

All services are containerized, isolated, and can be managed easily using Docker Compose commands.

#### Next Steps:

- **Data Exploration**: Use Superset to connect to the Civic Data Warehouse and start exploring data.
- **Database Management**: Use pgAdmin to manage your PostgreSQL databases, run queries, and perform administrative tasks.
- **Testing S3 Interactions**: Use the S3Mock service to test applications that interact with Amazon S3 without incurring costs or needing AWS credentials.

Remember to stop the services when not in use to free up system resources.