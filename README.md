# Airflow Pipelines

This repository stores custom Apache Airflow pipelines (DAGs) for various data integration tasks.

## Prerequisites

- Python 3.8+
- [uv](https://github.com/astral-sh/uv)

## Development Setup

To get started with development, you need to set up a local virtual environment using `uv`.

1.  **Create and activate the virtual environment:**
    `uv` can create a virtual environment in a `.venv` directory.
    ```bash
    uv venv
    ```
    Then, activate it using the script for your shell:
    ```bash
    # On macOS and Linux
    source .venv/bin/activate
    ```

2.  **Install dependencies:**
    Once the virtual environment is activated, sync it with the `uv.lock` file. This command ensures your environment has the exact versions of all dependencies specified in the lock file.
    ```bash
    uv sync --locked --all-extras --dev
    ```

## Running Tests

This project uses `pytest` for testing. With your virtual environment activated, you can run the entire test suite with a single command:

```bash
pytest
```

## DAG Configuration

The DAGs in this repository require specific Airflow Connections and/or Variables to be configured in your Airflow environment.

### Fitbit to InfluxDB Weight DAG (`fitbit_to_influxdb_weight`)

This DAG fetches daily weight data from the Fitbit API and writes it to InfluxDB.

#### InfluxDB Credentials (Choose one method)

1.  **Airflow Connection (Recommended):**
    -   **Conn Id:** `influxdb_default`
    -   **Conn Type:** `HTTP`
    -   **Host:** Your InfluxDB URL (e.g., `http://influxdb:8086`)
    -   **Extra:** `{"bucket": "your_bucket_name"}`
    -   *Note: `Login` (for org) and `Password` (for token) are optional for no-auth setups.*

2.  **Environment Variables:**
    -   `INFLUXDB_V2_URL`: The URL of your InfluxDB instance.
    -   `INFLUXDB_V2_BUCKET`: The bucket to write data to.
    -   *Note: `INFLUXDB_V2_TOKEN` and `INFLUXDB_V2_ORG` are optional.*

#### Fitbit Credentials

-   **Airflow Variable:**
    -   **Key:** `fitbit_api_bearer_token`
    -   **Value:** Your Fitbit API Bearer Token.
-   **Airflow Variable (Optional):**
    -   **Key:** `fitbit_user_id`
    -   **Value:** Your Fitbit user ID. Defaults to `-` (the authenticated user) if not provided.

## Deployment

To deploy these DAGs, copy the contents of the `dags/` directory to the `dags/` folder of your Apache Airflow instance. Ensure that all dependencies from `pyproject.toml` are installed in your Airflow environment. For reproducible deployments, it is recommended to install them from the `uv.lock` file.
