import logging
import os

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Set up logging
log = logging.getLogger(__name__)

# --- Configuration ---
# Store sensitive information in Airflow Connections and Variables.

# InfluxDB Connection Details (Store in Airflow Connection)
# - Conn Id: 'influxdb_default'
# - Conn Type: 'HTTP'
# - Host: Your InfluxDB URL (e.g., http://localhost:8086)
# - Extra: {"bucket": "your_bucket_name"}
# - Login (org) and Password (token) are optional for no-auth setups.
INFLUXDB_CONN_ID = "influxdb_default"

# Fitbit Credentials (Store in Airflow Variables)
# - fitbit_api_bearer_token: Your Fitbit API Bearer Token.
# - fitbit_user_id (optional): Defaults to '-' for the authenticated user.


@task()
def fetch_fitbit_weight_data(ds=None):
    """
    Fetches weight data for a given date from the Fitbit API using
    a bearer token.
    """
    user_id = Variable.get("fitbit_user_id", default_var="-")
    bearer_token = Variable.get("fitbit_api_bearer_token")
    api_url = f"https://api.fitbit.com/1/user/{user_id}/body/log/weight/date/{ds}.json"

    headers = {"Authorization": f"Bearer {bearer_token}"}

    response = requests.get(api_url, headers=headers)
    response.raise_for_status()

    data = response.json().get("weight", [])
    if not data:
        log.info(f"No weight data found for {ds}.")
        return None

    log.info(f"Found {len(data)} weight log(s) for {ds}.")
    return data


@task()
def write_data_to_influxdb(weight_data):
    """
    Writes the fetched weight data to InfluxDB. Skips if no data is provided.
    Credentials can be supplied via an Airflow connection or environment variables.
    """
    if not weight_data:
        log.info("No weight data to write to InfluxDB. Skipping.")
        return

    try:
        url, token, org, bucket = None, None, None, None
        try:
            # First, try to get credentials from Airflow Connection
            conn = BaseHook.get_connection(INFLUXDB_CONN_ID)
            url = conn.host
            token = conn.password
            org = conn.login
            bucket = conn.extra_dejson.get("bucket")
            log.info("Successfully loaded credentials from Airflow connection.")
        except AirflowNotFoundException:
            # Fallback to environment variables if connection is not found
            log.info(
                f"Airflow connection '{INFLUXDB_CONN_ID}' not found. "
                "Falling back to environment variables."
            )
            url = os.environ.get("INFLUXDB_V2_URL")
            token = os.environ.get("INFLUXDB_V2_TOKEN")
            org = os.environ.get("INFLUXDB_V2_ORG")
            bucket = os.environ.get("INFLUXDB_V2_BUCKET")

        if not all([url, bucket]):
            raise ValueError(
                "InfluxDB URL and bucket are not configured. "
                "Please set up the 'influxdb_default' connection (Host, Extra.bucket) "
                "or provide INFLUXDB_V2_URL and INFLUXDB_V2_BUCKET "
                "environment variables."
            )

        with InfluxDBClient(url=url, token=token, org=org) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)

            points_to_write = []
            for record in weight_data:
                p = (
                    Point("body_metrics")
                    .tag("source", record.get("source", "Unknown"))
                    .field("weight_kg", float(record["weight"]))
                    .field("bmi", float(record["bmi"]))
                    .time(f"{record['date']}T{record['time']}Z")
                )
                points_to_write.append(p)

            write_api.write(bucket=bucket, org=org, record=points_to_write)
            log.info(f"Successfully wrote {len(points_to_write)} points to InfluxDB.")

    except Exception as e:
        log.error(f"Error writing to InfluxDB: {e}")
        raise


@dag(
    dag_id="fitbit_to_influxdb_weight",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    doc_md="""
    ### Fitbit to InfluxDB Weight Data Pipeline

    This DAG fetches daily weight data from the Fitbit API and writes it to an InfluxDB bucket.

    **Required Setup:**
    1.  **InfluxDB Credentials (choose one):**
        -   **Airflow Connection (Recommended):**
            -   `Conn Id`: `influxdb_default`
            -   `Conn Type`: `HTTP`
            -   `Host`: Your InfluxDB URL (e.g., `http://influxdb:8086`)
            -   `Extra`: `{"bucket": "your_bucket_name"}`
            -   `Login` (org) and `Password` (token) are optional for no-auth setups.
        -   **Environment Variables:**
            -   `INFLUXDB_V2_URL`: The URL of your InfluxDB instance.
            -   `INFLUXDB_V2_BUCKET`: The bucket to write data to.
            -   `INFLUXDB_V2_TOKEN` and `INFLUXDB_V2_ORG` are optional for no-auth setups.

    2.  **Airflow Variables for Fitbit:**
        -   `fitbit_api_bearer_token`: Your Fitbit API Bearer Token.
        -   `fitbit_user_id` (optional): Defaults to `-` for the authenticated user.
    """,
    tags=["fitbit", "influxdb", "api", "health"],
)
def fitbit_to_influxdb_dag():
    """
    DAG to fetch weight data from Fitbit and load it into InfluxDB.
    """
    # Define the DAG's task flow
    weight_data_from_api = fetch_fitbit_weight_data()
    write_data_to_influxdb(weight_data_from_api)


# Instantiate the DAG
fitbit_to_influxdb_dag_instance = fitbit_to_influxdb_dag()
