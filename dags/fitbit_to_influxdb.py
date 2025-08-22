import base64
import logging

import httpx
import pendulum
from airflow.hooks.base import BaseHook
from airflow.providers.influxdb.hooks.influxdb import InfluxDBHook
from airflow.sdk import Variable, dag, task

# Set up logging
log = logging.getLogger(__name__)

# --- Configuration ---
# Store sensitive information in Airflow Connections and Variables.

# InfluxDB Connection Details (Store in Airflow Connection)
# This DAG uses the InfluxDB Airflow Provider.
INFLUXDB_CONN_ID = "influxdb_default"

# Fitbit Connection Details (Store in Airflow Connection)
FITBIT_CONN_ID = "fitbit_default"


@task()
def get_fitbit_oauth_token():
    """
    Refreshes and returns a Fitbit OAuth2 access token. If a new refresh
    token is issued, it is stored in an Airflow Variable.
    """
    try:
        # Retrieve connection details from Airflow
        conn = BaseHook.get_connection(FITBIT_CONN_ID)
        client_id = conn.login
        client_secret = conn.password
        refresh_token = Variable.get("fitbit_refresh_token", default=None)

        if not all([client_id, client_secret, refresh_token]):
            raise ValueError(
                "Fitbit connection is missing client_id/secret, or "
                "the 'fitbit_refresh_token' Variable is not set."
            )

        # Fitbit token refresh endpoint
        token_url = "https://api.fitbit.com/oauth2/token"

        # Prepare the authorization header for Basic Auth
        auth_string = f"{client_id}:{client_secret}"
        encoded_auth_string = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")
        headers = {
            "Authorization": f"Basic {encoded_auth_string}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        # Prepare the request payload
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        # Make the POST request to refresh the token
        with httpx.Client(timeout=30.0) as client:
            response = client.post(token_url, data=payload, headers=headers)
        response.raise_for_status()
        token_data = response.json()

        new_access_token = token_data["access_token"]
        new_refresh_token = token_data.get("refresh_token")

        # If a new refresh token is issued, update the Airflow Variable.
        if new_refresh_token and new_refresh_token != refresh_token:
            log.info("Updating Airflow Variable with new Fitbit refresh token.")
            Variable.set("fitbit_refresh_token", new_refresh_token)
            log.info("Successfully updated 'fitbit_refresh_token' Variable.")
        else:
            log.info("Fitbit refresh token is unchanged. No update required.")

        log.info("Successfully refreshed Fitbit OAuth token.")
        return new_access_token

    except httpx.HTTPError as e:
        log.error(f"HTTP error while refreshing Fitbit token: {e}")
        raise
    except (KeyError, ValueError) as e:
        log.error(f"Error processing Fitbit token data: {e}")
        raise
    except Exception as e:
        log.error(f"An unexpected error occurred: {e}")
        raise


@task()
def fetch_fitbit_weight_data(access_token, ds=None):
    """
    Fetches weight data for a given date from the Fitbit API using
    an OAuth2 access token.
    """
    user_id = Variable.get("fitbit_user_id", default="-")
    api_url = f"https://api.fitbit.com/1/user/{user_id}/body/log/weight/date/{ds}.json"

    headers = {"Authorization": f"Bearer {access_token}"}

    with httpx.Client(timeout=30.0) as client:
        response = client.get(api_url, headers=headers)
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
    Writes the fetched weight data to InfluxDB using the InfluxDBHook.
    Skips if no data is provided.
    """
    if not weight_data:
        log.info("No weight data to write to InfluxDB. Skipping.")
        return

    try:
        # The InfluxDBHook handles the connection details (URL, token, org).
        # We just need to retrieve the target bucket from the connection's extra field.
        conn = BaseHook.get_connection(INFLUXDB_CONN_ID)
        bucket = conn.extra_dejson.get("bucket")
        if not bucket:
            raise ValueError(
                f"The 'bucket' must be specified in the 'extra' field of the "
                f"'{INFLUXDB_CONN_ID}' connection."
            )

        log.info(f"Preparing to write data to InfluxDB bucket: '{bucket}'")

        # Instantiate the hook
        influxdb_hook = InfluxDBHook(conn_id=INFLUXDB_CONN_ID)

        # Prepare data points
        points_to_write = []
        for record in weight_data:
            point = {
                "measurement": "body_metrics",
                "tags": {"source": record.get("source", "Unknown")},
                "fields": {
                    "weight_kg": float(record["weight"]),
                    "bmi": float(record["bmi"]),
                },
                "time": f"{record['date']}T{record['time']}Z",
            }
            points_to_write.append(point)

        # Use the hook to write the points
        influxdb_hook.get_conn().write_api().write(bucket=bucket, record=points_to_write)
        log.info(f"Successfully wrote {len(points_to_write)} points to InfluxDB.")

    except Exception as e:
        log.error(f"Error writing to InfluxDB: {e}")
        raise


@dag(
    dag_id="fitbit_to_influxdb_weight",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    schedule="@daily",
    catchup=False,
    doc_md="""
    ### Fitbit to InfluxDB Weight Data Pipeline

    This DAG fetches daily weight data from the Fitbit API and writes it to an
    InfluxDB bucket using the official Airflow InfluxDB provider.

    **Required Setup:**
    1.  **Airflow InfluxDB Connection:**
        -   `Conn Id`: `influxdb_default`
        -   `Conn Type`: `InfluxDB`
        -   `Host`: Your InfluxDB URL (e.g., `http://influxdb:8086`)
        -   `Password`: Your InfluxDB Token (the provider uses the password field for the token).
        -   `Login`: Your InfluxDB Organization.
        -   `Extra`: `{"bucket": "your_bucket_name"}` (must contain the target bucket).

    2.  **Airflow Connection for Fitbit:**
        -   `Conn Id`: `fitbit_default`
        -   `Conn Type`: `HTTP`
        -   `Login`: Your Fitbit App's Client ID.
        -   `Password`: Your Fitbit App's Client Secret.

    3.  **Airflow Variables for Fitbit:**
        -   `fitbit_refresh_token`: Your OAuth2 refresh token for the Fitbit API.
        -   `fitbit_user_id` (optional): Defaults to `-` for the authenticated user.
    """,
    tags=["fitbit", "influxdb", "api", "health"],
)
def fitbit_to_influxdb_dag():
    """
    DAG to fetch weight data from Fitbit and load it into InfluxDB.
    """
    # Define the DAG's task flow
    access_token = get_fitbit_oauth_token()
    weight_data_from_api = fetch_fitbit_weight_data(access_token)
    write_data_to_influxdb(weight_data_from_api)


# Instantiate the DAG
fitbit_to_influxdb_dag_instance = fitbit_to_influxdb_dag()

if __name__ == "__main__":
    fitbit_to_influxdb_dag_instance.test()
