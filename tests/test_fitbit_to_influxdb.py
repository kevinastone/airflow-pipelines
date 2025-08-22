import pytest

# Since we are testing the functions directly, we need to import them.
# The .function attribute unwraps the TaskFlow decorator.
from dags.fitbit_to_influxdb import (
    fetch_fitbit_weight_data,
    write_data_to_influxdb,
)


@pytest.fixture
def mock_fitbit_user_id_variable(mocker):
    """Mock the 'fitbit_user_id' Airflow Variable."""
    mocker.patch("airflow.sdk.Variable.get", return_value="-")


def test_fetch_fitbit_weight_data_success(mocker, mock_fitbit_user_id_variable):
    """Test the Fitbit fetch task with a successful API response."""
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "weight": [
            {
                "bmi": 25.0,
                "date": "2023-01-15",
                "logId": 12345,
                "time": "10:30:00",
                "weight": 80.5,
                "source": "Aria",
            }
        ]
    }
    mocker.patch("requests.get", return_value=mock_response)

    # We call the unwrapped Python function for testing, passing a mock access token
    result = fetch_fitbit_weight_data.function(access_token="mock_token", ds="2023-01-15")

    assert result is not None
    assert len(result) == 1
    assert result[0]["weight"] == 80.5
    assert result[0]["date"] == "2023-01-15"


def test_fetch_fitbit_weight_data_no_data(mocker, mock_fitbit_user_id_variable):
    """Test the Fitbit fetch task when the API returns no weight logs."""
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"weight": []}  # No data
    mocker.patch("requests.get", return_value=mock_response)

    result = fetch_fitbit_weight_data.function(access_token="mock_token", ds="2023-01-15")

    assert result is None


def test_write_data_to_influxdb_skips_on_no_data(mocker):
    """Test that the InfluxDB write task skips if input data is None or empty."""
    # Patch the final `write` method and assert it's never called.
    mock_write = mocker.patch("influxdb_client.client.write_api.WriteApi.write")

    # Test with None
    write_data_to_influxdb.function(weight_data=None)
    mock_write.assert_not_called()

    # Test with an empty list
    write_data_to_influxdb.function(weight_data=[])
    mock_write.assert_not_called()


def test_write_data_to_influxdb_success(mocker):
    """Test the InfluxDB write task with valid data using the hook."""
    # Mock the Airflow connection that the hook will use
    mock_conn = mocker.Mock()
    mock_conn.host = "http://mock-influxdb:8086"
    mock_conn.password = "mock_token"  # Corresponds to token
    mock_conn.login = "mock_org"  # Corresponds to org
    mock_conn.extra_dejson = {"bucket": "mock_bucket"}
    mocker.patch("airflow.hooks.base.BaseHook.get_connection", return_value=mock_conn)

    # Patch the final `write` method in the call chain to prevent any real network calls.
    mock_write = mocker.patch("influxdb_client.client.write_api.WriteApi.write")

    # Sample data from the previous task
    sample_data = [
        {
            "bmi": 25.0,
            "date": "2023-01-15",
            "time": "10:30:00",
            "weight": 80.5,
            "source": "Aria",
        }
    ]

    # Execute the task function
    write_data_to_influxdb.function(weight_data=sample_data)

    # Assert that our mock `write` method was called once.
    mock_write.assert_called_once()

    # Check the arguments passed to the `write` method.
    # The `org` parameter is now handled by the hook when creating the client,
    # so it's not passed directly to the final `write` call.
    args, kwargs = mock_write.call_args
    assert kwargs["bucket"] == "mock_bucket"
    assert "org" not in kwargs  # Verify 'org' is not passed here

    # Check the actual point data (it's a dictionary now)
    written_record = kwargs["record"][0]
    assert written_record["measurement"] == "body_metrics"
    assert written_record["tags"]["source"] == "Aria"
    assert written_record["fields"]["weight_kg"] == 80.5
    assert written_record["fields"]["bmi"] == 25.0
    assert written_record["time"] == "2023-01-15T10:30:00Z"
