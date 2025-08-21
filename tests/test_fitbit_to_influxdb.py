import pytest

# Since we are testing the functions directly, we need to import them.
# The .function attribute unwraps the TaskFlow decorator.
from dags.fitbit_to_influxdb import (
    fetch_fitbit_weight_data,
    write_data_to_influxdb,
)


@pytest.fixture
def mock_airflow_variables(mocker):
    """Mock Airflow Variables used in the tasks."""

    def get_variable(key, default_var=None):
        if key == "fitbit_user_id":
            return "-"
        if key == "fitbit_api_bearer_token":
            return "test_bearer_token"
        return default_var

    mocker.patch("airflow.models.Variable.get", side_effect=get_variable)


def test_fetch_fitbit_weight_data_success(mocker, mock_airflow_variables):
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

    # We call the unwrapped Python function for testing
    result = fetch_fitbit_weight_data.function(ds="2023-01-15")

    assert result is not None
    assert len(result) == 1
    assert result[0]["weight"] == 80.5
    assert result[0]["date"] == "2023-01-15"


def test_fetch_fitbit_weight_data_no_data(mocker, mock_airflow_variables):
    """Test the Fitbit fetch task when the API returns no weight logs."""
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"weight": []}  # No data
    mocker.patch("requests.get", return_value=mock_response)

    result = fetch_fitbit_weight_data.function(ds="2023-01-15")

    assert result is None


def test_write_data_to_influxdb_skips_on_no_data(mocker):
    """Test that the InfluxDB write task skips if input data is None or empty."""
    # Mock the InfluxDB client so we can assert it's NOT called
    mock_client_class = mocker.patch("influxdb_client.InfluxDBClient")

    # Test with None
    write_data_to_influxdb.function(weight_data=None)
    mock_client_class.assert_not_called()

    # Test with an empty list
    write_data_to_influxdb.function(weight_data=[])
    mock_client_class.assert_not_called()


def test_write_data_to_influxdb_success(mocker):
    """Test the InfluxDB write task with valid data."""
    # Mock the Airflow connection
    mock_conn = mocker.Mock()
    mock_conn.host = "http://mock-influxdb:8086"
    mock_conn.password = "mock_token"
    mock_conn.login = "mock_org"
    mock_conn.extra_dejson = {"bucket": "mock_bucket"}
    mocker.patch("airflow.hooks.base.BaseHook.get_connection", return_value=mock_conn)

    # Patch the final `write` method in the call chain to prevent any
    # real network calls. This is the most robust way to mock this.
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

    write_data_to_influxdb.function(weight_data=sample_data)

    # Assert that our mock `write` method was called once.
    mock_write.assert_called_once()

    # Optional: More detailed check of what was written
    args, kwargs = mock_write.call_args
    assert kwargs["bucket"] == "mock_bucket"
    assert kwargs["org"] == "mock_org"

    # Check the actual point data
    written_record = kwargs["record"][0]
    assert (
        written_record.to_line_protocol()
        == "body_metrics,source=Aria bmi=25,weight_kg=80.5 1673778600000000000"
    )
