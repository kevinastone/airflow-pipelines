import base64

import httpx
import pytest

# The .function attribute unwraps the TaskFlow decorator for direct testing
from dags.fitbit_to_influxdb import get_fitbit_oauth_token

# --- Constants for Mocking ---
MOCK_CLIENT_ID = "test_client_id"
MOCK_CLIENT_SECRET = "test_client_secret"
MOCK_REFRESH_TOKEN = "mock_refresh_token_from_test"
MOCK_NEW_REFRESH_TOKEN = "mock_new_refresh_token"
MOCK_ACCESS_TOKEN = "mock_access_token_from_test"


@pytest.fixture
def mock_fitbit_connection(mocker):
    """
    Mocks the Airflow BaseHook to return a valid Fitbit connection object.
    """
    mock_conn = mocker.Mock()
    mock_conn.login = MOCK_CLIENT_ID
    mock_conn.password = MOCK_CLIENT_SECRET

    mocker.patch("airflow.hooks.base.BaseHook.get_connection", return_value=mock_conn)
    return mock_conn


@pytest.fixture
def mock_airflow_variables(mocker):
    """Mocks Airflow Variable.get and Variable.set."""
    mock_variable_get = mocker.patch(
        "dags.fitbit_to_influxdb.Variable.get", return_value=MOCK_REFRESH_TOKEN
    )
    mock_variable_set = mocker.patch("dags.fitbit_to_influxdb.Variable.set")
    return mock_variable_get, mock_variable_set


def test_get_fitbit_oauth_token_success(mocker, mock_fitbit_connection, mock_airflow_variables):
    """
    Tests the successful fetching of a Fitbit OAuth token and ensures the
    refresh token variable is NOT updated if the API doesn't return a new one.
    """
    # Mock the response from the httpx client
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": MOCK_ACCESS_TOKEN,
        "token_type": "Bearer",
        "expires_in": 3600,
        "scope": "weight",
    }
    # The DAG uses `with httpx.Client(...) as client:`, so we need to mock the
    # client's context manager and the `post` call.
    mock_client = mocker.patch("httpx.Client").return_value.__enter__.return_value
    mock_client.post.return_value = mock_response

    _, mock_variable_set = mock_airflow_variables

    # Execute the actual task function
    result = get_fitbit_oauth_token.function()

    # --- Assertions ---
    # 1. Assert the function returns the expected access token
    assert result == MOCK_ACCESS_TOKEN

    # 2. Verify that httpx.Client.post was called correctly
    mock_client.post.assert_called_once()
    call_args, call_kwargs = mock_client.post.call_args
    assert call_args[0] == "https://api.fitbit.com/oauth2/token"

    # 3. Check the payload sent in the request
    expected_payload = {
        "grant_type": "refresh_token",
        "refresh_token": MOCK_REFRESH_TOKEN,
    }
    assert call_kwargs["data"] == expected_payload

    # 4. Check that the correct Basic Auth headers were constructed and sent
    auth_string = f"{MOCK_CLIENT_ID}:{MOCK_CLIENT_SECRET}"
    expected_b64_string = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")
    expected_headers = {
        "Authorization": f"Basic {expected_b64_string}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    assert call_kwargs["headers"] == expected_headers

    # 5. Since the mock response didn't include a new refresh token,
    #    Variable.set should not have been called.
    mock_variable_set.assert_not_called()


def test_get_fitbit_oauth_token_http_error(mocker, mock_fitbit_connection, mock_airflow_variables):
    """
    Tests that the task properly raises an HTTPError if the Fitbit API
    returns an error status code.
    """
    # Configure the mock to simulate a failed API call by raising an exception
    # Patch the client and configure the post method to raise an HTTPError
    mock_client = mocker.patch("httpx.Client").return_value.__enter__.return_value
    mock_client.post.side_effect = httpx.HTTPError("401 Client Error: Unauthorized")

    # Use pytest.raises to assert that our function correctly bubbles up the exception
    with pytest.raises(httpx.HTTPError):
        get_fitbit_oauth_token.function()


def test_get_fitbit_oauth_token_missing_credentials(mocker, mock_airflow_variables):
    """
    Tests that the task raises a descriptive ValueError if the Airflow
    connection is missing required fields (e.g., password).
    """
    # Mock a connection that is missing the password (client_secret)
    mock_conn = mocker.Mock()
    mock_conn.login = MOCK_CLIENT_ID
    mock_conn.password = None  # Simulate missing secret
    mocker.patch("airflow.hooks.base.BaseHook.get_connection", return_value=mock_conn)

    # Assert that a ValueError is raised and check that the error message is helpful
    with pytest.raises(ValueError, match="Fitbit connection is missing"):
        get_fitbit_oauth_token.function()


def test_get_fitbit_oauth_token_missing_refresh_token(mocker, mock_fitbit_connection):
    """
    Tests that a ValueError is raised if the `fitbit_refresh_token`
    Airflow Variable is missing.
    """
    # Mock Variable.get to return None, as if the variable is not set.
    mocker.patch("dags.fitbit_to_influxdb.Variable.get", return_value=None)

    with pytest.raises(ValueError, match="Variable is not set"):
        get_fitbit_oauth_token.function()


def test_get_fitbit_oauth_token_updates_variable_on_new_refresh_token(
    mocker, mock_fitbit_connection, mock_airflow_variables
):
    """
    Tests that the Airflow Variable is updated if the Fitbit API returns
    a new refresh token.
    """
    # --- Mocks ---
    mock_variable_get, mock_variable_set = mock_airflow_variables

    # Mock the API response to include a new refresh token
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": MOCK_ACCESS_TOKEN,
        "refresh_token": MOCK_NEW_REFRESH_TOKEN,
        "token_type": "Bearer",
    }
    mock_client = mocker.patch("httpx.Client").return_value.__enter__.return_value
    mock_client.post.return_value = mock_response

    # --- Execute ---
    get_fitbit_oauth_token.function()

    # --- Assertions ---
    # 1. Verify that Variable.get was called to fetch the old token
    mock_variable_get.assert_called_once_with("fitbit_refresh_token", default=None)

    # 2. Verify that Variable.set was called to store the new token
    mock_variable_set.assert_called_once_with("fitbit_refresh_token", MOCK_NEW_REFRESH_TOKEN)
