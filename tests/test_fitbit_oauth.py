import base64

import pytest
import requests

# The .function attribute unwraps the TaskFlow decorator for direct testing
from dags.fitbit_to_influxdb import get_fitbit_oauth_token

# --- Constants for Mocking ---
MOCK_CLIENT_ID = "test_client_id"
MOCK_CLIENT_SECRET = "test_client_secret"
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


def test_get_fitbit_oauth_token_success(mocker, mock_fitbit_connection):
    """
    Tests the successful fetching of a Fitbit OAuth token using the
    client credentials grant type.
    """
    # Mock the response from the requests.post call
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": MOCK_ACCESS_TOKEN,
        "token_type": "Bearer",
        "expires_in": 3600,
        "scope": "weight",
    }
    # Patch the requests.post method to return our mock response
    mock_post = mocker.patch("requests.post", return_value=mock_response)

    # Execute the actual task function
    result = get_fitbit_oauth_token.function()

    # --- Assertions ---
    # 1. Assert the function returns the expected access token
    assert result == MOCK_ACCESS_TOKEN

    # 2. Verify that requests.post was called correctly
    mock_post.assert_called_once()
    call_args, call_kwargs = mock_post.call_args
    assert call_args[0] == "https://api.fitbit.com/oauth2/token"

    # 3. Check the payload sent in the request
    expected_payload = {"grant_type": "client_credentials"}
    assert call_kwargs["data"] == expected_payload

    # 4. Check that the correct Basic Auth headers were constructed and sent
    auth_string = f"{MOCK_CLIENT_ID}:{MOCK_CLIENT_SECRET}"
    expected_b64_string = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")
    expected_headers = {
        "Authorization": f"Basic {expected_b64_string}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    assert call_kwargs["headers"] == expected_headers


def test_get_fitbit_oauth_token_http_error(mocker, mock_fitbit_connection):
    """
    Tests that the task properly raises an HTTPError if the Fitbit API
    returns an error status code.
    """
    # Configure the mock to simulate a failed API call by raising an exception
    mocker.patch(
        "requests.post",
        side_effect=requests.exceptions.HTTPError("401 Client Error: Unauthorized"),
    )

    # Use pytest.raises to assert that our function correctly bubbles up the exception
    with pytest.raises(requests.exceptions.HTTPError):
        get_fitbit_oauth_token.function()


def test_get_fitbit_oauth_token_missing_credentials(mocker):
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
    with pytest.raises(ValueError, match="missing required fields"):
        get_fitbit_oauth_token.function()
