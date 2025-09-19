"""Pytest configuration and fixtures for aio-sf tests."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from aio_sf import SalesforceClient
from aio_sf.api.auth import AuthStrategy


class MockAuthStrategy(AuthStrategy):
    """Mock authentication strategy for testing."""

    def __init__(self):
        super().__init__("https://test.my.salesforce.com")
        self.access_token = "mock_access_token_123"
        self.expires_at = None

    async def authenticate(self, http_client):
        return self.access_token

    async def refresh_if_needed(self, http_client):
        return self.access_token

    def can_refresh(self):
        return True


@pytest.fixture
def mock_auth_strategy():
    """Provide a mock authentication strategy."""
    return MockAuthStrategy()


@pytest.fixture
def mock_http_response():
    """Create a mock HTTP response."""

    def _create_response(json_data=None, status_code=200, headers=None):
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.headers = headers or {}

        # Make json() a callable that returns the data directly
        def json_method():
            return json_data or {}

        mock_response.json = json_method

        # Make raise_for_status() a callable that does nothing
        def raise_for_status_method():
            pass

        mock_response.raise_for_status = raise_for_status_method

        return mock_response

    return _create_response


@pytest.fixture
def mock_client(mock_auth_strategy, monkeypatch):
    """Provide a SalesforceClient with mocked HTTP calls."""
    client = SalesforceClient(auth_strategy=mock_auth_strategy)

    # Create a mock HTTP client with proper async mocking
    mock_http_client = AsyncMock()

    # Mock the http_client property to return our mock
    monkeypatch.setattr(client, "_http_client", mock_http_client)

    # Also patch the property getter to always return our mock
    def mock_http_client_property(self):
        return mock_http_client

    monkeypatch.setattr(
        SalesforceClient, "http_client", property(mock_http_client_property)
    )

    return client, mock_http_client
