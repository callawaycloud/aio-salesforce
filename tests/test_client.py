"""Unit tests for SalesforceClient."""

import pytest
from aio_sf import SalesforceClient
from aio_sf.api.auth import SalesforceAuthError


class TestSalesforceClient:
    """Test the main SalesforceClient class."""

    def test_client_initialization(self, mock_auth_strategy):
        """Test client initializes correctly."""
        client = SalesforceClient(auth_strategy=mock_auth_strategy)

        assert client.auth_strategy == mock_auth_strategy
        assert client.version == "v60.0"
        assert client.timeout == 30.0
        assert client.instance_url == "https://test.my.salesforce.com"
        assert client.access_token == "mock_access_token_123"

    def test_client_with_custom_settings(self, mock_auth_strategy):
        """Test client with custom version and timeout."""
        client = SalesforceClient(
            auth_strategy=mock_auth_strategy, version="v59.0", timeout=60.0
        )

        assert client.version == "v59.0"
        assert client.timeout == 60.0

    @pytest.mark.asyncio
    async def test_ensure_authenticated(self, mock_client):
        """Test authentication flow."""
        client, mock_http_client = mock_client

        token = await client.ensure_authenticated()

        assert token == "mock_access_token_123"

    def test_headers_property(self, mock_auth_strategy):
        """Test headers property returns correct format."""
        client = SalesforceClient(auth_strategy=mock_auth_strategy)

        headers = client.headers

        assert headers["Authorization"] == "Bearer mock_access_token_123"
        assert headers["Content-Type"] == "application/json"
        assert headers["Accept"] == "application/json"

    def test_get_base_url(self, mock_auth_strategy):
        """Test base URL generation."""
        client = SalesforceClient(auth_strategy=mock_auth_strategy)

        base_url = client.get_base_url()
        assert base_url == "https://test.my.salesforce.com/services/data/v60.0"

        # Test with custom version
        base_url = client.get_base_url("v59.0")
        assert base_url == "https://test.my.salesforce.com/services/data/v59.0"

    def test_get_sobject_url(self, mock_auth_strategy):
        """Test SObject URL generation."""
        client = SalesforceClient(auth_strategy=mock_auth_strategy)

        url = client.get_sobject_url("Account")
        assert (
            url == "https://test.my.salesforce.com/services/data/v60.0/sobjects/Account"
        )

    def test_get_describe_url(self, mock_auth_strategy):
        """Test describe URL generation."""
        client = SalesforceClient(auth_strategy=mock_auth_strategy)

        url = client.get_describe_url("Account")
        assert (
            url
            == "https://test.my.salesforce.com/services/data/v60.0/sobjects/Account/describe"
        )

    def test_api_property_lazy_loading(self, mock_auth_strategy):
        """Test that API properties are lazily loaded."""
        client = SalesforceClient(auth_strategy=mock_auth_strategy)

        # Properties should be None initially
        assert client._describe_api is None
        assert client._query_api is None
        assert client._collections_api is None
        assert client._bulk_v2_api is None

        # Accessing properties should create instances
        describe_api = client.describe
        query_api = client.query
        collections_api = client.collections
        bulk_v2_api = client.bulk_v2

        assert describe_api is not None
        assert query_api is not None
        assert collections_api is not None
        assert bulk_v2_api is not None

        # Should return same instances on subsequent calls
        assert client.describe is describe_api
        assert client.query is query_api
        assert client.collections is collections_api
        assert client.bulk_v2 is bulk_v2_api

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_auth_strategy):
        """Test client works as async context manager."""
        async with SalesforceClient(auth_strategy=mock_auth_strategy) as client:
            assert client.access_token == "mock_access_token_123"

        # Client should be closed after context
        assert client._http_client is None or client._http_client.is_closed
