"""Unit tests for authentication strategies."""

import pytest
from unittest.mock import AsyncMock, patch
from aio_sf.api.auth import (
    ClientCredentialsAuth,
    RefreshTokenAuth,
    StaticTokenAuth,
    SfdxCliAuth,
    SalesforceAuthError,
)


class TestClientCredentialsAuth:
    """Test ClientCredentialsAuth strategy."""

    def test_initialization(self):
        """Test auth strategy initializes correctly."""
        auth = ClientCredentialsAuth(
            instance_url="https://test.my.salesforce.com",
            client_id="test_client_id",
            client_secret="test_secret",
        )

        assert auth.instance_url == "https://test.my.salesforce.com"
        assert auth.client_id == "test_client_id"
        assert auth.client_secret == "test_secret"
        assert auth.can_refresh() is True

    @pytest.mark.asyncio
    async def test_successful_authentication(self, mock_http_response):
        """Test successful authentication flow."""
        auth = ClientCredentialsAuth(
            instance_url="https://test.my.salesforce.com",
            client_id="test_client_id",
            client_secret="test_secret",
        )

        # Mock HTTP client
        mock_client = AsyncMock()

        # Mock token response
        token_response = mock_http_response(
            {
                "access_token": "mock_token_123",
                "instance_url": "https://test.my.salesforce.com",
                "token_type": "Bearer",
            }
        )

        # Mock introspect response
        introspect_response = mock_http_response({"active": True, "exp": 1234567890})

        mock_client.post.side_effect = [token_response, introspect_response]

        token = await auth.authenticate(mock_client)

        assert token == "mock_token_123"
        assert auth.access_token == "mock_token_123"
        assert mock_client.post.call_count == 2  # Token + introspect calls


class TestStaticTokenAuth:
    """Test StaticTokenAuth strategy."""

    def test_initialization(self):
        """Test static token auth initializes correctly."""
        auth = StaticTokenAuth(
            instance_url="https://test.my.salesforce.com",
            access_token="static_token_123",
        )

        assert auth.instance_url == "https://test.my.salesforce.com"
        assert auth.access_token == "static_token_123"
        assert auth.can_refresh() is False

    @pytest.mark.asyncio
    async def test_authenticate_returns_token(self):
        """Test authenticate returns the static token."""
        auth = StaticTokenAuth(
            instance_url="https://test.my.salesforce.com",
            access_token="static_token_123",
        )

        mock_client = AsyncMock()
        token = await auth.authenticate(mock_client)

        assert token == "static_token_123"
        # Should not make any HTTP calls
        mock_client.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_refresh_if_needed_with_expired_token(self):
        """Test refresh fails when token is expired (no refresh capability)."""
        auth = StaticTokenAuth(
            instance_url="https://test.my.salesforce.com",
            access_token="static_token_123",
        )

        # Simulate expired token
        auth.expires_at = 1000000000  # Past timestamp

        mock_client = AsyncMock()

        with pytest.raises(SalesforceAuthError, match="Access token has expired"):
            await auth.refresh_if_needed(mock_client)


class TestSfdxCliAuth:
    """Test SfdxCliAuth strategy."""

    def test_initialization(self):
        """Test SFDX CLI auth initializes correctly."""
        auth = SfdxCliAuth("test-org-alias")

        assert auth.username_or_alias == "test-org-alias"
        assert auth.instance_url is None  # Set during authentication
        assert auth.can_refresh() is True

    @pytest.mark.asyncio
    async def test_successful_authentication(self):
        """Test successful SFDX CLI authentication."""
        auth = SfdxCliAuth("test-org-alias")

        # Mock subprocess response
        mock_stdout = """
        {
            "status": 0,
            "result": {
                "accessToken": "sfdx_token_123",
                "instanceUrl": "https://test.my.salesforce.com",
                "username": "test@example.com"
            }
        }
        """

        mock_client = AsyncMock()

        with patch("asyncio.create_subprocess_shell") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 0
            mock_process.communicate.return_value = (mock_stdout.encode(), b"")
            mock_subprocess.return_value = mock_process

            token = await auth.authenticate(mock_client)

            assert token == "sfdx_token_123"
            assert auth.access_token == "sfdx_token_123"
            assert auth.instance_url == "https://test.my.salesforce.com"

    @pytest.mark.asyncio
    async def test_failed_sfdx_command(self):
        """Test handling of failed SFDX command."""
        auth = SfdxCliAuth("test-org-alias")
        mock_client = AsyncMock()

        with patch("asyncio.create_subprocess_shell") as mock_subprocess:
            mock_process = AsyncMock()
            mock_process.returncode = 1
            mock_process.communicate.return_value = (b"", b"Error: Org not found")
            mock_subprocess.return_value = mock_process

            with pytest.raises(SalesforceAuthError, match="SFDX command failed"):
                await auth.authenticate(mock_client)

    @pytest.mark.asyncio
    async def test_sfdx_not_installed(self):
        """Test handling when SFDX CLI is not installed."""
        auth = SfdxCliAuth("test-org-alias")
        mock_client = AsyncMock()

        with patch("asyncio.create_subprocess_shell", side_effect=FileNotFoundError):
            with pytest.raises(SalesforceAuthError, match="SFDX CLI not found"):
                await auth.authenticate(mock_client)
