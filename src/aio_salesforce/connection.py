"""
Salesforce connection module providing authentication and basic API functionality.
"""

import base64
import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
from urllib.parse import urljoin

import httpx

logger = logging.getLogger(__name__)


class SalesforceAuthError(Exception):
    """Raised when authentication fails or tokens are invalid."""

    pass


class AuthStrategy(ABC):
    """Abstract base class for Salesforce authentication strategies."""

    def __init__(self, instance_url: str):
        self.instance_url = instance_url.rstrip("/") if instance_url else None
        self.access_token: Optional[str] = None
        self.expires_at: Optional[int] = None

    @abstractmethod
    async def authenticate(self, http_client: httpx.AsyncClient) -> str:
        """Authenticate and return access token."""
        pass

    @abstractmethod
    async def refresh_if_needed(self, http_client: httpx.AsyncClient) -> str:
        """Refresh token if needed and return access token."""
        pass

    @abstractmethod
    def can_refresh(self) -> bool:
        """Return True if this strategy can refresh expired tokens."""
        pass

    def is_token_expired(self) -> bool:
        """Check if the current token is expired."""
        if not self.expires_at:
            return False
        return self.expires_at <= int(time.time())


class ClientCredentialsAuth(AuthStrategy):
    """OAuth Client Credentials authentication strategy."""

    def __init__(self, instance_url: str, client_id: str, client_secret: str):
        super().__init__(instance_url)
        self.client_id = client_id
        self.client_secret = client_secret

    async def authenticate(self, http_client: httpx.AsyncClient) -> str:
        """Authenticate using OAuth client credentials flow."""
        logger.info("Getting Salesforce access token using client credentials")

        oauth_url = urljoin(self.instance_url, "/services/oauth2/token")
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        oauth_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

        try:
            response = await http_client.post(
                oauth_url, data=data, headers=oauth_headers
            )
            response.raise_for_status()
            token_data = response.json()
            self.access_token = token_data["access_token"]

            # Get token expiration information
            await self._get_token_expiration(http_client)

            logger.info("Successfully obtained Salesforce access token")
            return self.access_token

        except httpx.HTTPError as e:
            logger.error(f"HTTP error getting access token: {e}")
            raise SalesforceAuthError(f"Authentication failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error getting access token: {e}")
            raise SalesforceAuthError(f"Authentication failed: {e}")

    async def refresh_if_needed(self, http_client: httpx.AsyncClient) -> str:
        """Refresh token if needed (always re-authenticate for client credentials)."""
        if self.access_token and not self.is_token_expired():
            return self.access_token
        return await self.authenticate(http_client)

    def can_refresh(self) -> bool:
        """Client credentials can always re-authenticate."""
        return True

    async def _get_token_expiration(self, http_client: httpx.AsyncClient) -> None:
        """Get token expiration time via introspection."""
        introspect_url = urljoin(self.instance_url, "/services/oauth2/introspect")
        introspect_data = {
            "token": self.access_token,
            "token_type_hint": "access_token",
        }

        auth_string = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode("utf-8")
        ).decode("utf-8")

        introspect_response = await http_client.post(
            introspect_url,
            data=introspect_data,
            headers={
                "Authorization": f"Basic {auth_string}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
        )
        introspect_response.raise_for_status()
        introspect_data = introspect_response.json()

        # Set expiration time with 30 second buffer
        self.expires_at = introspect_data["exp"] - 30


class RefreshTokenAuth(AuthStrategy):
    """OAuth Refresh Token authentication strategy."""

    def __init__(
        self,
        instance_url: str,
        access_token: str,
        refresh_token: str,
        client_id: str,
        client_secret: str,
    ):
        super().__init__(instance_url)
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret

    async def authenticate(self, http_client: httpx.AsyncClient) -> str:
        """Use the provided access token (refresh if needed)."""
        if self.access_token and not self.is_token_expired():
            return self.access_token
        return await self._refresh_token(http_client)

    async def refresh_if_needed(self, http_client: httpx.AsyncClient) -> str:
        """Refresh token if needed."""
        if self.access_token and not self.is_token_expired():
            return self.access_token
        return await self._refresh_token(http_client)

    def can_refresh(self) -> bool:
        """Refresh token auth can refresh tokens."""
        return bool(self.refresh_token)

    async def _refresh_token(self, http_client: httpx.AsyncClient) -> str:
        """Refresh the access token using the refresh token."""
        logger.info("Refreshing Salesforce access token using refresh token")

        oauth_url = urljoin(self.instance_url, "/services/oauth2/token")
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        oauth_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

        try:
            response = await http_client.post(
                oauth_url, data=data, headers=oauth_headers
            )
            response.raise_for_status()
            token_data = response.json()
            self.access_token = token_data["access_token"]

            # Update refresh token if a new one is provided
            if "refresh_token" in token_data:
                self.refresh_token = token_data["refresh_token"]

            # Get token expiration information
            await self._get_token_expiration(http_client)

            logger.info("Successfully refreshed Salesforce access token")
            return self.access_token

        except httpx.HTTPError as e:
            logger.error(f"HTTP error refreshing access token: {e}")
            raise SalesforceAuthError(f"Token refresh failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error refreshing access token: {e}")
            raise SalesforceAuthError(f"Token refresh failed: {e}")

    async def _get_token_expiration(self, http_client: httpx.AsyncClient) -> None:
        """Get token expiration time via introspection."""
        introspect_url = urljoin(self.instance_url, "/services/oauth2/introspect")
        introspect_data = {
            "token": self.access_token,
            "token_type_hint": "access_token",
        }

        auth_string = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode("utf-8")
        ).decode("utf-8")

        introspect_response = await http_client.post(
            introspect_url,
            data=introspect_data,
            headers={
                "Authorization": f"Basic {auth_string}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
        )
        introspect_response.raise_for_status()
        introspect_data = introspect_response.json()

        # Set expiration time with 30 second buffer
        self.expires_at = introspect_data["exp"] - 30


class StaticTokenAuth(AuthStrategy):
    """Static access token authentication strategy (no refresh capability)."""

    def __init__(self, instance_url: str, access_token: str):
        super().__init__(instance_url)
        self.access_token = access_token

    async def authenticate(self, http_client: httpx.AsyncClient) -> str:
        """Return the static access token."""
        if not self.access_token:
            raise SalesforceAuthError("No access token available")
        return self.access_token

    async def refresh_if_needed(self, http_client: httpx.AsyncClient) -> str:
        """Cannot refresh static tokens."""
        if self.is_token_expired():
            raise SalesforceAuthError(
                "Access token has expired and no refresh capability is available."
            )
        return await self.authenticate(http_client)

    def can_refresh(self) -> bool:
        """Static tokens cannot be refreshed."""
        return False


class SalesforceConnection:
    """
    A connection object containing Salesforce authentication details and basic API functionality.

    This provides a simple interface for Salesforce API interactions using explicit
    authentication strategies.
    """

    def __init__(
        self,
        auth_strategy: AuthStrategy,
        version: str = "v60.0",
        timeout: float = 30.0,
    ):
        """
        Initialize Salesforce connection with an explicit authentication strategy.

        :param auth_strategy: Authentication strategy to use (ClientCredentialsAuth, RefreshTokenAuth, or StaticTokenAuth)
        :param version: API version (e.g., "v60.0")
        :param timeout: HTTP request timeout in seconds
        """
        self.auth_strategy = auth_strategy
        self.version = version
        self.timeout = timeout

        # Persistent HTTP client for better connection management
        self._http_client: Optional[httpx.AsyncClient] = None

        # Extract instance from URL for compatibility
        if self.auth_strategy.instance_url:
            if "://" in self.auth_strategy.instance_url:
                self.instance = self.auth_strategy.instance_url.split("://")[1].split(
                    "/"
                )[0]
            else:
                self.instance = self.auth_strategy.instance_url.split("/")[0]
        else:
            self.instance = None

        # Initialize API modules
        self._describe_api = None
        self._bulk_v2_api = None
        self._query_api = None

    @property
    def instance_url(self) -> Optional[str]:
        """Get the instance URL from the auth strategy."""
        return self.auth_strategy.instance_url

    @property
    def access_token(self) -> Optional[str]:
        """Get the current access token from the auth strategy."""
        return self.auth_strategy.access_token

    @property
    def describe(self):
        """Access to Salesforce Describe API methods."""
        if self._describe_api is None:
            from .api.describe import DescribeAPI

            self._describe_api = DescribeAPI(self)
        return self._describe_api

    @property
    def bulk_v2(self):
        """Access to Salesforce Bulk API v2 methods."""
        if self._bulk_v2_api is None:
            from .api.bulk_v2 import BulkV2API

            self._bulk_v2_api = BulkV2API(self)
        return self._bulk_v2_api

    @property
    def query(self):
        """Access to Salesforce Query API methods."""
        if self._query_api is None:
            from .api.query import QueryAPI

            self._query_api = QueryAPI(self)
        return self._query_api

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=self.timeout)
        return self._http_client

    async def close(self):
        """Close the HTTP client and clean up resources."""
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
            self._http_client = None

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def ensure_authenticated(self) -> str:
        """
        Ensure the connection has a valid access token using the configured auth strategy.

        :returns: Valid access token
        :raises: SalesforceAuthError if authentication/refresh fails
        """
        return await self.auth_strategy.refresh_if_needed(self.http_client)

    @property
    def headers(self) -> Dict[str, str]:
        """Get the standard headers for API requests."""
        if not self.access_token:
            raise ValueError(
                "No access token available. Call ensure_authenticated() first."
            )
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def get_authenticated_headers(self) -> Dict[str, str]:
        """
        Get headers with a valid access token, ensuring authentication first.

        :returns: Headers dictionary with valid Bearer token
        """
        await self.ensure_authenticated()
        return self.headers

    def get_base_url(self, api_version: Optional[str] = None) -> str:
        """
        Get the base URL for API requests.

        :param api_version: API version to use (defaults to connection version)
        :returns: Base URL for Salesforce API
        """
        if not self.instance_url:
            raise ValueError("instance_url is required to build API URLs")
        effective_version = api_version or self.version
        return f"{self.instance_url}/services/data/{effective_version}"

    def get_sobject_url(
        self, sobject_type: str, api_version: Optional[str] = None
    ) -> str:
        """
        Get the URL for sobject operations.

        :param sobject_type: Salesforce object type (e.g., 'Account', 'Contact')
        :param api_version: API version to use (defaults to connection version)
        :returns: URL for sobject operations
        """
        base_url = self.get_base_url(api_version)
        return f"{base_url}/sobjects/{sobject_type}"

    def get_describe_url(
        self, sobject_type: str, api_version: Optional[str] = None
    ) -> str:
        """
        Get the URL for describing a Salesforce object.

        :param sobject_type: Salesforce object type (e.g., 'Account', 'Contact')
        :param api_version: API version to use (defaults to connection version)
        :returns: URL for describe operation
        """
        sobject_url = self.get_sobject_url(sobject_type, api_version)
        return f"{sobject_url}/describe"

    async def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        auto_auth: bool = True,
        **kwargs,
    ) -> httpx.Response:
        """
        Make an authenticated HTTP request through the connection.

        :param method: HTTP method (GET, POST, etc.)
        :param url: Full URL to request
        :param headers: Additional headers (will be merged with auth headers)
        :param auto_auth: Whether to automatically ensure authentication
        :param kwargs: Additional arguments passed to httpx request
        :returns: HTTP response
        :raises: SalesforceAuthError if authentication fails
        """
        if auto_auth:
            await self.ensure_authenticated()

        # Merge auth headers with any additional headers
        request_headers = self.headers.copy()
        if headers:
            request_headers.update(headers)

        client = self.http_client
        response = await client.request(method, url, headers=request_headers, **kwargs)

        # If we get a 401, try to re-authenticate once and retry
        if response.status_code == 401 and auto_auth:
            logger.info("Got 401 response, attempting to re-authenticate")
            try:
                # Force re-authentication by clearing current token in the strategy
                old_token = self.auth_strategy.access_token
                self.auth_strategy.access_token = None
                self.auth_strategy.expires_at = None

                await self.ensure_authenticated()

                # Retry the request with new token
                request_headers = self.headers.copy()
                if headers:
                    request_headers.update(headers)

                response = await client.request(
                    method, url, headers=request_headers, **kwargs
                )

            except Exception as e:
                logger.error(f"Re-authentication failed: {e}")
                # If re-auth fails and we had an old token, restore it
                if old_token:
                    self.auth_strategy.access_token = old_token
                raise SalesforceAuthError(
                    f"Authentication failed after 401 response: {e}"
                )

        return response

    async def get(self, url: str, **kwargs) -> httpx.Response:
        """Make an authenticated GET request."""
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs) -> httpx.Response:
        """Make an authenticated POST request."""
        return await self.request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs) -> httpx.Response:
        """Make an authenticated PUT request."""
        return await self.request("PUT", url, **kwargs)

    async def delete(self, url: str, **kwargs) -> httpx.Response:
        """Make an authenticated DELETE request."""
        return await self.request("DELETE", url, **kwargs)
