"""
Salesforce client module providing the main client interface.

This module contains the SalesforceClient class which serves as the primary
interface for interacting with Salesforce APIs using various authentication strategies.
"""

import logging
from typing import Dict, Optional

import httpx

from .auth import AuthStrategy, SalesforceAuthError

logger = logging.getLogger(__name__)


class SalesforceClient:
    """
    A client object containing Salesforce authentication details and basic API functionality.

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
        Initialize Salesforce client with an explicit authentication strategy.

        :param auth_strategy: Authentication strategy to use (ClientCredentialsAuth, RefreshTokenAuth, etc.)
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
        self._collections_api = None

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
            from .describe import DescribeAPI

            self._describe_api = DescribeAPI(self)
        return self._describe_api

    @property
    def bulk_v2(self):
        """Access to Salesforce Bulk API v2 methods."""
        if self._bulk_v2_api is None:
            from .bulk_v2 import BulkV2API

            self._bulk_v2_api = BulkV2API(self)
        return self._bulk_v2_api

    @property
    def query(self):
        """Access to Salesforce Query API methods."""
        if self._query_api is None:
            from .query import QueryAPI

            self._query_api = QueryAPI(self)
        return self._query_api

    @property
    def collections(self):
        """Access to Salesforce Collections API methods."""
        if self._collections_api is None:
            from .collections import CollectionsAPI

            self._collections_api = CollectionsAPI(self)
        return self._collections_api

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
        # Authenticate immediately when entering the context
        await self.ensure_authenticated()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def ensure_authenticated(self) -> str:
        """
        Ensure the client has a valid access token using the configured auth strategy.

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

        :param api_version: API version to use (defaults to client version)
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
        :param api_version: API version to use (defaults to client version)
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
        :param api_version: API version to use (defaults to client version)
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
        Make an authenticated HTTP request through the client.

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
