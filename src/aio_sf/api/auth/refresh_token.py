"""OAuth Refresh Token authentication strategy."""

import base64
import logging
from urllib.parse import urljoin

import httpx

from .base import AuthStrategy, SalesforceAuthError

logger = logging.getLogger(__name__)


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
