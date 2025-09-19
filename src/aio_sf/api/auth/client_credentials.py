"""OAuth Client Credentials authentication strategy."""

import base64
import logging
from urllib.parse import urljoin

import httpx

from .base import AuthStrategy, SalesforceAuthError

logger = logging.getLogger(__name__)


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
