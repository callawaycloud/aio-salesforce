"""Static access token authentication strategy."""

import httpx

from .base import AuthStrategy, SalesforceAuthError


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
