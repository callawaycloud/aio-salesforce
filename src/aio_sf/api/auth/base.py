"""Base authentication strategy and exceptions."""

import time
from abc import ABC, abstractmethod
from typing import Optional

import httpx


class SalesforceAuthError(Exception):
    """Raised when authentication fails or tokens are invalid."""

    pass


class AuthStrategy(ABC):
    """Abstract base class for Salesforce authentication strategies."""

    def __init__(self, instance_url: str | None = None):
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
