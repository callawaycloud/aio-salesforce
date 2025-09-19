"""Authentication strategies for Salesforce API."""

from .base import AuthStrategy, SalesforceAuthError
from .client_credentials import ClientCredentialsAuth
from .refresh_token import RefreshTokenAuth
from .static_token import StaticTokenAuth
from .sfdx_cli import SfdxCliAuth

__all__ = [
    "AuthStrategy",
    "SalesforceAuthError",
    "ClientCredentialsAuth",
    "RefreshTokenAuth",
    "StaticTokenAuth",
    "SfdxCliAuth",
]
