"""aio-salesforce: Async Salesforce library for Python with Bulk API 2.0 support."""

__version__ = "0.1.0b1"
__author__ = "Jonas"
__email__ = "charlie@callaway.cloud"

# Connection functionality
from .connection import (  # noqa: F401
    SalesforceConnection,
    SalesforceAuthError,
    AuthStrategy,
    ClientCredentialsAuth,
    RefreshTokenAuth,
    StaticTokenAuth,
)

# Core package only exports connection functionality
# Users import exporter functions directly: from aio_salesforce.exporter import bulk_query

__all__ = [
    "SalesforceConnection",
    "SalesforceAuthError",
    "AuthStrategy",
    "ClientCredentialsAuth",
    "RefreshTokenAuth",
    "StaticTokenAuth",
]
