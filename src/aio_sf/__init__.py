"""aio-salesforce: Async Salesforce library for Python with Bulk API 2.0 support."""

__author__ = "Jonas"
__email__ = "charlie@callaway.cloud"

# Client functionality
from .api.client import SalesforceClient  # noqa: F401
from .api.auth import (  # noqa: F401
    SalesforceAuthError,
    AuthStrategy,
    ClientCredentialsAuth,
    RefreshTokenAuth,
    StaticTokenAuth,
    SfdxCliAuth,
)

# Core package only exports client functionality
# Users import exporter functions directly: from aio_sf.exporter import bulk_query

__all__ = [
    "SalesforceClient",
    "SalesforceAuthError",
    "AuthStrategy",
    "ClientCredentialsAuth",
    "RefreshTokenAuth",
    "StaticTokenAuth",
    "SfdxCliAuth",
]
