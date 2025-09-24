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

# Core package exports client functionality
# Exporter functionality is included by default, but gracefully handles missing deps
__all__ = [
    "SalesforceClient",
    "SalesforceAuthError",
    "AuthStrategy",
    "ClientCredentialsAuth",
    "RefreshTokenAuth",
    "StaticTokenAuth",
    "SfdxCliAuth",
]

# Try to import exporter functionality if dependencies are available
try:
    from .exporter import (  # noqa: F401
        bulk_query,
        get_bulk_fields,
        resume_from_locator,
        write_records_to_csv,
        QueryResult,
        batch_records_async,
        ParquetWriter,
        create_schema_from_metadata,
        write_query_to_parquet,
        salesforce_to_arrow_type,
    )

    __all__.extend(
        [
            "bulk_query",
            "get_bulk_fields",
            "resume_from_locator",
            "write_records_to_csv",
            "QueryResult",
            "batch_records_async",
            "ParquetWriter",
            "create_schema_from_metadata",
            "write_query_to_parquet",
            "salesforce_to_arrow_type",
        ]
    )

except ImportError:
    # Exporter dependencies not available - this is fine for core-only installs
    pass
