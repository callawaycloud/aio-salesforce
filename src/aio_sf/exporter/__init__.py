"""
Exporter module for aio-salesforce.

This module contains utilities for exporting Salesforce data to various formats.
The entire module requires optional dependencies (pandas, pyarrow).
"""

from .bulk_export import (
    bulk_query,
    get_bulk_fields,
    resume_from_locator,
    write_records_to_csv,
    QueryResult,
    batch_records_async,
)
from .parquet_writer import (
    ParquetWriter,
    create_schema_from_metadata,
    write_query_to_parquet,
    salesforce_to_arrow_type,
)

__all__ = [
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
