"""
Salesforce Bulk API v2.
"""

from .client import BulkV2API
from .types import (
    BulkJobCreateRequest,
    BulkJobInfo,
    BulkJobStatus,
    BulkJobError,
)

__all__ = [
    # API Client
    "BulkV2API",
    # Types
    "BulkJobCreateRequest",
    "BulkJobInfo",
    "BulkJobStatus",
    "BulkJobError",
]
