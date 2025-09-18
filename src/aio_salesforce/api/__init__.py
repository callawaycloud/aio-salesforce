"""
Salesforce API modules.

This package provides organized access to Salesforce APIs:
- describe: Object and organization describe/metadata
- bulk_v2: Bulk API v2 for large data operations
- query: SOQL queries and QueryMore operations
"""

# Import API clients and types from organized submodules
from .bulk_v2 import (
    BulkV2API,
    BulkJobCreateRequest,
    BulkJobInfo,
    BulkJobStatus,
    BulkJobError,
)
from .describe import (
    DescribeAPI,
    FieldInfo,
    LimitInfo,
    OrganizationInfo,
    OrganizationLimits,
    PicklistValue,
    RecordTypeInfo,
    SObjectDescribe,
    SObjectInfo,
    SalesforceAttributes,
)
from .query import (
    QueryAPI,
    QueryResult,
    QueryResponse,
    QueryAllResponse,
    QueryMoreResponse,
    QueryErrorResponse,
)

__all__ = [
    # API Clients
    "BulkV2API",
    "DescribeAPI",
    "QueryAPI",
    # Bulk v2 Types
    "BulkJobCreateRequest",
    "BulkJobInfo",
    "BulkJobStatus",
    "BulkJobError",
    # Describe Types
    "FieldInfo",
    "LimitInfo",
    "OrganizationInfo",
    "OrganizationLimits",
    "PicklistValue",
    "RecordTypeInfo",
    "SObjectDescribe",
    "SObjectInfo",
    "SalesforceAttributes",
    # Query Types
    "QueryResult",
    "QueryResponse",
    "QueryAllResponse",
    "QueryMoreResponse",
    "QueryErrorResponse",
]
