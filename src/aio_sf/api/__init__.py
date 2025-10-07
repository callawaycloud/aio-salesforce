"""
Salesforce API modules.

This package provides organized access to Salesforce APIs:
- describe: Object and organization describe/metadata
- bulk_v2: Bulk API v2 for large data operations
- query: SOQL queries and QueryMore operations
- collections: Bulk record operations (insert, update, upsert, delete)
"""

# Import API clients and types from organized submodules
from .bulk_v2 import (
    BulkV2API,
    BulkJobCreateRequest,
    BulkJobInfo,
    BulkJobStatus,
    BulkJobError,
)
from .collections import (
    CollectionsAPI,
    CollectionError,
    CollectionResult,
    CollectionInsertResponse,
    CollectionUpdateResponse,
    CollectionUpsertResponse,
    CollectionDeleteResponse,
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
)
from .types import (
    SalesforceAttributes,
    SalesforceRecord,
    GenericSalesforceRecord,
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
    "CollectionsAPI",
    "DescribeAPI",
    "QueryAPI",
    # Bulk v2 Types
    "BulkJobCreateRequest",
    "BulkJobInfo",
    "BulkJobStatus",
    "BulkJobError",
    # Collections Types
    "CollectionError",
    "CollectionResult",
    "CollectionInsertResponse",
    "CollectionUpdateResponse",
    "CollectionUpsertResponse",
    "CollectionDeleteResponse",
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
    "SalesforceRecord",
    "GenericSalesforceRecord",
    # Query Types
    "QueryResult",
    "QueryResponse",
    "QueryAllResponse",
    "QueryMoreResponse",
    "QueryErrorResponse",
]
