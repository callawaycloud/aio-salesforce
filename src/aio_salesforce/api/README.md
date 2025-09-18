# Salesforce API Organization

This directory contains organized Salesforce API clients following a consistent convention.

## Directory Structure Convention

Each API module follows this structure:

```
api/
├── {api_name}/
│   ├── __init__.py          # Export API client and types
│   ├── client.py            # Main API client class
│   └── types.py             # TypedDict definitions for responses
└── __init__.py              # Export all APIs and types
```

## Current APIs

### `describe/` - Describe API
- **Client**: `DescribeAPI`
- **Purpose**: Object describe/metadata, organization info, limits
- **Key Methods**: 
  - `describe_sobject()` → `SObjectDescribe`
  - `list_sobjects()` → `List[SObjectInfo]`
  - `get_organization_info()` → `OrganizationInfo`
  - `get_limits()` → `OrganizationLimits`

### `bulk_v2/` - Bulk API v2
- **Client**: `BulkV2API`  
- **Purpose**: Large data operations, bulk queries
- **Key Methods**:
  - `create_job()` → `BulkJobInfo`
  - `get_job_status()` → `BulkJobStatus`
  - `get_job_results()` → `Tuple[str, Optional[str]]`
  - `wait_for_job_completion()` → `BulkJobStatus`

### `query/` - Query API
- **Client**: `QueryAPI`
- **Purpose**: SOQL queries, QueryMore, SOSL search
- **Key Methods**:
  - `soql(query, include_deleted=False)` → `QueryResult` (with async iteration)
  - `sosl(search)` → `List[Dict[str, Any]]` (SOSL search)
  - `explain(query)` → `Dict[str, Any]` (query execution plan)
  - `query_more()` → `QueryMoreResponse` (internal pagination)
- **Features**: SOQL injection protection, automatic pagination, deleted records support
- **Note**: Batch size is controlled by Salesforce, not configurable in Query API

## Naming Conventions

### API Clients
- **Class Name**: `{ApiName}API` (e.g., `DescribeAPI`, `BulkV2API`)
- **File**: `client.py`
- **Connection Property**: `sf.{api_name}` (e.g., `sf.describe`, `sf.bulk_v2`)

### Types
- **File**: `types.py`
- **Naming**: Descriptive, specific to the API
- **Examples**: `SObjectDescribe`, `BulkJobInfo`, `OrganizationLimits`

### Methods
- **Return Types**: Always use TypedDict for structured responses
- **Naming**: Clear, action-oriented (e.g., `get_job_status`, `describe_sobject`)
- **Parameters**: Use typed parameters with Optional where appropriate

## Adding New APIs

When adding a new API (e.g., `query` for SOQL):

1. **Create directory**: `api/query/`
2. **Create files**:
   ```python
   # api/query/__init__.py
   from .client import QueryAPI
   from .types import QueryResult, QueryError
   __all__ = ["QueryAPI", "QueryResult", "QueryError"]
   
   # api/query/client.py  
   class QueryAPI:
       def __init__(self, connection): ...
       async def execute(self, soql: str) -> QueryResult: ...
   
   # api/query/types.py
   class QueryResult(TypedDict): ...
   ```
3. **Add to main API `__init__.py`**:
   ```python
   from .query import QueryAPI, QueryResult, QueryError
   ```
4. **Add to connection**:
   ```python
   @property
   def query(self):
       if self._query_api is None:
           from .api.query import QueryAPI
           self._query_api = QueryAPI(self)
       return self._query_api
   ```

## Benefits

- **Organization**: Each API is self-contained
- **Type Safety**: Full TypedDict coverage
- **Discoverability**: Clear structure and naming
- **Maintainability**: Easy to add/modify APIs
- **Testing**: Each API can be tested independently
- **Documentation**: Types serve as API documentation
