# aio-salesforce

An async Salesforce library for Python with Bulk API 2.0 support.

## Features

### ✅ Supported APIs
- [x] **OAuth Client Credentials Flow** - Automatic authentication
- [x] **Bulk API 2.0** - Efficient querying of large datasets
- [x] **Describe API** - Field metadata and object descriptions
- [x] **SOQL Query API** - Standard Salesforce queries

### 🔄 Planned APIs
- [ ] **SObjects API** - Standard CRUD operations
- [ ] **Tooling API** - Development and deployment tools
- [ ] **Bulk API 1.0** - Legacy bulk operations
- [ ] **Streaming API** - Real-time event streaming

### 🚀 Export Features
- [x] **Parquet Export** - Efficient columnar storage with schema mapping
- [x] **CSV Export** - Simple text format export
- [x] **Resume Support** - Resume interrupted queries using job IDs
- [x] **Streaming Processing** - Memory-efficient processing of large datasets
- [x] **Type Mapping** - Automatic Salesforce to PyArrow type conversion

## Installation

### Core (Connection Only)
```bash
uv add aio-salesforce
# or: pip install aio-salesforce
```

### With Export Capabilities
```bash
uv add "aio-salesforce[exporter]"
# or: pip install "aio-salesforce[exporter]"
```

## Quick Start

### Authentication & Connection
```python
import asyncio
import os
from aio_salesforce import SalesforceConnection, ClientCredentialsAuth

async def main():
    auth = ClientCredentialsAuth(
        client_id=os.getenv('SF_CLIENT_ID'),
        client_secret=os.getenv('SF_CLIENT_SECRET'),
        instance_url=os.getenv('SF_INSTANCE_URL'),
    )
    
    async with SalesforceConnection(auth_strategy=auth) as sf:
        print(f"✅ Connected to: {sf.instance_url}")

        sobjects = await sf.describe.list_sobjects()
        print(sobjects[0]["name"])

        contact_describe = await sf.describe.sobject("Contact")

        # retrieve first 5 "creatable" fields on contact
        queryable_fields = [
            field.get("name", "")
            for field in contact_describe["fields"]
            if field.get("createable")
        ][:5]

        query = f"SELECT {', '.join(queryable_fields)} FROM Contact LIMIT 5"
        print(query)

        query_result = await sf.query.soql(query)
        # Loop over records using async iteration
        async for record in query_result:
            print(record.get("AccountId"))

asyncio.run(main())
```




## Exporter

The Exporter library contains a streamlined and "opinionated" way to export data from Salesforce to various formats.  

### 2. Query Records
```python
from aio_salesforce.exporter import bulk_query

async def main():
    # ... authentication code from above ...
    
    async with SalesforceConnection(auth_strategy=auth) as sf:
        # Execute bulk query
        query_result = await bulk_query(
            sf=sf,
            soql_query="SELECT Id, Name, Email FROM Contact LIMIT 1000"
        )
        
        # Process records
        count = 0
        async for record in query_result:
            print(f"Contact: {record['Name']} - {record['Email']}")
            count += 1
            
        print(f"Processed {count} records")
```

### 3. Export to Parquet
```python
from aio_salesforce.exporter import bulk_query, write_query_to_parquet

async def main():
    # ... authentication code from above ...
    
    async with SalesforceConnection(auth_strategy=auth) as sf:
        # Query with proper schema
        query_result = await bulk_query(
            sf=sf,
            soql_query="SELECT Id, Name, Email, CreatedDate FROM Contact"
        )
        
        # Export to Parquet
        write_query_to_parquet(
            query_result=query_result,
            file_path="contacts.parquet"
        )
        
        print(f"✅ Exported {len(query_result)} contacts to Parquet")
```


## License

MIT License