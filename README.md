# aio-sf

An async Salesforce library for Python.

## Features

### ✅ Supported APIs
- [x] **Bulk API 2.0** - Efficient querying of large datasets
- [x] **Describe API** - Field metadata and object descriptions
- [x] **SOQL Query API** - Standard Salesforce queries
- [x] **SObjects Collections API** - CRUD on collections of SObjects (up to 2000 records at a time)
- [ ] **Tooling API** - Development and deployment tools
- [ ] **Bulk API 1.0** - Legacy bulk operations
- [ ] **Streaming API** - Real-time event streaming

### ✅ Supported Authentication Strategies
- [x] **OAuth Client Credentials** - Automatic authentication
- [x] **Static Token** - Existing access tokens
- [x] **Refresh Token** - Refresh token flow
- [x] **SFDX CLI** - Login by grabbing a token from the SFDX CLI
- [ ] **Password Authentication** - Password + ST authentication (soap login)

### 🚀 Export Features
- [x] **Parquet Export** - Efficient columnar storage with schema mapping
- [x] **CSV Export** - Simple text format export
- [x] **Resume Support** - Resume interrupted queries using job IDs
- [x] **Streaming Processing** - Memory-efficient processing of large datasets

## Installation

### Full Package (Default - Includes Everything)
```bash
uv add aio-sf
# or: pip install aio-sf
```

### Core Only (Minimal Dependencies)
```bash
uv add "aio-sf[core]"
# or: pip install "aio-sf[core]"
```

## Quick Start

### Authentication & Connection
```python
import asyncio
import os
from aio_sf import SalesforceClient, ClientCredentialsAuth

async def main():
    auth = ClientCredentialsAuth(
        client_id=os.getenv('SF_CLIENT_ID'),
        client_secret=os.getenv('SF_CLIENT_SECRET'),
        instance_url=os.getenv('SF_INSTANCE_URL'),
    )
    
    async with SalesforceClient(auth_strategy=auth) as sf:
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
        # or: await query_result.collect_all() to collect all records into a list
        async for record in query_result:
            print(record.get("AccountId"))

        # Create a new Account
        await sf.collections.insert(
            sobject_type="Account",
            records=[{"Name": "Test Account"}]
        )

asyncio.run(main())
```

### Collections API - Batch Operations

Bulk operations (insert, update, upsert, delete) with automatic batching and concurrency.

**Basic Usage:**
```python
async with SalesforceClient(auth_strategy=auth) as sf:
    records = [{"Name": f"Account {i}"} for i in range(1000)]
    
    results = await sf.collections.insert(records, sobject_type="Account")
    # Also: update(), upsert(), delete()
```

**Advanced - With Retries, Concurrency Scaling, and Progress:**
```python
from aio_sf.api.collections import ResultInfo

async def on_result(info: ResultInfo):
    # Called after each batch completes with successes and errors split
    print(
        f"Batch: {len(info['successes'])} succeeded, {len(info['errors'])} failed | "
        f"Attempt {info['current_attempt']}, "
        f"Overall: {info['records_succeeded']} OK, {info['records_failed']} failed, "
        f"{info['records_pending']} pending"
    )
    # Inspect errors (includes both API errors and HTTP failures)
    for error in info['errors']:
        print(f"  Error: {error['errors']}")

async with SalesforceClient(auth_strategy=auth) as sf:
    results = await sf.collections.insert(
        records=records,
        sobject_type="Account",
        batch_size=[200, 100, 25],         # Shrink batch size on retry
        max_concurrent_batches=[5, 3, 1],  # Reduce concurrency on retry
        max_attempts=5,                    # Retry up to 5 times
        on_result=on_result,               # Callback with results
    )
```


## Exporter

The Exporter library contains a streamlined and "opinionated" way to export data from Salesforce to various formats.  


### 3. Export to Parquet
```python
# With full installation (default), you can import directly from aio_sf
from aio_sf import SalesforceClient, ClientCredentialsAuth, bulk_query, write_query_to_parquet

# Or import from the exporter module (both work)
# from aio_sf.exporter import bulk_query, write_query_to_parquet

async def main():
    # ... authentication code from above ...
    
    async with SalesforceClient(auth_strategy=auth) as sf:
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