import asyncio
import os
from dotenv import load_dotenv

from aio_salesforce import SalesforceConnection, ClientCredentialsAuth
from aio_salesforce.exporter import (
    create_schema_from_metadata,
    write_query_to_parquet_async,
    bulk_query,
    get_bulk_fields,
)


load_dotenv()


async def main():
    # Create explicit auth strategy
    auth_strategy = ClientCredentialsAuth(
        instance_url=os.getenv("SF_INSTANCE_URL", ""),
        client_id=os.getenv("SF_CLIENT_ID", ""),
        client_secret=os.getenv("SF_CLIENT_SECRET", ""),
    )

    # Create connection with explicit auth strategy
    async with SalesforceConnection(auth_strategy=auth_strategy) as sf:

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
