"""Salesforce Query API client."""

import asyncio
from typing import Any, Dict, List, Optional, AsyncGenerator

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..client import SalesforceClient

from .types import QueryResponse, QueryMoreResponse


class QueryResult:
    """
    A query result that supports len() and acts as an async iterator over individual records.
    Handles pagination automatically via QueryMore.
    """

    def __init__(
        self,
        query_api: "QueryAPI",
        initial_response: QueryResponse,
    ):
        """
        Initialize QueryResult.

        :param query_api: QueryAPI instance for making follow-up requests
        :param initial_response: Initial query response
        """
        self._query_api = query_api
        self._total_size = initial_response["totalSize"]
        self._done = initial_response["done"]
        self._records = initial_response["records"]
        self._next_records_url = initial_response.get("nextRecordsUrl")
        self._current_index = 0

    def __len__(self) -> int:
        """Return the total number of records."""
        return self._total_size

    @property
    def total_size(self) -> int:
        """Get the total number of records."""
        return self._total_size

    @property
    def done(self) -> bool:
        """Check if all records have been fetched."""
        return self._done and self._current_index >= len(self._records)

    def __iter__(self):
        """Synchronous iterator - collects all records in memory."""
        # For backward compatibility, we'll collect all records in a blocking manner
        # This is not ideal for large datasets, but maintains the existing API
        try:
            loop = asyncio.get_running_loop()
            # If we're in an async context, we can't block
            raise RuntimeError(
                "Cannot iterate QueryResult synchronously when an async event loop is already running. "
                "Use 'async for record in query_result' instead."
            )
        except RuntimeError as e:
            if "Cannot iterate" in str(e):
                raise e
            # No event loop is running, we can create one
            return asyncio.run(self._collect_all_records())

    async def _collect_all_records(self):
        """Collect all records into a list for synchronous iteration."""
        records = []
        async for record in self:
            records.append(record)
        return iter(records)

    async def __aiter__(self):
        """Async iterator that yields individual records."""
        # First, yield records from the initial response
        for record in self._records[self._current_index :]:
            self._current_index += 1
            yield record

        # Then handle pagination if needed
        next_url = self._next_records_url
        while next_url and not self._done:
            # Make QueryMore request
            more_response = await self._query_api.query_more(next_url)

            # Yield records from this batch
            for record in more_response["records"]:
                yield record

            # Update pagination state
            self._done = more_response["done"]
            next_url = more_response.get("nextRecordsUrl")

    async def collect_all(self) -> List[Dict[str, Any]]:
        """Collect all records into a list."""
        records = []
        async for record in self:
            records.append(record)
        return records


class QueryAPI:
    """Salesforce Query API client."""

    def __init__(self, client: "SalesforceClient"):
        """Initialize QueryAPI with a Salesforce client."""
        self._client = client

    def _get_query_url(self, api_version: Optional[str] = None) -> str:
        """Get the base URL for query requests."""
        version = api_version or self._client.version
        return f"{self._client.instance_url}/services/data/{version}/query"

    def _get_queryall_url(self, api_version: Optional[str] = None) -> str:
        """Get the base URL for queryAll requests (includes deleted records)."""
        version = api_version or self._client.version
        return f"{self._client.instance_url}/services/data/{version}/queryAll"

    def _sanitize_soql(self, soql: str) -> str:
        """
        Basic SOQL sanitization to prevent injection attacks.

        :param soql: SOQL query string
        :returns: Sanitized SOQL string
        :raises ValueError: If query contains potentially dangerous patterns
        """
        # Remove leading/trailing whitespace
        soql = soql.strip()

        # other sanitization?

        return soql

    async def soql(
        self,
        query: str,
        include_deleted: bool = False,
        api_version: Optional[str] = None,
    ) -> QueryResult:
        """
        Execute a SOQL query.

        :param query: SOQL query string
        :param include_deleted: If True, include deleted and archived records (uses queryAll endpoint)
        :param api_version: API version to use
        :returns: QueryResult that can be iterated over
        :raises ValueError: If SOQL is invalid or potentially dangerous
        """
        # Sanitize the SOQL query
        sanitized_soql = self._sanitize_soql(query)

        # Choose endpoint based on include_deleted parameter
        if include_deleted:
            url = self._get_queryall_url(api_version)
        else:
            url = self._get_query_url(api_version)

        # Prepare the request
        params = {"q": sanitized_soql}

        # Make the request
        response = await self._client.get(url, params=params)
        response.raise_for_status()

        # Return QueryResult for iteration
        return QueryResult(self, response.json())

    async def query_more(self, next_records_url: str) -> QueryMoreResponse:
        """
        Execute a QueryMore request to get the next batch of records.

        :param next_records_url: The nextRecordsUrl from a previous query response
        :returns: QueryMoreResponse with the next batch of records
        """
        # The next_records_url is relative to the instance, so we need to construct the full URL
        if next_records_url.startswith("/"):
            url = f"{self._client.instance_url}{next_records_url}"
        else:
            url = next_records_url

        # Make the request
        response = await self._client.get(url)
        response.raise_for_status()
        return response.json()

    async def explain(
        self,
        query: str,
        api_version: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get query execution plan for a SOQL query.

        :param query: SOQL query string
        :param api_version: API version to use
        :returns: Query execution plan
        """
        # Sanitize the SOQL query
        sanitized_soql = self._sanitize_soql(query)

        # Prepare the request
        url = self._get_query_url(api_version)
        params = {"q": sanitized_soql, "explain": "true"}

        # Make the request
        response = await self._client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    async def sosl(
        self,
        search: str,
        api_version: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute a SOSL (Salesforce Object Search Language) search.

        :param search: SOSL search string
        :param api_version: API version to use
        :returns: List of search results
        """
        # Basic SOSL validation
        if not search.strip().upper().startswith("FIND"):
            raise ValueError("SOSL queries must start with FIND")

        # Prepare the request
        version = api_version or self._client.version
        url = f"{self._client.instance_url}/services/data/{version}/search"
        params = {"q": search.strip()}

        # Make the request
        response = await self._client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("searchRecords", [])
