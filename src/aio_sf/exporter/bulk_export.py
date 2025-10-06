import logging
from typing import Any, Dict, List, Generator, Optional
import csv
import asyncio
import io

from ..api.describe.types import FieldInfo
from ..api.client import SalesforceClient


class QueryResult:
    """
    A query result that supports len() and acts as an iterator over individual records.
    Can be created from a completed job or resumed from a locator.
    """

    def __init__(
        self,
        sf: SalesforceClient,
        job_id: str,
        total_records: Optional[int] = None,
        query_locator: Optional[str] = None,
        batch_size: int = 10000,
        api_version: Optional[str] = None,
    ):
        """
        Initialize QueryResult.

        :param sf: Salesforce client instance
        :param job_id: Salesforce job ID
        :param total_records: Total number of records (None if unknown, e.g., when resuming)
        :param query_locator: Starting locator (None to start from beginning)
        :param batch_size: Number of records to fetch per batch
        :param api_version: Salesforce API version (defaults to client version)
        """
        self._sf = sf
        self._job_id = job_id
        self._total_records = total_records
        self._query_locator = query_locator
        self._batch_size = batch_size
        self._api_version = api_version or sf.version

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

    def __aiter__(self):
        """Async iterator protocol - enables 'async for record in query_result'."""
        return self._generate_records()

    async def _collect_all_records(self):
        """Collect all records into a list for synchronous iteration."""
        records = []
        async for record in self._generate_records():
            records.append(record)
        return iter(records)

    def __len__(self) -> int:
        """Return the total number of records."""
        if self._total_records is None:
            raise ValueError(
                "Total record count is not available (likely resumed from locator)"
            )
        return self._total_records

    @property
    def total_records(self) -> Optional[int]:
        """Get the total number of records (None if unknown)."""
        return self._total_records

    def has_total_count(self) -> bool:
        """Check if total record count is available."""
        return self._total_records is not None

    @property
    def job_id(self) -> str:
        """Get the job ID."""
        return self._job_id

    def resume_from_locator(self, locator: str) -> "QueryResult":
        """
        Create a new QueryResult starting from the given locator.

        :param locator: The locator to resume from
        :returns: New QueryResult instance
        """
        return QueryResult(
            sf=self._sf,
            job_id=self._job_id,
            total_records=None,  # Unknown when resuming
            query_locator=locator,
            batch_size=self._batch_size,
            api_version=self._api_version,
        )

    def _stream_csv_to_records(
        self, response_text: str
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Stream CSV response and convert to record dictionaries.

        Uses proper CSV parsing to handle quotes, newlines, and special characters correctly.

        :param response_text: CSV response text
        :yields: Individual record dictionaries
        """
        if not response_text or not response_text.strip():
            # No data in this batch
            return

        try:
            # Create a StringIO object for proper CSV parsing
            csv_buffer = io.StringIO(response_text)

            # Use DictReader for proper CSV parsing with header detection
            # This handles quotes, newlines in fields, and escaping correctly
            csv_reader = csv.DictReader(
                csv_buffer,
                delimiter=",",
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                skipinitialspace=True,
            )

            for row_num, record in enumerate(csv_reader, start=1):
                try:
                    # Convert None values to empty strings for consistency
                    cleaned_record = {
                        key: (value if value is not None else "")
                        for key, value in record.items()
                    }
                    yield cleaned_record
                except Exception as e:
                    logging.warning(f"Error processing CSV record {row_num}: {e}")
                    # Continue processing other records
                    continue

        except csv.Error as e:
            logging.error(f"CSV parsing error: {e}")
            # If CSV parsing fails completely, don't yield any records
            return
        except Exception as e:
            logging.error(f"Unexpected error parsing CSV response: {e}")
            return

    async def _generate_records(self):
        """Async generator that yields individual records."""
        locator = self._query_locator
        ctn = 0

        try:
            while True:

                # Use the bulk_v2 API to get results
                response_text, next_locator = await self._sf.bulk_v2.get_job_results(
                    job_id=self._job_id,
                    locator=locator,
                    max_records=self._batch_size,
                    api_version=self._api_version,
                )

                for record in self._stream_csv_to_records(response_text):
                    ctn += 1
                    yield record

                # setup next locator
                locator = next_locator

                if not locator:
                    break

        except Exception as e:
            raise Exception(
                f"Error processing record {ctn}: {e}. Current Query Locator: {locator}. "
                f"This may indicate a CSV parsing issue - check if the response contains "
                f"malformed CSV data or fields with special characters."
            )


async def _wait_for_job_completion(
    sf: SalesforceClient,
    job_id: str,
    api_version: str,
    poll_interval: int,
    timeout: Optional[int],
) -> int:
    """
    Wait for a Salesforce bulk job to complete and return the total record count.

    :param sf: Salesforce client instance
    :param job_id: Job ID to monitor
    :param api_version: API version to use
    :param poll_interval: Time in seconds between status checks
    :param timeout: Maximum time to wait (None for no timeout)
    :returns: Total number of records processed
    :raises TimeoutError: If job doesn't complete within timeout
    :raises Exception: If job fails
    """
    # Use the new bulk_v2 API
    job_status = await sf.bulk_v2.wait_for_job_completion(
        job_id=job_id,
        poll_interval=poll_interval,
        timeout=timeout,
        api_version=api_version,
    )
    return job_status.get("numberRecordsProcessed", 0)


async def bulk_query(
    sf: SalesforceClient,
    soql_query: Optional[str],
    all_rows: bool = False,
    existing_job_id: Optional[str] = None,
    query_locator: Optional[str] = None,
    batch_size: int = 10000,
    api_version: Optional[str] = None,
    poll_interval: int = 5,
    timeout: Optional[int] = None,
) -> QueryResult:
    """
    Executes a Salesforce query via the BULK2 API and returns a QueryResult.

    :param sf: A SalesforceClient instance containing access_token, instance_url, and version.
    :param soql_query: The SOQL query string to execute.
    :param all_rows: If True, includes deleted and archived records.
    :param existing_job_id: Use an existing batch ID to continue processing.
    :param query_locator: Use an existing query locator to continue processing.
    :param batch_size: Number of records to fetch per batch.
    :param api_version: Salesforce API version to use (defaults to connection version).
    :param poll_interval: Time in seconds between job status checks.
    :param timeout: Maximum time in seconds to wait for job completion (None = no timeout).
    :returns: QueryResult that can be iterated over and supports len().
    """
    if not soql_query and not existing_job_id:
        raise ValueError("SOQL query or existing job ID must be provided")

    if query_locator and not existing_job_id:
        raise ValueError("query_locator may only be used with an existing job ID")

    # Use client version if no api_version specified
    effective_api_version = api_version or sf.version

    # Step 1: Create the job (if needed)
    if existing_job_id:
        job_id = existing_job_id
        logging.info(f"Using existing job id: {job_id}")
    elif soql_query:
        # Use the new bulk_v2 API to create the job
        job_info = await sf.bulk_v2.create_job(
            soql_query=soql_query,
            all_rows=all_rows,
            api_version=effective_api_version,
        )
        job_id = job_info["id"]
    else:
        raise ValueError("SOQL query or existing job ID must be provided")

    # Step 2: Wait for the job to complete
    total_records = await _wait_for_job_completion(
        sf, job_id, effective_api_version, poll_interval, timeout
    )

    # Step 3: Return QueryResult that manages its own data fetching
    return QueryResult(
        sf=sf,
        job_id=job_id,
        total_records=total_records,
        query_locator=query_locator,
        batch_size=batch_size,
        api_version=effective_api_version,
    )


def resume_from_locator(
    sf: SalesforceClient,
    job_id: str,
    locator: str,
    batch_size: int = 10000,
    api_version: Optional[str] = None,
) -> QueryResult:
    """
    Resume a bulk query from a locator. Useful when you only have a locator and job_id.

    :param sf: Salesforce client instance
    :param job_id: Salesforce job ID
    :param locator: Query locator to resume from
    :param batch_size: Number of records to fetch per batch
    :param api_version: Salesforce API version (defaults to client version)
    :returns: QueryResult that can be iterated over (len() will raise error since total is unknown)
    """
    return QueryResult(
        sf=sf,
        job_id=job_id,
        total_records=None,  # Unknown when resuming
        query_locator=locator,
        batch_size=batch_size,
        api_version=api_version,
    )


# Helper function to get all fields that can be queried by bulk API
async def get_bulk_fields(fields_metadata: List[FieldInfo]) -> List[FieldInfo]:
    """Get field metadata for queryable fields in a Salesforce object."""
    # Use the metadata API to get object description

    # Filter to only queryable fields that aren't compound fields (unless field is actually name)
    queryable_fields = [
        field
        for field in fields_metadata
        if field.get("type") not in ["address", "location", "base64"]
    ]

    return queryable_fields


def write_records_to_csv(
    query_result: QueryResult,
    file_path: str,
    encoding: str = "utf-8",
    delimiter: str = ",",
):
    """
    Write records from a QueryResult to a CSV file.

    :param query_result: QueryResult object yielding individual records
    :param file_path: Path to the output CSV file
    :param encoding: File encoding (default: utf-8)
    :param delimiter: CSV delimiter (default: comma)
    """
    with open(file_path, "w", newline="", encoding=encoding) as csvfile:
        writer = None

        for record in query_result:
            # Initialize writer with fieldnames from first record
            if writer is None:
                fieldnames = record.keys()
                writer = csv.DictWriter(
                    csvfile, fieldnames=fieldnames, delimiter=delimiter
                )
                writer.writeheader()

            writer.writerow(record)


async def batch_records_async(query_result: QueryResult, batch_size: int = 1000):
    """
    Convert individual records into batches for bulk operations (async version).

    :param query_result: QueryResult object yielding individual records
    :param batch_size: Number of records per batch
    :yields: Lists of records (batches)
    """
    batch = []
    async for record in query_result:
        batch.append(record)
        if len(batch) >= batch_size:
            yield batch
            batch = []

    # Yield any remaining records
    if batch:
        yield batch
