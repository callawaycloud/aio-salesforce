"""Salesforce Collections API client."""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Union, Tuple

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..client import SalesforceClient

from .types import (
    CollectionInsertResponse,
    CollectionUpdateResponse,
    CollectionUpsertResponse,
    CollectionDeleteResponse,
)
from ..types import GenericSalesforceRecord, SalesforceRecord, SalesforceAttributes

logger = logging.getLogger(__name__)


class CollectionsAPI:
    """Salesforce Collections API client for bulk record operations."""

    # Salesforce Collections API limits by operation type
    MAX_RECORDS_INSERT = 200
    MAX_RECORDS_UPDATE = 200
    MAX_RECORDS_UPSERT = 200
    MAX_RECORDS_DELETE = 200

    def __init__(self, client: "SalesforceClient"):
        """Initialize CollectionsAPI with a Salesforce client."""
        self._client = client

    def _get_collections_url(self, api_version: Optional[str] = None) -> str:
        """Get the base URL for collections requests."""
        base_url = self._client.get_base_url(api_version)
        return f"{base_url}/composite/sobjects"

    def _detect_record_type_and_sobject(
        self,
        records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
        sobject_type: Optional[str] = None,
    ) -> Tuple[str, bool]:
        """
        Detect if records are SalesforceRecord type and extract/validate sobject_type.

        :param records: List of records to analyze
        :param sobject_type: Optional sobject_type parameter
        :returns: Tuple of (sobject_type, has_attributes) where has_attributes indicates if records have SalesforceAttributes
        :raises ValueError: If sobject_type is required but not provided, or if records have inconsistent types
        """
        if not records:
            if sobject_type:
                return sobject_type, False
            raise ValueError("sobject_type is required when records list is empty")

        # Check first record to determine type
        first_record = records[0]
        has_attributes = isinstance(first_record, dict) and "attributes" in first_record

        if has_attributes:
            # Validate all records have attributes and consistent sobject_type
            extracted_sobject_type = None
            for i, record in enumerate(records):
                if not isinstance(record, dict) or "attributes" not in record:
                    raise ValueError(
                        f"Record at index {i} is missing 'attributes' field, but other records have it. All records must be consistent."
                    )

                attributes = record.get("attributes", {})
                if not isinstance(attributes, dict) or "type" not in attributes:
                    raise ValueError(
                        f"Record at index {i} has invalid 'attributes' structure. Expected dict with 'type' field."
                    )

                record_type = attributes["type"]
                if extracted_sobject_type is None:
                    extracted_sobject_type = record_type
                elif extracted_sobject_type != record_type:
                    raise ValueError(
                        f"Record at index {i} has sobject_type '{record_type}', but previous records have '{extracted_sobject_type}'. All records must have the same sobject_type."
                    )

            # If sobject_type was provided, validate it matches extracted type
            if sobject_type is not None and sobject_type != extracted_sobject_type:
                raise ValueError(
                    f"Provided sobject_type '{sobject_type}' does not match sobject_type '{extracted_sobject_type}' found in record attributes."
                )

            # extracted_sobject_type is guaranteed to be a string at this point
            assert extracted_sobject_type is not None
            return extracted_sobject_type, True
        else:
            # Records don't have attributes, sobject_type is required
            if sobject_type is None:
                raise ValueError(
                    "sobject_type is required when records do not have SalesforceAttributes (attributes field)"
                )

            # Validate that none of the records have attributes (consistency check)
            for i, record in enumerate(records):
                if isinstance(record, dict) and "attributes" in record:
                    raise ValueError(
                        f"Record at index {i} has 'attributes' field, but first record doesn't. All records must be consistent."
                    )

            return sobject_type, False

    def _prepare_records(
        self,
        records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
        sobject_type: str,
        has_attributes: bool = False,
    ) -> List[GenericSalesforceRecord]:
        """
        Prepare records for collection operations by adding attributes if needed.

        :param records: List of records to prepare
        :param sobject_type: Salesforce object type (e.g., 'Account', 'Contact')
        :param has_attributes: Whether records already have SalesforceAttributes
        :returns: Records with attributes added (if needed)
        """
        if has_attributes:
            # Records already have attributes, return as-is (they're already GenericSalesforceRecord)
            # Type cast to satisfy the type checker since we know these are SalesforceRecord with attributes
            return records  # type: ignore

        # Add attributes to records that don't have them
        prepared_records = []
        for record in records:
            prepared_record = {"attributes": {"type": sobject_type}, **record}
            prepared_records.append(prepared_record)
        return prepared_records

    def _split_into_batches(
        self, items: List[Any], batch_size: int, max_limit: int
    ) -> List[List[Any]]:
        """
        Split a list of items into batches of specified size.

        :param items: List of items to split
        :param batch_size: Maximum size of each batch
        :param max_limit: Maximum allowed batch size for the operation
        :returns: List of batches
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")
        if batch_size > max_limit:
            raise ValueError(
                f"batch_size ({batch_size}) cannot exceed Salesforce limit ({max_limit})"
            )

        batches = []
        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]
            batches.append(batch)
        return batches

    async def _process_batches_concurrently(
        self,
        batches: List[Any],
        operation_func,
        max_concurrent_batches: int,
        *args,
        **kwargs,
    ) -> List[Any]:
        """
        Process batches concurrently with a limit on concurrent operations.

        IMPORTANT: Order preservation is guaranteed - results are returned in the same
        order as the input batches, regardless of which batch completes first. This
        ensures that errors can be correlated back to the original record indices.

        :param batches: List of batches to process
        :param operation_func: Function to call for each batch
        :param max_concurrent_batches: Maximum number of concurrent batch operations
        :param args: Additional positional arguments for operation_func
        :param kwargs: Additional keyword arguments for operation_func
        :returns: List of results from all batches in the same order as input
        """
        if max_concurrent_batches <= 0:
            raise ValueError("max_concurrent_batches must be greater than 0")

        semaphore = asyncio.Semaphore(max_concurrent_batches)

        async def process_batch_with_semaphore(batch):
            async with semaphore:
                return await operation_func(batch, *args, **kwargs)

        # Process all batches concurrently with semaphore limiting concurrency
        tasks = [process_batch_with_semaphore(batch) for batch in batches]
        # asyncio.gather() preserves order - results will be in same order as input batches
        results = await asyncio.gather(*tasks)

        # Flatten results from all batches, maintaining order
        # This ensures result[i] corresponds to the original input item at index i
        flattened_results = []
        for batch_result in results:
            flattened_results.extend(batch_result)

        return flattened_results

    async def _insert_single_batch(
        self,
        records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
        sobject_type: Optional[str] = None,
        all_or_none: bool = True,
        api_version: Optional[str] = None,
    ) -> CollectionInsertResponse:
        """
        Insert a single batch of records using the Collections API.
        Internal method used by the public insert method.

        :param records: List of records to insert (must be <= MAX_RECORDS_PER_REQUEST)
        :param sobject_type: Salesforce object type (e.g., 'Account', 'Contact') - optional if records have attributes
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param api_version: API version to use
        :returns: List of results for each record
        :raises httpx.HTTPError: If the request fails
        :raises ValueError: If sobject_type is required but not provided
        """
        if len(records) > self.MAX_RECORDS_INSERT:
            raise ValueError(
                f"Batch size ({len(records)}) exceeds maximum allowed ({self.MAX_RECORDS_INSERT})"
            )

        # Detect record type and extract/validate sobject_type
        actual_sobject_type, has_attributes = self._detect_record_type_and_sobject(
            records, sobject_type
        )

        url = self._get_collections_url(api_version)

        # Prepare the request payload
        payload = {
            "allOrNone": all_or_none,
            "records": self._prepare_records(
                records, actual_sobject_type, has_attributes
            ),
        }

        # Make the request
        response = await self._client.post(url, json=payload)
        response.raise_for_status()

        return response.json()

    async def insert(
        self,
        records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
        sobject_type: Optional[str] = None,
        all_or_none: bool = True,
        batch_size: int = 200,
        max_concurrent_batches: int = 5,
        api_version: Optional[str] = None,
    ) -> CollectionInsertResponse:
        """
        Insert multiple records using the Collections API.
        Automatically handles batching for large datasets.

        Order preservation: Results are returned in the same order as input records,
        enabling error correlation by index (result[i] corresponds to records[i]).

        :param records: List of records to insert (Dict or SalesforceRecord)
        :param sobject_type: Salesforce object type (e.g., 'Account', 'Contact') - optional if records have attributes
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param batch_size: Number of records per batch (max 200)
        :param max_concurrent_batches: Maximum number of concurrent batch operations
        :param api_version: API version to use
        :returns: List of results for each record, in same order as input
        :raises httpx.HTTPError: If the request fails
        :raises ValueError: If batch_size exceeds limits or sobject_type is required but not provided
        """
        if not records:
            return []

        # Detect record type and extract/validate sobject_type early
        actual_sobject_type, has_attributes = self._detect_record_type_and_sobject(
            records, sobject_type
        )

        logger.info(
            f"Inserting {len(records)} {actual_sobject_type} records via Collections API "
            f"(batch_size={batch_size}, max_concurrent={max_concurrent_batches})"
        )

        # If records fit in a single batch, process directly
        if len(records) <= batch_size:
            result = await self._insert_single_batch(
                records, actual_sobject_type, all_or_none, api_version
            )
            logger.info(
                f"Insert operation completed for {len(records)} {actual_sobject_type} records"
            )
            return result

        # Split into batches and process concurrently
        batches = self._split_into_batches(records, batch_size, self.MAX_RECORDS_INSERT)
        logger.info(f"Split {len(records)} records into {len(batches)} batches")

        results = await self._process_batches_concurrently(
            batches,
            self._insert_single_batch,
            max_concurrent_batches,
            actual_sobject_type,
            all_or_none,
            api_version,
        )

        logger.info(
            f"Insert operation completed for {len(records)} {actual_sobject_type} records "
            f"across {len(batches)} batches"
        )
        return results

    async def _update_single_batch(
        self,
        records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
        sobject_type: Optional[str] = None,
        all_or_none: bool = True,
        api_version: Optional[str] = None,
    ) -> CollectionUpdateResponse:
        """
        Update a single batch of records using the Collections API.
        Internal method used by the public update method.

        :param records: List of records to update (must be <= MAX_RECORDS_UPDATE)
        :param sobject_type: Salesforce object type (e.g., 'Account', 'Contact') - optional if records have attributes
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param api_version: API version to use
        :returns: List of results for each record
        :raises httpx.HTTPError: If the request fails
        :raises ValueError: If records don't contain Id field, batch size exceeds limit, or sobject_type is required but not provided
        """
        if len(records) > self.MAX_RECORDS_UPDATE:
            raise ValueError(
                f"Batch size ({len(records)}) exceeds maximum allowed ({self.MAX_RECORDS_UPDATE})"
            )

        # Detect record type and extract/validate sobject_type
        actual_sobject_type, has_attributes = self._detect_record_type_and_sobject(
            records, sobject_type
        )

        # Validate that all records have Id field
        for i, record in enumerate(records):
            if "Id" not in record:
                raise ValueError(
                    f"Record at index {i} is missing required 'Id' field for update operation"
                )

        url = self._get_collections_url(api_version)

        # Prepare the request payload
        payload = {
            "allOrNone": all_or_none,
            "records": self._prepare_records(
                records, actual_sobject_type, has_attributes
            ),
        }

        # Make the request using PATCH method
        response = await self._client.request("PATCH", url, json=payload)
        response.raise_for_status()

        return response.json()

    async def update(
        self,
        records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
        sobject_type: Optional[str] = None,
        all_or_none: bool = True,
        batch_size: int = 200,
        max_concurrent_batches: int = 5,
        api_version: Optional[str] = None,
    ) -> CollectionUpdateResponse:
        """
        Update multiple records using the Collections API.
        Automatically handles batching for large datasets.

        Order preservation: Results are returned in the same order as input records,
        enabling error correlation by index (result[i] corresponds to records[i]).

        :param records: List of records to update (must include Id field) (Dict or SalesforceRecord)
        :param sobject_type: Salesforce object type (e.g., 'Account', 'Contact') - optional if records have attributes
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param batch_size: Number of records per batch (max 200)
        :param max_concurrent_batches: Maximum number of concurrent batch operations
        :param api_version: API version to use
        :returns: List of results for each record, in same order as input
        :raises httpx.HTTPError: If the request fails
        :raises ValueError: If records don't contain Id field, batch_size exceeds limits, or sobject_type is required but not provided
        """
        if not records:
            return []

        # Detect record type and extract/validate sobject_type early
        actual_sobject_type, has_attributes = self._detect_record_type_and_sobject(
            records, sobject_type
        )

        logger.info(
            f"Updating {len(records)} {actual_sobject_type} records via Collections API "
            f"(batch_size={batch_size}, max_concurrent={max_concurrent_batches})"
        )

        # If records fit in a single batch, process directly
        if len(records) <= batch_size:
            result = await self._update_single_batch(
                records, actual_sobject_type, all_or_none, api_version
            )
            logger.info(
                f"Update operation completed for {len(records)} {actual_sobject_type} records"
            )
            return result

        # Split into batches and process concurrently
        batches = self._split_into_batches(records, batch_size, self.MAX_RECORDS_UPDATE)
        logger.info(f"Split {len(records)} records into {len(batches)} batches")

        results = await self._process_batches_concurrently(
            batches,
            self._update_single_batch,
            max_concurrent_batches,
            actual_sobject_type,
            all_or_none,
            api_version,
        )

        logger.info(
            f"Update operation completed for {len(records)} {actual_sobject_type} records "
            f"across {len(batches)} batches"
        )
        return results

    async def _upsert_single_batch(
        self,
        records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
        external_id_field: str,
        sobject_type: Optional[str] = None,
        all_or_none: bool = True,
        api_version: Optional[str] = None,
    ) -> CollectionUpsertResponse:
        """
        Upsert a single batch of records using the Collections API.
        Internal method used by the public upsert method.

        :param records: List of records to upsert (must be <= MAX_RECORDS_UPSERT)
        :param external_id_field: External ID field name for upsert operations
        :param sobject_type: Salesforce object type (e.g., 'Account', 'Contact') - optional if records have attributes
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param api_version: API version to use
        :returns: List of results for each record
        :raises httpx.HTTPError: If the request fails
        :raises ValueError: If records don't contain external_id_field, batch size exceeds limit, or sobject_type is required but not provided
        """
        if len(records) > self.MAX_RECORDS_UPSERT:
            raise ValueError(
                f"Batch size ({len(records)}) exceeds maximum allowed ({self.MAX_RECORDS_UPSERT})"
            )

        # Detect record type and extract/validate sobject_type
        actual_sobject_type, has_attributes = self._detect_record_type_and_sobject(
            records, sobject_type
        )

        # Validate that all records have the external ID field
        for i, record in enumerate(records):
            if external_id_field not in record:
                raise ValueError(
                    f"Record at index {i} is missing required '{external_id_field}' field for upsert operation"
                )

        # For upsert, the URL includes the sobject type and external ID field
        base_url = self._get_collections_url(api_version)
        url = f"{base_url}/{actual_sobject_type}/{external_id_field}"

        # Prepare the request payload
        payload = {
            "allOrNone": all_or_none,
            "records": self._prepare_records(
                records, actual_sobject_type, has_attributes
            ),
        }

        # Make the request using PATCH method
        response = await self._client.request("PATCH", url, json=payload)
        response.raise_for_status()

        return response.json()

    async def upsert(
        self,
        records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
        external_id_field: str,
        sobject_type: Optional[str] = None,
        all_or_none: bool = True,
        batch_size: int = 200,
        max_concurrent_batches: int = 5,
        api_version: Optional[str] = None,
    ) -> CollectionUpsertResponse:
        """
        Upsert multiple records using the Collections API.
        Automatically handles batching for large datasets.

        Order preservation: Results are returned in the same order as input records,
        enabling error correlation by index (result[i] corresponds to records[i]).

        :param records: List of records to upsert (must include external_id_field) (Dict or SalesforceRecord)
        :param external_id_field: External ID field name for upsert operations
        :param sobject_type: Salesforce object type (e.g., 'Account', 'Contact') - optional if records have attributes
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param batch_size: Number of records per batch (max 200)
        :param max_concurrent_batches: Maximum number of concurrent batch operations
        :param api_version: API version to use
        :returns: List of results for each record, in same order as input
        :raises httpx.HTTPError: If the request fails
        :raises ValueError: If records don't contain external_id_field, batch_size exceeds limits, or sobject_type is required but not provided
        """
        if not records:
            return []

        # Detect record type and extract/validate sobject_type early
        actual_sobject_type, has_attributes = self._detect_record_type_and_sobject(
            records, sobject_type
        )

        logger.info(
            f"Upserting {len(records)} {actual_sobject_type} records via Collections API using {external_id_field} "
            f"(batch_size={batch_size}, max_concurrent={max_concurrent_batches})"
        )

        # If records fit in a single batch, process directly
        if len(records) <= batch_size:
            result = await self._upsert_single_batch(
                records,
                external_id_field,
                actual_sobject_type,
                all_or_none,
                api_version,
            )
            logger.info(
                f"Upsert operation completed for {len(records)} {actual_sobject_type} records"
            )
            return result

        # Split into batches and process concurrently
        batches = self._split_into_batches(records, batch_size, self.MAX_RECORDS_UPSERT)
        logger.info(f"Split {len(records)} records into {len(batches)} batches")

        results = await self._process_batches_concurrently(
            batches,
            self._upsert_single_batch,
            max_concurrent_batches,
            external_id_field,
            actual_sobject_type,
            all_or_none,
            api_version,
        )

        logger.info(
            f"Upsert operation completed for {len(records)} {actual_sobject_type} records "
            f"across {len(batches)} batches"
        )
        return results

    async def _delete_single_batch(
        self,
        record_ids: List[str],
        all_or_none: bool = True,
        api_version: Optional[str] = None,
    ) -> CollectionDeleteResponse:
        """
        Delete a single batch of records using the Collections API.
        Internal method used by the public delete method.

        :param record_ids: List of record IDs to delete (must be <= MAX_RECORDS_DELETE)
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param api_version: API version to use
        :returns: List of results for each record
        :raises httpx.HTTPError: If the request fails
        :raises ValueError: If batch size exceeds limit
        """
        if len(record_ids) > self.MAX_RECORDS_DELETE:
            raise ValueError(
                f"Batch size ({len(record_ids)}) exceeds maximum allowed ({self.MAX_RECORDS_DELETE})"
            )

        url = self._get_collections_url(api_version)

        # For delete operations, we pass ids and allOrNone as query parameters
        params = {"ids": ",".join(record_ids), "allOrNone": str(all_or_none).lower()}

        # Make the request using DELETE method
        response = await self._client.request("DELETE", url, params=params)
        response.raise_for_status()

        return response.json()

    async def delete(
        self,
        record_ids: List[str],
        all_or_none: bool = True,
        batch_size: int = 200,
        max_concurrent_batches: int = 5,
        api_version: Optional[str] = None,
    ) -> CollectionDeleteResponse:
        """
        Delete multiple records using the Collections API.
        Automatically handles batching for large datasets.

        Order preservation: Results are returned in the same order as input record_ids,
        enabling error correlation by index (result[i] corresponds to record_ids[i]).

        :param record_ids: List of record IDs to delete
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param batch_size: Number of records per batch (max 2000)
        :param max_concurrent_batches: Maximum number of concurrent batch operations
        :param api_version: API version to use
        :returns: List of results for each record, in same order as input
        :raises httpx.HTTPError: If the request fails
        :raises ValueError: If record_ids is empty or batch_size exceeds limits
        """
        if not record_ids:
            raise ValueError("record_ids cannot be empty")

        logger.info(
            f"Deleting {len(record_ids)} records via Collections API "
            f"(batch_size={batch_size}, max_concurrent={max_concurrent_batches})"
        )

        # If records fit in a single batch, process directly
        if len(record_ids) <= batch_size:
            result = await self._delete_single_batch(
                record_ids, all_or_none, api_version
            )
            logger.info(f"Delete operation completed for {len(record_ids)} records")
            return result

        # Split into batches and process concurrently
        batches = self._split_into_batches(
            record_ids, batch_size, self.MAX_RECORDS_DELETE
        )
        logger.info(f"Split {len(record_ids)} records into {len(batches)} batches")

        results = await self._process_batches_concurrently(
            batches,
            self._delete_single_batch,
            max_concurrent_batches,
            all_or_none,
            api_version,
        )

        logger.info(
            f"Delete operation completed for {len(record_ids)} records "
            f"across {len(batches)} batches"
        )
        return results
