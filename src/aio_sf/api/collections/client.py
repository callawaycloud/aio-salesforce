"""Salesforce Collections API client."""

import logging
from typing import Any, Dict, List, Optional, Union

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..client import SalesforceClient

from .batch import ResultCallback, process_with_retries
from .records import (
    detect_record_type_and_sobject,
    prepare_records,
    validate_records_have_field,
)
from .retry import RecordWithAttempt, ShouldRetryCallback
from .types import (
    CollectionDeleteResponse,
    CollectionInsertResponse,
    CollectionUpdateResponse,
    CollectionUpsertResponse,
)
from ..types import SalesforceRecord


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

        :param records: List of records to insert (must be <= MAX_RECORDS_INSERT)
        :param sobject_type: Salesforce object type - optional if records have attributes
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

        actual_sobject_type, has_attributes = detect_record_type_and_sobject(
            records, sobject_type
        )

        url = self._get_collections_url(api_version)
        payload = {
            "allOrNone": all_or_none,
            "records": prepare_records(records, actual_sobject_type, has_attributes),
        }

        response = await self._client.post(url, json=payload)
        response.raise_for_status()

        return response.json()

    async def insert(
        self,
        records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
        sobject_type: Optional[str] = None,
        all_or_none: bool = True,
        batch_size: Union[int, List[int]] = 200,
        max_concurrent_batches: Union[int, List[int]] = 5,
        api_version: Optional[str] = None,
        on_result: Optional[ResultCallback] = None,
        max_attempts: int = 1,
        should_retry: Optional[ShouldRetryCallback] = None,
    ) -> CollectionInsertResponse:
        """
        Insert multiple records using the Collections API.
        Automatically handles batching for large datasets with optional retry logic.

        Order preservation: Results are returned in the same order as input records,
        enabling error correlation by index (result[i] corresponds to records[i]).

        :param records: List of records to insert (Dict or SalesforceRecord)
        :param sobject_type: Salesforce object type - optional if records have attributes
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param batch_size: Batch size (int for same size, or list of ints per attempt). Max 200.
        :param max_concurrent_batches: Maximum number of concurrent batch operations
        :param api_version: API version to use
        :param on_result: Optional async callback invoked after each batch completes with results
        :param max_attempts: Maximum number of attempts per record (default: 1, no retries)
        :param should_retry: Optional callback to determine if a failed record should be retried
        :returns: List of results for each record, in same order as input
        :raises httpx.HTTPError: If the request fails
        :raises ValueError: If batch_size exceeds limits or sobject_type is required but not provided
        """
        if not records:
            return []

        actual_sobject_type, has_attributes = detect_record_type_and_sobject(
            records, sobject_type
        )

        logger.info(
            f"Inserting {len(records)} {actual_sobject_type} records via Collections API "
            f"(batch_size={batch_size}, max_concurrent={max_concurrent_batches})"
        )

        records_with_attempts = [
            RecordWithAttempt(record, i, 1) for i, record in enumerate(records)
        ]

        results = await process_with_retries(
            records_with_attempts,
            self._insert_single_batch,
            batch_size,
            max_attempts,
            should_retry,
            max_concurrent_batches,
            on_result,
            self.MAX_RECORDS_INSERT,
            actual_sobject_type,
            all_or_none,
            api_version,
        )

        logger.info(
            f"Insert operation completed for {len(records)} {actual_sobject_type} records"
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
        :param sobject_type: Salesforce object type - optional if records have attributes
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param api_version: API version to use
        :returns: List of results for each record
        :raises httpx.HTTPError: If the request fails
        :raises ValueError: If records don't contain Id field or sobject_type is required but not provided
        """
        if len(records) > self.MAX_RECORDS_UPDATE:
            raise ValueError(
                f"Batch size ({len(records)}) exceeds maximum allowed ({self.MAX_RECORDS_UPDATE})"
            )

        actual_sobject_type, has_attributes = detect_record_type_and_sobject(
            records, sobject_type
        )

        validate_records_have_field(records, "Id", "update")

        url = self._get_collections_url(api_version)
        payload = {
            "allOrNone": all_or_none,
            "records": prepare_records(records, actual_sobject_type, has_attributes),
        }

        response = await self._client.request("PATCH", url, json=payload)
        response.raise_for_status()

        return response.json()

    async def update(
        self,
        records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
        sobject_type: Optional[str] = None,
        all_or_none: bool = True,
        batch_size: Union[int, List[int]] = 200,
        max_concurrent_batches: Union[int, List[int]] = 5,
        api_version: Optional[str] = None,
        on_result: Optional[ResultCallback] = None,
        max_attempts: int = 1,
        should_retry: Optional[ShouldRetryCallback] = None,
    ) -> CollectionUpdateResponse:
        """
        Update multiple records using the Collections API.
        Automatically handles batching for large datasets with optional retry logic.

        Order preservation: Results are returned in the same order as input records,
        enabling error correlation by index (result[i] corresponds to records[i]).

        :param records: List of records to update (must include Id field)
        :param sobject_type: Salesforce object type - optional if records have attributes
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param batch_size: Batch size (int for same size, or list of ints per attempt). Max 200.
        :param max_concurrent_batches: Maximum number of concurrent batch operations
        :param api_version: API version to use
        :param on_result: Optional async callback invoked after each batch completes with results
        :param max_attempts: Maximum number of attempts per record (default: 1, no retries)
        :param should_retry: Optional callback to determine if a failed record should be retried
        :returns: List of results for each record, in same order as input
        :raises httpx.HTTPError: If the request fails
        :raises ValueError: If records don't contain Id field or sobject_type is required but not provided
        """
        if not records:
            return []

        actual_sobject_type, has_attributes = detect_record_type_and_sobject(
            records, sobject_type
        )

        logger.info(
            f"Updating {len(records)} {actual_sobject_type} records via Collections API "
            f"(batch_size={batch_size}, max_concurrent={max_concurrent_batches})"
        )

        records_with_attempts = [
            RecordWithAttempt(record, i, 1) for i, record in enumerate(records)
        ]

        results = await process_with_retries(
            records_with_attempts,
            self._update_single_batch,
            batch_size,
            max_attempts,
            should_retry,
            max_concurrent_batches,
            on_result,
            self.MAX_RECORDS_UPDATE,
            actual_sobject_type,
            all_or_none,
            api_version,
        )

        logger.info(
            f"Update operation completed for {len(records)} {actual_sobject_type} records"
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
        :param sobject_type: Salesforce object type - optional if records have attributes
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param api_version: API version to use
        :returns: List of results for each record
        :raises httpx.HTTPError: If the request fails
        :raises ValueError: If records don't contain external_id_field or sobject_type is required but not provided
        """
        if len(records) > self.MAX_RECORDS_UPSERT:
            raise ValueError(
                f"Batch size ({len(records)}) exceeds maximum allowed ({self.MAX_RECORDS_UPSERT})"
            )

        actual_sobject_type, has_attributes = detect_record_type_and_sobject(
            records, sobject_type
        )

        validate_records_have_field(records, external_id_field, "upsert")

        base_url = self._get_collections_url(api_version)
        url = f"{base_url}/{actual_sobject_type}/{external_id_field}"

        payload = {
            "allOrNone": all_or_none,
            "records": prepare_records(records, actual_sobject_type, has_attributes),
        }

        response = await self._client.request("PATCH", url, json=payload)
        response.raise_for_status()

        return response.json()

    async def upsert(
        self,
        records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
        external_id_field: str,
        sobject_type: Optional[str] = None,
        all_or_none: bool = True,
        batch_size: Union[int, List[int]] = 200,
        max_concurrent_batches: Union[int, List[int]] = 5,
        api_version: Optional[str] = None,
        on_result: Optional[ResultCallback] = None,
        max_attempts: int = 1,
        should_retry: Optional[ShouldRetryCallback] = None,
    ) -> CollectionUpsertResponse:
        """
        Upsert multiple records using the Collections API.
        Automatically handles batching for large datasets with optional retry logic.

        Order preservation: Results are returned in the same order as input records,
        enabling error correlation by index (result[i] corresponds to records[i]).

        :param records: List of records to upsert (must include external_id_field)
        :param external_id_field: External ID field name for upsert operations
        :param sobject_type: Salesforce object type - optional if records have attributes
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param batch_size: Batch size (int for same size, or list of ints per attempt). Max 200.
        :param max_concurrent_batches: Maximum number of concurrent batch operations
        :param api_version: API version to use
        :param on_result: Optional async callback invoked after each batch completes with results
        :param max_attempts: Maximum number of attempts per record (default: 1, no retries)
        :param should_retry: Optional callback to determine if a failed record should be retried
        :returns: List of results for each record, in same order as input
        :raises httpx.HTTPError: If the request fails
        :raises ValueError: If records don't contain external_id_field or sobject_type is required but not provided
        """
        if not records:
            return []

        actual_sobject_type, has_attributes = detect_record_type_and_sobject(
            records, sobject_type
        )

        logger.info(
            f"Upserting {len(records)} {actual_sobject_type} records via Collections API "
            f"using {external_id_field} (batch_size={batch_size}, max_concurrent={max_concurrent_batches})"
        )

        records_with_attempts = [
            RecordWithAttempt(record, i, 1) for i, record in enumerate(records)
        ]

        results = await process_with_retries(
            records_with_attempts,
            self._upsert_single_batch,
            batch_size,
            max_attempts,
            should_retry,
            max_concurrent_batches,
            on_result,
            self.MAX_RECORDS_UPSERT,
            external_id_field,
            actual_sobject_type,
            all_or_none,
            api_version,
        )

        logger.info(
            f"Upsert operation completed for {len(records)} {actual_sobject_type} records"
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
        params = {"ids": ",".join(record_ids), "allOrNone": str(all_or_none).lower()}

        response = await self._client.request("DELETE", url, params=params)
        response.raise_for_status()

        return response.json()

    async def delete(
        self,
        record_ids: List[str],
        all_or_none: bool = True,
        batch_size: Union[int, List[int]] = 200,
        max_concurrent_batches: Union[int, List[int]] = 5,
        api_version: Optional[str] = None,
        on_result: Optional[ResultCallback] = None,
        max_attempts: int = 1,
        should_retry: Optional[ShouldRetryCallback] = None,
    ) -> CollectionDeleteResponse:
        """
        Delete multiple records using the Collections API.
        Automatically handles batching for large datasets with optional retry logic.

        Order preservation: Results are returned in the same order as input record_ids,
        enabling error correlation by index (result[i] corresponds to record_ids[i]).

        :param record_ids: List of record IDs to delete
        :param all_or_none: If True, all records must succeed or all changes are rolled back
        :param batch_size: Batch size (int for same size, or list of ints per attempt). Max 200.
        :param max_concurrent_batches: Maximum number of concurrent batch operations
        :param api_version: API version to use
        :param on_result: Optional async callback invoked after each batch completes with results
        :param max_attempts: Maximum number of attempts per record (default: 1, no retries)
        :param should_retry: Optional callback to determine if a failed record should be retried
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

        records_with_attempts = [
            RecordWithAttempt(record_id, i, 1) for i, record_id in enumerate(record_ids)
        ]

        results = await process_with_retries(
            records_with_attempts,
            self._delete_single_batch,
            batch_size,
            max_attempts,
            should_retry,
            max_concurrent_batches,
            on_result,
            self.MAX_RECORDS_DELETE,
            all_or_none,
            api_version,
        )

        logger.info(f"Delete operation completed for {len(record_ids)} records")
        return results
