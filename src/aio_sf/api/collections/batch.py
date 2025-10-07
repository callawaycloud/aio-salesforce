"""Batch processing and concurrency management for Collections API."""

import asyncio
import logging
from typing import Any, Awaitable, Callable, List, Optional, TypedDict, Union

from .retry import (
    RecordWithAttempt,
    ShouldRetryCallback,
    convert_exception_to_result,
    get_value_for_attempt,
    should_retry_record,
)
from .types import CollectionResult


logger = logging.getLogger(__name__)


class ProgressInfo(TypedDict):
    """Progress information for batch operations."""

    batch_index: int  # Current batch number (0-indexed)
    total_batches: int  # Total number of batches
    records_processed: int  # Number of records processed so far
    total_records: int  # Total number of records to process
    batch_size: int  # Size of the current batch
    retry_count: int  # Number of records being retried in this operation


# Type alias for progress callback
ProgressCallback = Callable[[ProgressInfo], Awaitable[None]]


def split_into_batches(
    items: List[Any], batch_size: int, max_limit: int
) -> List[List[Any]]:
    """
    Split a list of items into batches of specified size.

    :param items: List of items to split
    :param batch_size: Maximum size of each batch
    :param max_limit: Maximum allowed batch size for the operation
    :returns: List of batches
    :raises ValueError: If batch_size is invalid
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


async def process_batches_concurrently(
    batches: List[Any],
    operation_func,
    max_concurrent_batches: int,
    total_records: int,
    on_batch_complete: Optional[ProgressCallback] = None,
    *args,
    **kwargs,
) -> List[Any]:
    """
    Process batches concurrently with a limit on concurrent operations.

    Order preservation: Results are returned in the same order as input batches,
    regardless of which batch completes first.

    :param batches: List of batches to process
    :param operation_func: Function to call for each batch
    :param max_concurrent_batches: Maximum number of concurrent batch operations
    :param total_records: Total number of records being processed
    :param on_batch_complete: Optional callback invoked after each batch completes
    :param args: Additional positional arguments for operation_func
    :param kwargs: Additional keyword arguments for operation_func
    :returns: List of results from all batches in the same order as input
    :raises ValueError: If max_concurrent_batches is invalid
    """
    if max_concurrent_batches <= 0:
        raise ValueError("max_concurrent_batches must be greater than 0")

    semaphore = asyncio.Semaphore(max_concurrent_batches)
    total_batches = len(batches)
    callback_lock = asyncio.Lock() if on_batch_complete else None
    records_processed = 0

    async def process_batch_with_semaphore(batch_index: int, batch):
        nonlocal records_processed
        async with semaphore:
            try:
                result = await operation_func(batch, *args, **kwargs)
            except Exception as e:
                # HTTP/network error - return the exception for each record
                logger.warning(
                    f"Batch {batch_index} failed with exception: {type(e).__name__}: {e}"
                )
                result = [e for _ in range(len(batch))]

            # Invoke progress callback if provided
            if on_batch_complete and callback_lock:
                batch_size = len(batch)
                async with callback_lock:
                    records_processed += batch_size
                    progress_info: ProgressInfo = {
                        "batch_index": batch_index,
                        "total_batches": total_batches,
                        "records_processed": records_processed,
                        "total_records": total_records,
                        "batch_size": batch_size,
                        "retry_count": 0,  # Set by wrapper in process_with_retries
                    }
                    await on_batch_complete(progress_info)

            return result

    # Process all batches concurrently with semaphore limiting concurrency
    tasks = [process_batch_with_semaphore(i, batch) for i, batch in enumerate(batches)]
    # asyncio.gather() preserves order
    results = await asyncio.gather(*tasks)

    # Flatten results from all batches, maintaining order
    flattened_results = []
    for batch_result in results:
        flattened_results.extend(batch_result)

    return flattened_results


async def process_with_retries(
    records_with_attempts: List[RecordWithAttempt],
    operation_func,
    batch_size: Union[int, List[int]],
    max_attempts: int,
    should_retry_callback: Optional[ShouldRetryCallback],
    max_concurrent_batches: Union[int, List[int]],
    on_batch_complete: Optional[ProgressCallback],
    max_limit: int,
    *args,
    **kwargs,
) -> List[CollectionResult]:
    """
    Process records with retry logic, shrinking batch sizes, and scaling concurrency.

    :param records_with_attempts: List of records with attempt tracking
    :param operation_func: The single-batch operation function to call
    :param batch_size: Batch size (int or list of ints per attempt)
    :param max_attempts: Maximum number of attempts per record
    :param should_retry_callback: Optional callback to determine if record should be retried
    :param max_concurrent_batches: Maximum concurrent batches (int or list of ints per attempt)
    :param on_batch_complete: Progress callback
    :param max_limit: Maximum batch size limit for the operation
    :param args: Additional args for operation_func
    :param kwargs: Additional kwargs for operation_func
    :returns: List of results in order of original input
    """
    # Initialize result array with None placeholders
    max_index = max(r.original_index for r in records_with_attempts)
    final_results: List[Optional[CollectionResult]] = [None] * (max_index + 1)

    current_records = records_with_attempts
    total_retry_count = 0

    while current_records:
        current_attempt = current_records[0].attempt
        current_batch_size = min(
            get_value_for_attempt(current_attempt, batch_size), max_limit
        )
        current_concurrency = get_value_for_attempt(
            current_attempt, max_concurrent_batches
        )

        logger.debug(
            f"Processing {len(current_records)} records on attempt {current_attempt} "
            f"with batch_size={current_batch_size}, concurrency={current_concurrency}"
        )

        # Extract records and split into batches
        records_to_process = [r.record for r in current_records]
        batches = split_into_batches(records_to_process, current_batch_size, max_limit)

        # Wrap progress callback to include retry count
        wrapped_callback = None
        if on_batch_complete:

            async def progress_wrapper(progress: ProgressInfo):
                progress["retry_count"] = total_retry_count
                await on_batch_complete(progress)

            wrapped_callback = progress_wrapper

        # Process batches with current concurrency level
        batch_results = await process_batches_concurrently(
            batches,
            operation_func,
            current_concurrency,
            len(records_to_process),
            wrapped_callback,
            *args,
            **kwargs,
        )

        # Process results and determine retries
        records_to_retry = await _collect_records_for_retry(
            current_records,
            batch_results,
            max_attempts,
            should_retry_callback,
            final_results,
        )

        if records_to_retry:
            logger.info(
                f"Retrying {len(records_to_retry)} failed records "
                f"(attempt {records_to_retry[0].attempt})"
            )
            total_retry_count += len(records_to_retry)

        current_records = records_to_retry

    # Return results (all should be non-None at this point)
    return [r for r in final_results if r is not None]


async def _collect_records_for_retry(
    current_records: List[RecordWithAttempt],
    batch_results: List[Union[CollectionResult, Exception]],
    max_attempts: int,
    should_retry_callback: Optional[ShouldRetryCallback],
    final_results: List[Optional[CollectionResult]],
) -> List[RecordWithAttempt]:
    """Process results and collect records that should be retried."""
    records_to_retry: List[RecordWithAttempt] = []

    for record_wrapper, result in zip(current_records, batch_results):
        original_index = record_wrapper.original_index

        if isinstance(result, Exception):
            # HTTP/network error
            await _handle_exception_result(
                record_wrapper,
                result,
                max_attempts,
                should_retry_callback,
                records_to_retry,
                final_results,
                original_index,
            )
        elif result.get("success", False):
            # Success - store the result
            final_results[original_index] = result
        else:
            # Failed CollectionResult
            await _handle_failed_result(
                record_wrapper,
                result,
                max_attempts,
                should_retry_callback,
                records_to_retry,
                final_results,
                original_index,
            )

    return records_to_retry


async def _handle_exception_result(
    record_wrapper: RecordWithAttempt,
    exception: Exception,
    max_attempts: int,
    should_retry_callback: Optional[ShouldRetryCallback],
    records_to_retry: List[RecordWithAttempt],
    final_results: List[Optional[CollectionResult]],
    original_index: int,
) -> None:
    """Handle an exception result - either retry or convert to error result."""
    can_retry = record_wrapper.attempt < max_attempts
    if can_retry and await should_retry_record(
        record_wrapper.record,
        exception,
        record_wrapper.attempt,
        should_retry_callback,
    ):
        records_to_retry.append(
            RecordWithAttempt(
                record_wrapper.record,
                original_index,
                record_wrapper.attempt + 1,
            )
        )
    else:
        # No more retries - convert Exception to CollectionResult format
        final_results[original_index] = convert_exception_to_result(exception)


async def _handle_failed_result(
    record_wrapper: RecordWithAttempt,
    result: CollectionResult,
    max_attempts: int,
    should_retry_callback: Optional[ShouldRetryCallback],
    records_to_retry: List[RecordWithAttempt],
    final_results: List[Optional[CollectionResult]],
    original_index: int,
) -> None:
    """Handle a failed CollectionResult - either retry or store the failure."""
    can_retry = record_wrapper.attempt < max_attempts
    if can_retry and await should_retry_record(
        record_wrapper.record,
        result,
        record_wrapper.attempt,
        should_retry_callback,
    ):
        records_to_retry.append(
            RecordWithAttempt(
                record_wrapper.record,
                original_index,
                record_wrapper.attempt + 1,
            )
        )
    else:
        # No more retries - store the failed result
        final_results[original_index] = result
