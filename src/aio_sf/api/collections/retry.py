"""Retry logic and utilities for Collections API."""

import asyncio
import httpx
from typing import Any, Awaitable, Callable, List, Optional, Union

from .types import CollectionResult


# Type alias for should_retry callback
ShouldRetryCallback = Callable[
    [Any, Union[CollectionResult, Exception], int], Union[bool, Awaitable[bool]]
]


class RecordWithAttempt:
    """Track a record with its attempt count and original index."""

    def __init__(self, record: Any, original_index: int, attempt: int = 1):
        self.record = record
        self.original_index = original_index
        self.attempt = attempt


def default_should_retry(
    record: Any, result: Union[CollectionResult, Exception], attempt: int
) -> bool:
    """
    Default retry logic: retry on row lock errors and common transient errors.

    This is used when should_retry=None. It retries records that failed with:
    - HTTP/Network transient exceptions: Timeouts, connection errors, 5xx server errors
    - UNABLE_TO_LOCK_ROW: Concurrent access conflicts
    - REQUEST_LIMIT_EXCEEDED: API rate limits (usually temporary)
    - SERVER_UNAVAILABLE: Temporary server issues

    :param record: The record that failed
    :param result: Either a CollectionResult (parsed response) or an Exception (HTTP/network error)
    :param attempt: Current attempt number (1-indexed)
    :returns: True if the record should be retried
    """
    if isinstance(result, Exception):
        return _should_retry_exception(result)

    # Check for specific Salesforce error codes
    return _should_retry_salesforce_errors(result)


def _should_retry_exception(exception: Exception) -> bool:
    """Check if an HTTP/network exception is retryable."""
    # Retry on transient network errors
    if isinstance(exception, (httpx.TimeoutException, httpx.ConnectError)):
        return True

    # Retry on 5xx server errors, but not 4xx client errors
    if isinstance(exception, httpx.HTTPStatusError):
        return 500 <= exception.response.status_code < 600

    return False


def _should_retry_salesforce_errors(result: CollectionResult) -> bool:
    """Check if a Salesforce error result is retryable."""
    errors = result.get("errors", [])

    retryable_error_codes = {
        "UNABLE_TO_LOCK_ROW",  # Concurrent access conflicts
        "REQUEST_LIMIT_EXCEEDED",  # API rate limits
        "SERVER_UNAVAILABLE",  # Temporary server issues
    }

    for error in errors:
        error_code = error.get("statusCode", "")
        if error_code in retryable_error_codes:
            return True

    return False


async def should_retry_record(
    record: Any,
    result: Union[CollectionResult, Exception],
    attempt: int,
    callback: Optional[ShouldRetryCallback],
) -> bool:
    """
    Determine if a record should be retried based on the result and attempt.

    :param record: The record that was attempted
    :param result: Either a CollectionResult or an Exception if the HTTP request failed
    :param attempt: The current attempt number
    :param callback: Optional callback to determine retry
    :returns: True if the record should be retried, False otherwise
    """
    # If record succeeded, don't retry
    if not isinstance(result, Exception) and result.get("success", False):
        return False

    # Use callback or default logic
    retry_func = callback if callback is not None else default_should_retry
    retry_result = retry_func(record, result, attempt)

    # Handle both sync and async callbacks
    if asyncio.iscoroutine(retry_result):
        return await retry_result
    return bool(retry_result)


def convert_exception_to_result(exception: Exception) -> CollectionResult:
    """Convert an HTTP/network exception to a CollectionResult format."""
    return {
        "id": None,
        "success": False,
        "errors": [
            {
                "statusCode": "HTTP_ERROR",
                "message": f"{type(exception).__name__}: {str(exception)}",
                "fields": [],
            }
        ],
    }


def get_value_for_attempt(attempt: int, values: Union[int, List[int]]) -> int:
    """
    Get a value for a given attempt number from either a single value or list.

    :param attempt: The attempt number (1-indexed)
    :param values: Either a single int or list of ints per attempt
    :returns: The value to use for this attempt
    """
    if isinstance(values, int):
        return values

    # Attempt is 1-indexed, list is 0-indexed
    index = attempt - 1
    if index < len(values):
        return values[index]
    else:
        # Return the last value if we've exceeded the list length
        return values[-1]
