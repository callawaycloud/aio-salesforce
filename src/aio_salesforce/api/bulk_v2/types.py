"""
TypedDict definitions for Salesforce Bulk API v2 responses.
"""

from typing import Any, Dict, List, Optional, TypedDict


class BulkJobInfo(TypedDict):
    """Bulk job information."""

    id: str
    operation: str
    object: str
    createdById: str
    createdDate: str
    systemModstamp: str
    state: str
    concurrencyMode: str
    contentType: str
    apiVersion: str
    jobType: str
    lineEnding: str
    columnDelimiter: str


class BulkJobCreateRequest(TypedDict):
    """Request payload for creating a bulk job."""

    operation: str
    query: str
    contentType: str


class BulkJobStatus(TypedDict):
    """Bulk job status information."""

    id: str
    operation: str
    object: str
    createdById: str
    createdDate: str
    systemModstamp: str
    state: str
    concurrencyMode: str
    contentType: str
    apiVersion: str
    jobType: str
    lineEnding: str
    columnDelimiter: str
    numberBatchesQueued: int
    numberBatchesInProgress: int
    numberBatchesCompleted: int
    numberBatchesFailed: int
    numberBatchesTotal: int
    numberRequestsCompleted: int
    numberRequestsFailed: int
    numberRequestsTotal: int
    numberRecordsProcessed: int
    numberRecordsFailed: int
    numberRetries: int
    apiActiveProcessingTime: int
    apexProcessingTime: int
    totalProcessingTime: int


class BulkJobError(TypedDict):
    """Bulk job error information."""

    message: str
    errorCode: str
    fields: List[str]
