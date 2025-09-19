"""
TypedDict definitions for Salesforce Collections API responses.
"""

from typing import Any, Dict, List, Optional, TypedDict, Union
from ..types import GenericSalesforceRecord


class CollectionRequest(TypedDict):
    """Base request structure for collection operations."""

    allOrNone: bool
    records: List[GenericSalesforceRecord]  # Records with attributes and data


class CollectionError(TypedDict):
    """Error information for failed collection operations."""

    statusCode: str
    message: str
    fields: List[str]


class CollectionResult(TypedDict):
    """Result for individual record in collection operation."""

    id: Optional[str]  # Present for successful operations, None for failures
    success: bool
    errors: List[CollectionError]


class CollectionResponse(TypedDict):
    """Response from collection operations."""

    # List of results, one per record in the request
    # The order matches the order of records in the request
    pass  # This will be a List[CollectionResult] but TypedDict doesn't support direct list inheritance


# Specific operation request types
class InsertCollectionRequest(CollectionRequest):
    """Request structure for insert operations."""

    pass


class UpdateCollectionRequest(CollectionRequest):
    """Request structure for update operations."""

    pass


class UpsertCollectionRequest(CollectionRequest):
    """Request structure for upsert operations."""

    pass


class DeleteCollectionRequest(TypedDict):
    """Request structure for delete operations (different from others)."""

    allOrNone: bool
    ids: List[str]


# Response types are just lists of CollectionResult
CollectionInsertResponse = List[CollectionResult]
CollectionUpdateResponse = List[CollectionResult]
CollectionUpsertResponse = List[CollectionResult]
CollectionDeleteResponse = List[CollectionResult]
