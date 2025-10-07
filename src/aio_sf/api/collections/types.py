"""
TypedDict definitions for Salesforce Collections API responses.
"""

from typing import List, Optional, TypedDict


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


# Response types are just lists of CollectionResult
CollectionInsertResponse = List[CollectionResult]
CollectionUpdateResponse = List[CollectionResult]
CollectionUpsertResponse = List[CollectionResult]
CollectionDeleteResponse = List[CollectionResult]
