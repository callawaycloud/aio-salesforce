"""Salesforce Collections API module."""

from .client import CollectionsAPI
from .types import (
    CollectionError,
    CollectionRequest,
    CollectionResult,
    CollectionResponse,
    InsertCollectionRequest,
    UpdateCollectionRequest,
    UpsertCollectionRequest,
    DeleteCollectionRequest,
    CollectionInsertResponse,
    CollectionUpdateResponse,
    CollectionUpsertResponse,
    CollectionDeleteResponse,
)

__all__ = [
    "CollectionsAPI",
    "CollectionError",
    "CollectionRequest",
    "CollectionResult",
    "CollectionResponse",
    "InsertCollectionRequest",
    "UpdateCollectionRequest",
    "UpsertCollectionRequest",
    "DeleteCollectionRequest",
    "CollectionInsertResponse",
    "CollectionUpdateResponse",
    "CollectionUpsertResponse",
    "CollectionDeleteResponse",
]
