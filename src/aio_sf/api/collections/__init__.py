"""Salesforce Collections API module."""

from .client import CollectionsAPI
from .batch import ProgressInfo, ProgressCallback
from .retry import ShouldRetryCallback, default_should_retry
from .types import (
    CollectionError,
    CollectionResult,
    CollectionInsertResponse,
    CollectionUpdateResponse,
    CollectionUpsertResponse,
    CollectionDeleteResponse,
)

__all__ = [
    "CollectionsAPI",
    "ProgressInfo",
    "ProgressCallback",
    "ShouldRetryCallback",
    "default_should_retry",
    "CollectionError",
    "CollectionResult",
    "CollectionInsertResponse",
    "CollectionUpdateResponse",
    "CollectionUpsertResponse",
    "CollectionDeleteResponse",
]
