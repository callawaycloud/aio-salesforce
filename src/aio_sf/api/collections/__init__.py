"""Salesforce Collections API module."""

from .client import CollectionsAPI
from .batch import ResultInfo, ResultCallback
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
    "ResultInfo",
    "ResultCallback",
    "ShouldRetryCallback",
    "default_should_retry",
    "CollectionError",
    "CollectionResult",
    "CollectionInsertResponse",
    "CollectionUpdateResponse",
    "CollectionUpsertResponse",
    "CollectionDeleteResponse",
]
