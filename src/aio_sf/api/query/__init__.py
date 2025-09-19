"""Salesforce Query API module."""

from .client import QueryAPI, QueryResult
from .types import (
    QueryResponse,
    QueryAllResponse,
    QueryMoreResponse,
    QueryErrorResponse,
)

__all__ = [
    "QueryAPI",
    "QueryResult",
    "QueryResponse",
    "QueryAllResponse",
    "QueryMoreResponse",
    "QueryErrorResponse",
]
