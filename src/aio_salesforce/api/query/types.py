"""TypedDict definitions for Salesforce Query API responses."""

from typing import TypedDict, List, Dict, Any, Optional


class QueryResponse(TypedDict):
    """Response from a SOQL query."""

    totalSize: int
    done: bool
    records: List[Dict[str, Any]]
    nextRecordsUrl: Optional[str]


class QueryAllResponse(TypedDict):
    """Response from a SOQL QueryAll query (includes deleted records)."""

    totalSize: int
    done: bool
    records: List[Dict[str, Any]]
    nextRecordsUrl: Optional[str]


class QueryMoreResponse(TypedDict):
    """Response from a QueryMore request."""

    totalSize: int
    done: bool
    records: List[Dict[str, Any]]
    nextRecordsUrl: Optional[str]


class QueryErrorResponse(TypedDict):
    """Error response from Query API."""

    message: str
    errorCode: str
    fields: Optional[List[str]]
