"""
Salesforce Describe API.
"""

from .client import DescribeAPI
from .types import (
    FieldInfo,
    LimitInfo,
    OrganizationInfo,
    OrganizationLimits,
    PicklistValue,
    RecordTypeInfo,
    SObjectDescribe,
    SObjectInfo,
    SalesforceAttributes,
)

__all__ = [
    # API Client
    "DescribeAPI",
    # Types
    "FieldInfo",
    "LimitInfo",
    "OrganizationInfo",
    "OrganizationLimits",
    "PicklistValue",
    "RecordTypeInfo",
    "SObjectDescribe",
    "SObjectInfo",
    "SalesforceAttributes",
]
