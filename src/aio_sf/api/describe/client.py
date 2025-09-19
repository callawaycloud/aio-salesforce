"""
Salesforce Describe API methods.
"""

import logging
from typing import List, Optional, TYPE_CHECKING

from .types import (
    OrganizationInfo,
    OrganizationLimits,
    SObjectDescribe,
    SObjectInfo,
)

if TYPE_CHECKING:
    from ..client import SalesforceClient

logger = logging.getLogger(__name__)


class DescribeAPI:
    """Salesforce Describe API methods."""

    def __init__(self, client: "SalesforceClient"):
        self.client = client

    async def sobject(
        self, sobject_type: str, api_version: Optional[str] = None
    ) -> SObjectDescribe:
        """
        Get metadata for a Salesforce object.

        :param sobject_type: Name of the Salesforce object (e.g., 'Account', 'Contact')
        :param api_version: API version to use (defaults to client version)
        :returns: Object metadata dictionary
        """
        url = self.client.get_describe_url(sobject_type, api_version)
        response = await self.client.get(url)
        response.raise_for_status()
        return response.json()

    async def list_sobjects(
        self, api_version: Optional[str] = None
    ) -> List[SObjectInfo]:
        """
        List all available Salesforce objects.

        :param api_version: API version to use (defaults to client version)
        :returns: List of object metadata dictionaries
        """
        base_url = self.client.get_base_url(api_version)
        url = f"{base_url}/sobjects"
        response = await self.client.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("sobjects", [])

    async def get_limits(self, api_version: Optional[str] = None) -> OrganizationLimits:
        """
        Get organization limits.

        :param api_version: API version to use (defaults to client version)
        :returns: Organization limits dictionary
        """
        base_url = self.client.get_base_url(api_version)
        url = f"{base_url}/limits"
        response = await self.client.get(url)
        response.raise_for_status()
        return response.json()

    async def get_organization_info(
        self, api_version: Optional[str] = None
    ) -> OrganizationInfo:
        """
        Get organization information.

        :param api_version: API version to use (defaults to client version)
        :returns: Organization info dictionary
        """
        # Query the Organization object for basic org info
        base_url = self.client.get_base_url(api_version)
        url = f"{base_url}/query"
        params = {
            "q": "SELECT Id, Name, OrganizationType, InstanceName, IsSandbox FROM Organization LIMIT 1"
        }
        response = await self.client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        records = data.get("records", [])
        if records:
            return records[0]
        else:
            raise RuntimeError("Unable to retrieve organization information")
