"""
Salesforce Bulk API v2 methods.
"""

import asyncio
import logging
import time
from typing import Optional, Tuple, TYPE_CHECKING

from .types import (
    BulkJobCreateRequest,
    BulkJobInfo,
    BulkJobStatus,
)

if TYPE_CHECKING:
    from ..client import SalesforceClient

logger = logging.getLogger(__name__)


class BulkV2API:
    """Salesforce Bulk API v2 methods."""

    def __init__(self, client: "SalesforceClient"):
        self.client = client

    def _get_base_url(self, api_version: Optional[str] = None) -> str:
        """Get the base URL for Bulk API v2 requests."""
        return self.client.get_base_url(api_version)

    def _get_job_url(self, job_id: str, api_version: Optional[str] = None) -> str:
        """Get the URL for a specific bulk job."""
        base_url = self._get_base_url(api_version)
        return f"{base_url}/jobs/query/{job_id}"

    def _get_job_results_url(
        self, job_id: str, api_version: Optional[str] = None
    ) -> str:
        """Get the URL for fetching bulk job results."""
        base_url = self._get_base_url(api_version)
        return f"{base_url}/jobs/query/{job_id}/results"

    def _get_jobs_url(self, api_version: Optional[str] = None) -> str:
        """Get the URL for creating bulk jobs."""
        base_url = self._get_base_url(api_version)
        return f"{base_url}/jobs/query"

    async def create_job(
        self,
        soql_query: str,
        all_rows: bool = False,
        api_version: Optional[str] = None,
    ) -> BulkJobInfo:
        """
        Create a new bulk query job.

        :param soql_query: The SOQL query to execute
        :param all_rows: If True, includes deleted and archived records (queryAll)
        :param api_version: API version to use (defaults to client version)
        :returns: Job information
        """
        job_url = self._get_jobs_url(api_version)
        job_data: BulkJobCreateRequest = {
            "operation": "queryAll" if all_rows else "query",
            "query": soql_query,
            "contentType": "CSV",
        }

        response = await self.client.post(job_url, json=job_data)
        response.raise_for_status()
        job_info = response.json()

        logger.info(
            f"Created bulk job {job_info['id']} for query: {soql_query[:100]}..."
        )
        return job_info

    async def get_job_status(
        self,
        job_id: str,
        api_version: Optional[str] = None,
    ) -> BulkJobStatus:
        """
        Get the status of a bulk job.

        :param job_id: The job ID
        :param api_version: API version to use (defaults to client version)
        :returns: Job status information
        """
        status_url = self._get_job_url(job_id, api_version)
        response = await self.client.get(status_url)
        response.raise_for_status()
        return response.json()

    async def get_job_results(
        self,
        job_id: str,
        locator: Optional[str] = None,
        max_records: int = 10000,
        api_version: Optional[str] = None,
    ) -> Tuple[str, Optional[str]]:
        """
        Get results from a completed bulk job.

        :param job_id: The job ID
        :param locator: Query locator for pagination (optional)
        :param max_records: Maximum number of records to fetch
        :param api_version: API version to use (defaults to client version)
        :returns: Tuple of (CSV response text, next locator or None)
        """
        results_url = self._get_job_results_url(job_id, api_version)
        params = {"maxRecords": max_records}
        if locator:
            params["locator"] = locator

        response = await self.client.get(results_url, params=params)
        response.raise_for_status()

        # Get next locator from headers
        next_locator = response.headers.get("Sforce-Locator")
        if next_locator == "null":
            next_locator = None

        return response.text, next_locator

    async def wait_for_job_completion(
        self,
        job_id: str,
        poll_interval: int = 5,
        timeout: Optional[int] = None,
        api_version: Optional[str] = None,
    ) -> BulkJobStatus:
        """
        Wait for a bulk job to complete.

        :param job_id: The job ID to monitor
        :param poll_interval: Time in seconds between status checks
        :param timeout: Maximum time to wait (None for no timeout)
        :param api_version: API version to use (defaults to client version)
        :returns: Final job status
        :raises TimeoutError: If job doesn't complete within timeout
        :raises Exception: If job fails
        """
        logger.info(f"Waiting for job {job_id} to complete...")
        start_time = time.time()

        while True:
            job_status = await self.get_job_status(job_id, api_version)
            state = job_status["state"]

            if state == "JobComplete":
                total_records = job_status.get("numberRecordsProcessed", 0)
                logger.info(
                    f"Job {job_id} completed successfully. Total records: {total_records}"
                )
                return job_status
            elif state == "Failed":
                raise Exception(f"Job {job_id} failed: {job_status}")
            else:
                # Check timeout
                if timeout and (time.time() - start_time) > timeout:
                    raise TimeoutError(
                        f"Job {job_id} did not complete within {timeout} seconds"
                    )

                # Job is still running
                logger.debug(
                    f"Job {job_id} state: {state}, waiting {poll_interval}s..."
                )
                await asyncio.sleep(poll_interval)

    async def execute_query(
        self,
        soql_query: str,
        all_rows: bool = False,
        poll_interval: int = 5,
        timeout: Optional[int] = None,
        api_version: Optional[str] = None,
    ) -> BulkJobStatus:
        """
        Execute a SOQL query via Bulk API v2 and wait for completion.

        This is a convenience method that creates a job and waits for it to complete.

        :param soql_query: The SOQL query to execute
        :param all_rows: If True, includes deleted and archived records
        :param poll_interval: Time in seconds between status checks
        :param timeout: Maximum time to wait (None for no timeout)
        :param api_version: API version to use (defaults to client version)
        :returns: Final job status
        """
        # Create the job
        job_info = await self.create_job(soql_query, all_rows, api_version)
        job_id = job_info["id"]

        # Wait for completion
        return await self.wait_for_job_completion(
            job_id, poll_interval, timeout, api_version
        )
