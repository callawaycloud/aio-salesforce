"""SFDX CLI authentication strategy."""

import asyncio
import json
import logging
import re
import time
from typing import Dict, Any

import httpx

from .base import AuthStrategy, SalesforceAuthError

logger = logging.getLogger(__name__)


class SfdxCliAuth(AuthStrategy):
    """SFDX CLI authentication strategy using 'sf org display' / 'sf org auth show-access-token'."""

    def __init__(self, username_or_alias: str):
        """
        Initialize SFDX CLI authentication strategy.

        :param username_or_alias: Salesforce org username or alias configured in SFDX CLI
        """
        # We'll get the instance URL from the CLI command
        super().__init__(None)
        self.username_or_alias = username_or_alias
        self._ansi_escape = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")

    async def authenticate(self, http_client: httpx.AsyncClient) -> str:
        """Get access token from SFDX CLI."""
        logger.info(
            f"Getting Salesforce access token from SFDX CLI for {self.username_or_alias}"
        )

        try:
            # Execute the SFDX command asynchronously
            token_info = await self._execute_sfdx_command()

            # Extract token and instance URL
            self.access_token = token_info["accessToken"]
            self.instance_url = token_info["instanceUrl"].rstrip("/")

            # SFDX tokens typically have a reasonable expiration time
            # We'll set a conservative 1-hour expiration to trigger refresh
            self.expires_at = int(time.time()) + 3600

            logger.info("Successfully obtained Salesforce access token from SFDX CLI")
            return self.access_token

        except Exception as e:
            logger.error(f"Error getting access token from SFDX CLI: {e}")
            raise SalesforceAuthError(f"SFDX CLI authentication failed: {e}")

    async def refresh_if_needed(self, http_client: httpx.AsyncClient) -> str:
        """Refresh token if needed (re-execute CLI command)."""
        if self.access_token and not self.is_token_expired():
            return self.access_token
        return await self.authenticate(http_client)

    def can_refresh(self) -> bool:
        """SFDX CLI can always re-authenticate."""
        return True

    async def _execute_sfdx_command(self) -> Dict[str, Any]:
        """
        Execute the SFDX CLI commands asynchronously and parse the result.

        ``sf org display`` provides the instance URL (and refreshes the session).
        Newer CLI versions redact the access token in that output, in which case
        it is fetched with ``sf org auth show-access-token``.

        :returns: Dictionary containing accessToken and instanceUrl
        :raises: SalesforceAuthError if command fails or output is invalid
        """
        try:
            result = await self._run_sf_json(
                f"sf org display -o {self.username_or_alias} --json"
            )

            if "instanceUrl" not in result:
                raise SalesforceAuthError(
                    "SFDX CLI response missing 'instanceUrl' field"
                )

            if _is_redacted(result.get("accessToken")):
                token_result = await self._run_sf_json(
                    f"sf org auth show-access-token -o {self.username_or_alias} --json"
                )
                result["accessToken"] = token_result.get("accessToken")

            if _is_redacted(result.get("accessToken")):
                raise SalesforceAuthError(
                    "SFDX CLI did not return a usable 'accessToken'"
                )

            return result

        except asyncio.TimeoutError:
            raise SalesforceAuthError("SFDX CLI command timed out")
        except FileNotFoundError:
            raise SalesforceAuthError(
                "SFDX CLI not found. Please ensure Salesforce CLI is installed and in PATH"
            )

    async def _run_sf_json(self, cmd: str) -> Dict[str, Any]:
        """Run an ``sf ... --json`` command and return its ``result`` object."""
        process = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_msg = stderr.decode().strip() if stderr else "Unknown error"
            raise SalesforceAuthError(f"SFDX command failed: {error_msg}")

        # Clean ANSI escape sequences from output
        cleaned_output = self._ansi_escape.sub("", stdout.decode())

        try:
            sfdx_info = json.loads(cleaned_output)
        except json.JSONDecodeError as e:
            raise SalesforceAuthError(f"Invalid JSON response from SFDX CLI: {e}")

        if "result" not in sfdx_info:
            raise SalesforceAuthError("SFDX CLI response missing 'result' field")

        return sfdx_info["result"]


def _is_redacted(token: Any) -> bool:
    """True when the CLI returned no token or a redaction placeholder."""
    return not token or "REDACTED" in str(token)
