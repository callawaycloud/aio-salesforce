"""Unit tests for API client modules."""

import pytest
from unittest.mock import AsyncMock, ANY
from aio_sf.api.describe import DescribeAPI
from aio_sf.api.query import QueryAPI
from aio_sf.api.collections import CollectionsAPI


class TestDescribeAPI:
    """Test DescribeAPI functionality."""

    @pytest.mark.asyncio
    async def test_list_sobjects(self, mock_client, mock_http_response):
        """Test listing SObjects."""
        client, mock_http_client = mock_client
        describe_api = DescribeAPI(client)

        # Mock response data
        mock_response = mock_http_response(
            {
                "sobjects": [
                    {"name": "Account", "label": "Account", "createable": True},
                    {"name": "Contact", "label": "Contact", "createable": True},
                ]
            }
        )
        # Configure the async mock to return our mock response
        mock_http_client.get = AsyncMock(return_value=mock_response)
        mock_http_client.request = AsyncMock(return_value=mock_response)

        sobjects = await describe_api.list_sobjects()

        assert len(sobjects) == 2
        assert sobjects[0]["name"] == "Account"
        assert sobjects[1]["name"] == "Contact"
        # Check that either get or request was called (the client might use either)
        assert mock_http_client.get.called or mock_http_client.request.called

    @pytest.mark.asyncio
    async def test_describe_sobject(self, mock_client, mock_http_response):
        """Test describing a specific SObject."""
        client, mock_http_client = mock_client
        describe_api = DescribeAPI(client)

        # Mock response data
        mock_response = mock_http_response(
            {
                "name": "Account",
                "label": "Account",
                "fields": [
                    {"name": "Id", "type": "id", "createable": False},
                    {"name": "Name", "type": "string", "createable": True},
                ],
            }
        )
        mock_http_client.get = AsyncMock(return_value=mock_response)
        mock_http_client.request = AsyncMock(return_value=mock_response)

        account_describe = await describe_api.sobject("Account")

        assert account_describe["name"] == "Account"
        assert len(account_describe["fields"]) == 2
        assert mock_http_client.get.called or mock_http_client.request.called

    @pytest.mark.asyncio
    async def test_get_limits(self, mock_client, mock_http_response):
        """Test getting organization limits."""
        client, mock_http_client = mock_client
        describe_api = DescribeAPI(client)

        # Mock response data
        mock_response = mock_http_response(
            {
                "DailyApiRequests": {"Max": 15000, "Remaining": 14500},
                "DataStorageMB": {"Max": 1024, "Remaining": 512},
            }
        )
        mock_http_client.get = AsyncMock(return_value=mock_response)
        mock_http_client.request = AsyncMock(return_value=mock_response)

        limits = await describe_api.get_limits()

        assert "DailyApiRequests" in limits
        assert limits["DailyApiRequests"]["Max"] == 15000
        assert mock_http_client.get.called or mock_http_client.request.called


class TestQueryAPI:
    """Test QueryAPI functionality."""

    @pytest.mark.asyncio
    async def test_soql_query(self, mock_client, mock_http_response):
        """Test SOQL query execution."""
        client, mock_http_client = mock_client
        query_api = QueryAPI(client)

        # Mock response data
        mock_response = mock_http_response(
            {
                "totalSize": 2,
                "done": True,
                "records": [
                    {"Id": "001000000000001", "Name": "Test Account 1"},
                    {"Id": "001000000000002", "Name": "Test Account 2"},
                ],
            }
        )
        mock_http_client.get = AsyncMock(return_value=mock_response)
        mock_http_client.request = AsyncMock(return_value=mock_response)

        query_result = await query_api.soql("SELECT Id, Name FROM Account LIMIT 2")

        records = []
        async for record in query_result:
            records.append(record)

        assert len(records) == 2
        assert records[0]["Name"] == "Test Account 1"
        assert records[1]["Name"] == "Test Account 2"
        assert mock_http_client.get.called or mock_http_client.request.called

    @pytest.mark.asyncio
    async def test_sosl_search(self, mock_client, mock_http_response):
        """Test SOSL search execution."""
        client, mock_http_client = mock_client
        query_api = QueryAPI(client)

        # Mock response data
        mock_response = mock_http_response(
            {"searchRecords": [{"Id": "001000000000001", "Name": "Test Account"}]}
        )
        mock_http_client.get = AsyncMock(return_value=mock_response)
        mock_http_client.request = AsyncMock(return_value=mock_response)

        results = await query_api.sosl(
            "FIND {Test} IN ALL FIELDS RETURNING Account(Id, Name)"
        )

        assert len(results) == 1
        assert results[0]["Name"] == "Test Account"
        assert mock_http_client.get.called or mock_http_client.request.called


class TestCollectionsAPI:
    """Test CollectionsAPI functionality."""

    @pytest.mark.asyncio
    async def test_insert_records(self, mock_client, mock_http_response):
        """Test record insertion."""
        client, mock_http_client = mock_client
        collections_api = CollectionsAPI(client)

        # Mock response data
        mock_response = mock_http_response([{"id": "001000000000001", "success": True}])
        mock_http_client.post = AsyncMock(return_value=mock_response)
        mock_http_client.request = AsyncMock(return_value=mock_response)

        records = [{"Name": "Test Account", "Type": "Customer"}]
        results = await collections_api.insert(records, "Account")

        assert len(results) == 1
        assert results[0]["success"] is True
        assert results[0]["id"] == "001000000000001"
        assert mock_http_client.post.called or mock_http_client.request.called

    @pytest.mark.asyncio
    async def test_update_records(self, mock_client, mock_http_response):
        """Test record updates."""
        client, mock_http_client = mock_client
        collections_api = CollectionsAPI(client)

        # Mock response data
        mock_response = mock_http_response([{"id": "001000000000001", "success": True}])
        mock_http_client.request = AsyncMock(return_value=mock_response)

        records = [{"Id": "001000000000001", "Name": "Updated Account"}]
        results = await collections_api.update(records, "Account")

        assert len(results) == 1
        assert results[0]["success"] is True
        assert mock_http_client.request.called

    @pytest.mark.asyncio
    async def test_delete_records(self, mock_client, mock_http_response):
        """Test record deletion."""
        client, mock_http_client = mock_client
        collections_api = CollectionsAPI(client)

        # Mock response data
        mock_response = mock_http_response([{"id": "001000000000001", "success": True}])
        mock_http_client.request = AsyncMock(return_value=mock_response)

        record_ids = ["001000000000001"]
        results = await collections_api.delete(record_ids)

        assert len(results) == 1
        assert results[0]["success"] is True
        assert mock_http_client.request.called
