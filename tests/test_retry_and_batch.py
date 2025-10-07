"""Simplified comprehensive tests for retry and batch handling."""

import pytest
import asyncio
from aio_sf.api.collections import CollectionsAPI, ResultInfo


class MockClient:
    """Simple mock client for testing."""

    def __init__(self):
        self.post_calls = []
        self.request_calls = []
        self.responses = []
        self.response_index = 0

    def set_responses(self, responses):
        """Set list of responses to return."""
        self.responses = responses
        self.response_index = 0

    async def post(self, url, json=None):
        """Mock POST method."""
        self.post_calls.append({"url": url, "json": json})
        response = self.responses[self.response_index]
        self.response_index += 1
        return response

    async def request(self, method, url, json=None, params=None):
        """Mock request method."""
        self.request_calls.append(
            {"method": method, "url": url, "json": json, "params": params}
        )
        response = self.responses[self.response_index]
        self.response_index += 1
        return response

    def get_base_url(self, api_version=None):
        """Mock get_base_url."""
        return "https://test.salesforce.com/services/data/v60.0"


class MockResponse:
    """Mock HTTP response."""

    def __init__(self, data):
        self.data = data

    def json(self):
        """Return JSON data."""
        return self.data

    def raise_for_status(self):
        """Mock raise_for_status."""
        pass


@pytest.fixture
def client():
    """Provide a mock client."""
    return MockClient()


class TestOrderPreservation:
    """Test that order is preserved across batches and retries."""

    @pytest.mark.asyncio
    async def test_batch_order_preserved(self, client):
        """Test that large datasets maintain order across multiple batches."""
        collections_api = CollectionsAPI(client)

        # 300 records will be split into 2 batches of 200 and 100
        records = [{"Name": f"Account {i}"} for i in range(300)]

        # Mock responses for each batch
        batch1_response = MockResponse(
            [{"id": f"001{i:05d}", "success": True} for i in range(200)]
        )
        batch2_response = MockResponse(
            [{"id": f"001{i:05d}", "success": True} for i in range(200, 300)]
        )

        client.set_responses([batch1_response, batch2_response])

        results = await collections_api.insert(
            records, sobject_type="Account", batch_size=200
        )

        # Verify order is maintained
        assert len(results) == 300
        for i, result in enumerate(results):
            assert result["id"] == f"001{i:05d}"
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_retry_order_preserved(self, client):
        """Test that order is preserved when some records are retried."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": f"Account {i}"} for i in range(10)]

        # First attempt: even indices succeed, odd fail
        first_batch = []
        for i in range(10):
            if i % 2 == 0:
                first_batch.append({"id": f"001{i:05d}", "success": True})
            else:
                first_batch.append(
                    {
                        "success": False,
                        "errors": [
                            {"statusCode": "UNABLE_TO_LOCK_ROW", "message": "Lock"}
                        ],
                    }
                )

        # Second attempt: only the 5 failed records, all succeed
        second_batch = [{"id": f"001{i:05d}", "success": True} for i in range(1, 10, 2)]

        client.set_responses([MockResponse(first_batch), MockResponse(second_batch)])

        results = await collections_api.insert(
            records,
            sobject_type="Account",
            batch_size=200,
            max_attempts=2,
        )

        # All should succeed and be in correct order
        assert len(results) == 10
        for i, result in enumerate(results):
            assert result["success"], f"Record {i} failed"
            assert result["id"] == f"001{i:05d}"


class TestConcurrency:
    """Test concurrent batch processing."""

    @pytest.mark.asyncio
    async def test_concurrent_execution(self, client):
        """Test that batches execute concurrently."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": f"Account {i}"} for i in range(400)]

        # Track when each batch starts
        batch_start_times = []

        async def mock_post_with_timing(*args, **kwargs):
            start_time = asyncio.get_event_loop().time()
            batch_start_times.append(start_time)
            await asyncio.sleep(0.01)  # Simulate network delay

            # Return appropriate response based on call index
            idx = len(batch_start_times) - 1
            start = idx * 200
            end = min(start + 200, 400)
            return MockResponse(
                [{"id": f"001{i:05d}", "success": True} for i in range(start, end)]
            )

        client.post = mock_post_with_timing

        results = await collections_api.insert(
            records, sobject_type="Account", batch_size=200, max_concurrent_batches=2
        )

        assert len(results) == 400
        # Both batches should start close together (concurrent)
        assert len(batch_start_times) == 2
        time_diff = abs(batch_start_times[1] - batch_start_times[0])
        assert time_diff < 0.005, "Batches should start concurrently"


class TestRetryLogic:
    """Test retry functionality."""

    @pytest.mark.asyncio
    async def test_default_retry_on_lock_errors(self, client):
        """Test that row lock errors are retried by default."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": "Account 1"}]

        # First attempt: UNABLE_TO_LOCK_ROW error
        # Second attempt: success
        client.set_responses(
            [
                MockResponse(
                    [
                        {
                            "success": False,
                            "errors": [
                                {
                                    "statusCode": "UNABLE_TO_LOCK_ROW",
                                    "message": "unable to obtain exclusive access to this record",
                                    "fields": [],
                                }
                            ],
                        }
                    ]
                ),
                MockResponse([{"id": "001000001", "success": True}]),
            ]
        )

        # Don't provide should_retry - should use default
        results = await collections_api.insert(
            records, sobject_type="Account", batch_size=200, max_attempts=2
        )

        assert len(results) == 1
        assert results[0]["success"]
        # Should have made 2 POST calls (initial + retry)
        assert len(client.post_calls) == 2

    @pytest.mark.asyncio
    async def test_default_no_retry_on_validation_errors(self, client):
        """Test that validation errors are NOT retried by default."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": "Account 1"}]

        # Validation error - should NOT be retried
        client.set_responses(
            [
                MockResponse(
                    [
                        {
                            "success": False,
                            "errors": [
                                {
                                    "statusCode": "REQUIRED_FIELD_MISSING",
                                    "message": "Required fields are missing: [Type]",
                                    "fields": ["Type"],
                                }
                            ],
                        }
                    ]
                )
            ]
        )

        # Don't provide should_retry - should use default
        results = await collections_api.insert(
            records, sobject_type="Account", batch_size=200, max_attempts=5
        )

        assert len(results) == 1
        assert not results[0]["success"]
        # Should have made only 1 POST call (no retry for validation errors)
        assert len(client.post_calls) == 1

    @pytest.mark.asyncio
    async def test_default_retry_on_rate_limit(self, client):
        """Test that rate limit errors are retried by default."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": "Account 1"}]

        client.set_responses(
            [
                MockResponse(
                    [
                        {
                            "success": False,
                            "errors": [
                                {
                                    "statusCode": "REQUEST_LIMIT_EXCEEDED",
                                    "message": "TotalRequests Limit exceeded",
                                }
                            ],
                        }
                    ]
                ),
                MockResponse([{"id": "001000001", "success": True}]),
            ]
        )

        results = await collections_api.insert(
            records, sobject_type="Account", batch_size=200, max_attempts=2
        )

        assert len(results) == 1
        assert results[0]["success"]
        # Should have retried the rate limit error
        assert len(client.post_calls) == 2

    @pytest.mark.asyncio
    async def test_basic_retry(self, client):
        """Test that failed records are retried."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": "Account 1"}]

        # First attempt fails, second succeeds
        client.set_responses(
            [
                MockResponse(
                    [
                        {
                            "success": False,
                            "errors": [
                                {"statusCode": "UNABLE_TO_LOCK_ROW", "message": "Lock"}
                            ],
                        }
                    ]
                ),
                MockResponse([{"id": "001000001", "success": True}]),
            ]
        )

        results = await collections_api.insert(
            records, sobject_type="Account", batch_size=200, max_attempts=2
        )

        assert len(results) == 1
        assert results[0]["success"]
        # Should have made 2 POST calls (initial + retry)
        assert len(client.post_calls) == 2

    @pytest.mark.asyncio
    async def test_shrinking_batch_sizes(self, client):
        """Test that batch sizes shrink on retry."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": f"Account {i}"} for i in range(100)]

        # Track batch sizes
        batch_sizes = []

        async def mock_post_tracking(*args, **kwargs):
            json_data = kwargs.get("json", {})
            batch_size = len(json_data.get("records", []))
            batch_sizes.append(batch_size)

            if len(batch_sizes) == 1:
                # First attempt: all fail
                return MockResponse(
                    [
                        {
                            "success": False,
                            "errors": [
                                {"statusCode": "UNABLE_TO_LOCK_ROW", "message": "Lock"}
                            ],
                        }
                        for _ in range(batch_size)
                    ]
                )
            else:
                # Retry attempts: succeed
                return MockResponse(
                    [{"id": f"001{i:05d}", "success": True} for i in range(batch_size)]
                )

        client.post = mock_post_tracking

        results = await collections_api.insert(
            records,
            sobject_type="Account",
            batch_size=[100, 50],  # Shrink from 100 to 50
            max_attempts=2,
        )

        assert len(results) == 100
        assert all(r["success"] for r in results)
        # First batch should be 100, retry batches should be 50 each
        assert batch_sizes[0] == 100
        assert all(size == 50 for size in batch_sizes[1:])

    @pytest.mark.asyncio
    async def test_should_retry_callback(self, client):
        """Test that should_retry callback controls retry logic."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": "Account 1"}]

        retry_calls = []

        def should_retry_callback(record, result, attempt):
            retry_calls.append({"record": record, "result": result, "attempt": attempt})
            # Only retry UNABLE_TO_LOCK_ROW errors
            errors = result.get("errors", [])
            return any(e.get("statusCode") == "UNABLE_TO_LOCK_ROW" for e in errors)

        client.set_responses(
            [
                MockResponse(
                    [
                        {
                            "success": False,
                            "errors": [
                                {"statusCode": "UNABLE_TO_LOCK_ROW", "message": "Lock"}
                            ],
                        }
                    ]
                ),
                MockResponse([{"id": "001000001", "success": True}]),
            ]
        )

        results = await collections_api.insert(
            records,
            sobject_type="Account",
            batch_size=200,
            max_attempts=3,
            should_retry=should_retry_callback,
        )

        assert len(results) == 1
        assert results[0]["success"]
        # should_retry should have been called once (for the failure)
        assert len(retry_calls) == 1
        assert retry_calls[0]["attempt"] == 1

    @pytest.mark.asyncio
    async def test_max_attempts_enforced(self, client):
        """Test that records stop retrying after max_attempts."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": "Account 1"}]

        # Always fail
        failure_response = MockResponse(
            [
                {
                    "success": False,
                    "errors": [{"statusCode": "UNABLE_TO_LOCK_ROW", "message": "Lock"}],
                }
            ]
        )

        client.set_responses([failure_response, failure_response, failure_response])

        results = await collections_api.insert(
            records, sobject_type="Account", batch_size=200, max_attempts=3
        )

        assert len(results) == 1
        assert not results[0]["success"]
        # Should have made exactly 3 calls
        assert len(client.post_calls) == 3


class TestProgressTracking:
    """Test progress callback functionality."""

    @pytest.mark.asyncio
    async def test_progress_callback_invoked(self, client):
        """Test that progress callback is called correctly."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": f"Account {i}"} for i in range(300)]

        client.set_responses(
            [
                MockResponse(
                    [{"id": f"001{i:05d}", "success": True} for i in range(200)]
                ),
                MockResponse(
                    [{"id": f"001{i:05d}", "success": True} for i in range(200, 300)]
                ),
            ]
        )

        result_calls = []

        async def result_callback(result: ResultInfo):
            result_calls.append(dict(result))

        results = await collections_api.insert(
            records,
            sobject_type="Account",
            batch_size=200,
            on_result=result_callback,
        )

        assert len(results) == 300
        # With the new design, callback is invoked once per batch
        # 300 records with batch_size=200 means 2 batches
        assert len(result_calls) == 2

        # Both callbacks split successes and errors
        assert len(result_calls[0]["successes"]) == 200
        assert len(result_calls[0]["errors"]) == 0
        assert len(result_calls[1]["successes"]) == 100
        assert len(result_calls[1]["errors"]) == 0

        # Verify successes are properly typed CollectionResults
        assert all(r.get("success") for r in result_calls[0]["successes"])
        assert all(r.get("id") is not None for r in result_calls[0]["successes"])

        # Context information is provided
        assert result_calls[0]["total_records"] == 300
        assert result_calls[0]["current_batch_size"] == 200
        assert result_calls[0]["current_concurrency"] == 5
        assert result_calls[0]["current_attempt"] == 1

    @pytest.mark.asyncio
    async def test_progress_with_retries(self, client):
        """Test that progress tracking works with retries."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": f"Account {i}"} for i in range(10)]

        client.set_responses(
            [
                # First attempt: all fail
                MockResponse(
                    [
                        {
                            "success": False,
                            "errors": [
                                {"statusCode": "UNABLE_TO_LOCK_ROW", "message": "Lock"}
                            ],
                        }
                        for _ in range(10)
                    ]
                ),
                # Second attempt: all succeed
                MockResponse(
                    [{"id": f"001{i:05d}", "success": True} for i in range(10)]
                ),
            ]
        )

        result_calls = []

        async def result_callback(result: ResultInfo):
            result_calls.append(dict(result))

        results = await collections_api.insert(
            records,
            sobject_type="Account",
            batch_size=200,
            max_attempts=2,
            on_result=result_callback,
        )

        assert len(results) == 10
        assert all(r["success"] for r in results)

        # Should have 2 callbacks (initial attempt + retry attempt)
        assert len(result_calls) == 2

        # First attempt: all failed - errors array contains them
        assert result_calls[0]["current_attempt"] == 1
        assert len(result_calls[0]["successes"]) == 0
        assert len(result_calls[0]["errors"]) == 10
        # Can inspect error codes in errors array
        for error in result_calls[0]["errors"]:
            assert not error.get("success")
            assert error["errors"][0]["statusCode"] == "UNABLE_TO_LOCK_ROW"

        # Second attempt: all succeeded - successes array contains them
        assert result_calls[1]["current_attempt"] == 2
        assert len(result_calls[1]["successes"]) == 10
        assert len(result_calls[1]["errors"]) == 0
        assert all(r.get("success") for r in result_calls[1]["successes"])


class TestConcurrencyScaling:
    """Test concurrency scaling across retry attempts."""

    @pytest.mark.asyncio
    async def test_scaling_concurrency(self, client):
        """Test that concurrency scales down on retry."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": f"Account {i}"} for i in range(100)]

        # Track call count
        call_count = [0]

        async def mock_post_tracking(*args, **kwargs):
            current_call = call_count[0]
            call_count[0] += 1

            json_data = kwargs.get("json", {})
            batch_size = len(json_data.get("records", []))

            if current_call < 2:
                # First attempt: fail all (2 batches of 50)
                return MockResponse(
                    [
                        {
                            "success": False,
                            "errors": [
                                {
                                    "statusCode": "UNABLE_TO_LOCK_ROW",
                                    "message": "Lock error",
                                }
                            ],
                        }
                        for _ in range(batch_size)
                    ]
                )
            else:
                # Retry: succeed
                return MockResponse(
                    [{"id": f"001{i:05d}", "success": True} for i in range(batch_size)]
                )

        client.post = mock_post_tracking

        results = await collections_api.insert(
            records,
            sobject_type="Account",
            batch_size=50,  # Fixed batch size
            max_concurrent_batches=[5, 2],  # Scale down from 5 to 2
            max_attempts=2,
        )

        assert len(results) == 100
        assert all(r["success"] for r in results)

        # Should have made 4 calls: 2 initial (both batches fail) + 2 retries (both succeed)
        assert call_count[0] == 4

    @pytest.mark.asyncio
    async def test_fixed_concurrency(self, client):
        """Test that integer concurrency doesn't scale."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": f"Account {i}"} for i in range(50)]

        async def mock_post(*args, **kwargs):
            json_data = kwargs.get("json", {})
            batch_size = len(json_data.get("records", []))

            if len(client.post_calls) == 1:
                return MockResponse(
                    [
                        {
                            "success": False,
                            "errors": [
                                {
                                    "statusCode": "UNABLE_TO_LOCK_ROW",
                                    "message": "Lock error",
                                }
                            ],
                        }
                        for _ in range(batch_size)
                    ]
                )
            else:
                return MockResponse(
                    [{"id": f"001{i:05d}", "success": True} for i in range(batch_size)]
                )

        client.post = mock_post

        results = await collections_api.insert(
            records,
            sobject_type="Account",
            batch_size=50,
            max_concurrent_batches=3,  # Fixed int - shouldn't change
            max_attempts=2,
        )

        assert len(results) == 50
        assert all(r["success"] for r in results)


class TestHTTPErrorHandling:
    """Test HTTP-level error handling."""

    @pytest.mark.asyncio
    async def test_default_retries_transient_http_errors(self, client):
        """Test that default behavior retries transient HTTP errors."""
        import httpx

        collections_api = CollectionsAPI(client)

        records = [{"Name": "Account 1"}]

        call_count = [0]

        async def mock_post(*args, **kwargs):
            current_call = call_count[0]
            call_count[0] += 1

            if current_call == 0:
                # First attempt: raise timeout error (should retry)
                raise httpx.TimeoutException("Request timeout")
            else:
                # Second attempt: succeed
                return MockResponse([{"id": "001000001", "success": True}])

        client.post = mock_post

        # Use default retry logic (should_retry=None)
        results = await collections_api.insert(
            records, sobject_type="Account", batch_size=200, max_attempts=2
        )

        # Should succeed after retry
        assert len(results) == 1
        assert results[0]["success"]
        # Should have made 2 calls (1 failed + 1 retry)
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_default_does_not_retry_4xx_errors(self, client):
        """Test that default behavior does NOT retry 4xx client errors."""
        import httpx

        collections_api = CollectionsAPI(client)

        records = [{"Name": "Account 1"}]

        # Create a mock 404 response
        mock_response = MockResponse({})
        mock_response.status_code = 404

        call_count = [0]

        async def mock_post(*args, **kwargs):
            call_count[0] += 1
            # Raise 404 error (should NOT retry)
            raise httpx.HTTPStatusError(
                "Not found", request=None, response=mock_response
            )

        client.post = mock_post

        # Use default retry logic (should_retry=None)
        results = await collections_api.insert(
            records, sobject_type="Account", batch_size=200, max_attempts=3
        )

        # Should fail without retry (4xx errors are not transient)
        assert len(results) == 1
        assert not results[0]["success"]
        # Should have made only 1 call (no retries)
        assert call_count[0] == 1

    @pytest.mark.asyncio
    async def test_http_error_converts_to_retryable_failure(self, client):
        """Test that transient HTTP errors are retried and converted to results."""
        import httpx

        collections_api = CollectionsAPI(client)

        records = [{"Name": "Account 1"}, {"Name": "Account 2"}]

        call_count = [0]

        async def mock_post(*args, **kwargs):
            current_call = call_count[0]
            call_count[0] += 1

            if current_call == 0:
                # First attempt: raise transient HTTP error
                raise httpx.TimeoutException("Connection timeout")
            else:
                # Second attempt: succeed
                return MockResponse(
                    [
                        {"id": "001000001", "success": True},
                        {"id": "001000002", "success": True},
                    ]
                )

        client.post = mock_post

        # Use default retry logic
        results = await collections_api.insert(
            records, sobject_type="Account", batch_size=200, max_attempts=2
        )

        # Should succeed after retry
        assert len(results) == 2
        assert all(r["success"] for r in results)
        # Should have made 2 calls (1 failed + 1 retry)
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_http_error_respects_should_retry_callback(self, client):
        """Test that HTTP errors are passed to should_retry callback."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": "Account 1"}]

        retry_callback_invocations = []

        def should_retry_callback(record, result, attempt):
            retry_callback_invocations.append(
                {"record": record, "result": result, "attempt": attempt}
            )
            # Check if it's an Exception (HTTP/network error)
            if isinstance(result, Exception):
                return True
            return False

        async def mock_post(*args, **kwargs):
            raise Exception("Network error")

        client.post = mock_post

        results = await collections_api.insert(
            records,
            sobject_type="Account",
            batch_size=200,
            max_attempts=3,
            should_retry=should_retry_callback,
        )

        # Should fail after all attempts
        assert len(results) == 1
        assert not results[0]["success"]

        # Callback should have been invoked for attempts 1 and 2
        assert len(retry_callback_invocations) == 2
        # Verify Exception was passed to callback
        for invocation in retry_callback_invocations:
            result = invocation["result"]
            assert isinstance(result, Exception)
            assert "Network error" in str(result)

    @pytest.mark.asyncio
    async def test_http_error_converted_to_result_format(self, client):
        """Test that HTTP errors are converted to CollectionResult format in final results."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": "Account 1"}]

        async def mock_post(*args, **kwargs):
            raise ValueError("Test error")

        client.post = mock_post

        results = await collections_api.insert(
            records, sobject_type="Account", batch_size=200, max_attempts=1
        )

        # Final result should be a CollectionResult, not an Exception
        assert len(results) == 1
        assert isinstance(results[0], dict)
        assert not results[0]["success"]
        errors = results[0]["errors"]
        assert len(errors) == 1
        assert errors[0]["statusCode"] == "HTTP_ERROR"
        assert "ValueError" in errors[0]["message"]
        assert "Test error" in errors[0]["message"]


class TestEdgeCases:
    """Test edge cases."""

    @pytest.mark.asyncio
    async def test_empty_records(self, client):
        """Test handling of empty records list."""
        collections_api = CollectionsAPI(client)
        results = await collections_api.insert([], sobject_type="Account")
        assert results == []

    @pytest.mark.asyncio
    async def test_fixed_batch_size(self, client):
        """Test that integer batch_size doesn't shrink."""
        collections_api = CollectionsAPI(client)

        records = [{"Name": f"Account {i}"} for i in range(50)]

        batch_sizes = []

        async def mock_post(*args, **kwargs):
            json_data = kwargs.get("json", {})
            batch_size = len(json_data.get("records", []))
            batch_sizes.append(batch_size)

            if len(batch_sizes) == 1:
                return MockResponse(
                    [
                        {
                            "success": False,
                            "errors": [
                                {"statusCode": "UNABLE_TO_LOCK_ROW", "message": "Lock"}
                            ],
                        }
                        for _ in range(batch_size)
                    ]
                )
            else:
                return MockResponse(
                    [{"id": f"001{i:05d}", "success": True} for i in range(batch_size)]
                )

        client.post = mock_post

        results = await collections_api.insert(
            records,
            sobject_type="Account",
            batch_size=50,  # Fixed int
            max_attempts=2,
        )

        assert len(results) == 50
        # All batches should be size 50 (no shrinking)
        assert all(size == 50 for size in batch_sizes)
