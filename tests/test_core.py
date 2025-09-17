"""Unit tests for core functionality."""

from unittest.mock import Mock, patch

import pytest

from salesforce_to_s3.core import ExportConfig, SalesforceToS3Exporter


class TestExportConfig:
    """Test the ExportConfig model."""

    def test_valid_config(self):
        """Test creating a valid configuration."""
        config = ExportConfig(
            username="test@example.com",
            password="password",
            security_token="token123",
            s3_bucket="test-bucket",
            sobjects=["Account", "Contact"],
        )
        assert config.username == "test@example.com"
        assert config.s3_bucket == "test-bucket"
        assert config.sobjects == ["Account", "Contact"]
        assert config.domain == "login"  # default value

    def test_missing_required_fields(self):
        """Test that missing required fields raise validation error."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            ExportConfig(
                username="test@example.com",
                # missing password, security_token, s3_bucket, sobjects
            )


class TestSalesforceToS3Exporter:
    """Test the main exporter class."""

    def test_init(self):
        """Test exporter initialization."""
        config = ExportConfig(
            username="test@example.com",
            password="password",
            security_token="token123",
            s3_bucket="test-bucket",
            sobjects=["Account"],
        )
        exporter = SalesforceToS3Exporter(config)
        assert exporter.config == config
        assert exporter._sf is None
        assert exporter._s3_client is None

    @patch("salesforce_to_s3.core.Salesforce")
    def test_sf_property(self, mock_sf):
        """Test Salesforce connection property."""
        config = ExportConfig(
            username="test@example.com",
            password="password",
            security_token="token123",
            s3_bucket="test-bucket",
            sobjects=["Account"],
        )
        exporter = SalesforceToS3Exporter(config)

        # First access should create connection
        sf = exporter.sf
        mock_sf.assert_called_once_with(
            username="test@example.com",
            password="password",
            security_token="token123",
            domain="login",
        )

        # Second access should reuse connection
        sf2 = exporter.sf
        assert sf is sf2
        assert mock_sf.call_count == 1

    @patch("salesforce_to_s3.core.boto3")
    def test_s3_client_property(self, mock_boto3):
        """Test S3 client property."""
        config = ExportConfig(
            username="test@example.com",
            password="password",
            security_token="token123",
            s3_bucket="test-bucket",
            sobjects=["Account"],
            aws_access_key_id="key123",
            aws_secret_access_key="secret123",
        )
        exporter = SalesforceToS3Exporter(config)

        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        # First access should create client
        client = exporter.s3_client
        mock_boto3.Session.assert_called_once_with(
            aws_access_key_id="key123",
            aws_secret_access_key="secret123",
            region_name="us-east-1",
        )
        mock_session.client.assert_called_once_with("s3")

        # Second access should reuse client
        client2 = exporter.s3_client
        assert client is client2
        assert mock_boto3.Session.call_count == 1
