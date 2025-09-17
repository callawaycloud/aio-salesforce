"""Unit tests for CLI functionality."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from click.testing import CliRunner

from salesforce_to_s3.cli import main


class TestCLI:
    """Test the command-line interface."""

    def test_help_output(self):
        """Test that help output is displayed correctly."""
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "Export Salesforce SObjects to S3 as Parquet files" in result.output

    def test_missing_required_args(self):
        """Test that missing required arguments cause failure."""
        runner = CliRunner()
        result = runner.invoke(main, [])
        assert result.exit_code != 0
        assert (
            "Must provide either --config file or all required arguments"
            in result.output
        )

    def test_missing_sobjects(self):
        """Test that missing sobjects argument causes failure."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "--username",
                "test@example.com",
                "--password",
                "password",
                "--security-token",
                "token123",
                "--s3-bucket",
                "test-bucket",
            ],
        )
        assert result.exit_code != 0
        assert "Must specify at least one SObject type" in result.output

    @patch("salesforce_to_s3.cli.SalesforceToS3Exporter")
    def test_successful_export_with_args(self, mock_exporter_class):
        """Test successful export using command-line arguments."""
        mock_exporter = Mock()
        mock_exporter.export_all.return_value = [
            {
                "sobject": "Account",
                "records_exported": 100,
                "files_created": 1,
                "s3_location": "s3://test-bucket/salesforce-exports/Account.parquet",
            }
        ]
        mock_exporter_class.return_value = mock_exporter

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "--username",
                "test@example.com",
                "--password",
                "password",
                "--security-token",
                "token123",
                "--s3-bucket",
                "test-bucket",
                "--sobjects",
                "Account",
                "--verbose",
            ],
        )

        assert result.exit_code == 0
        assert "Export completed: 100 records in 1 files" in result.output
        assert "Account: 100 records" in result.output

    @patch("salesforce_to_s3.cli.SalesforceToS3Exporter")
    def test_successful_export_with_config_file(self, mock_exporter_class):
        """Test successful export using configuration file."""
        mock_exporter = Mock()
        mock_exporter.export_all.return_value = [
            {
                "sobject": "Contact",
                "records_exported": 50,
                "files_created": 1,
                "s3_location": "s3://test-bucket/salesforce-exports/Contact.parquet",
            }
        ]
        mock_exporter_class.return_value = mock_exporter

        config_data = {
            "username": "test@example.com",
            "password": "password",
            "security_token": "token123",
            "s3_bucket": "test-bucket",
            "sobjects": ["Contact"],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            config_file = Path(f.name)

        try:
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "--config",
                    str(config_file),
                    "--verbose",
                ],
            )

            assert result.exit_code == 0
            assert "Export completed: 50 records in 1 files" in result.output
        finally:
            config_file.unlink()

    @patch("salesforce_to_s3.cli.SalesforceToS3Exporter")
    def test_export_with_errors(self, mock_exporter_class):
        """Test export that encounters errors."""
        mock_exporter = Mock()
        mock_exporter.export_all.return_value = [
            {
                "sobject": "Account",
                "records_exported": 100,
                "files_created": 1,
                "s3_location": "s3://test-bucket/salesforce-exports/Account.parquet",
            },
            {
                "sobject": "Contact",
                "error": "Permission denied",
                "records_exported": 0,
                "files_created": 0,
            },
        ]
        mock_exporter_class.return_value = mock_exporter

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "--username",
                "test@example.com",
                "--password",
                "password",
                "--security-token",
                "token123",
                "--s3-bucket",
                "test-bucket",
                "--sobjects",
                "Account",
                "--sobjects",
                "Contact",
            ],
        )

        assert result.exit_code == 0
        assert "Export completed: 100 records in 1 files" in result.output
        assert "Errors occurred:" in result.output
        assert "Contact: Permission denied" in result.output
