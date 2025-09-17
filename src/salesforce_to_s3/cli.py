"""Command-line interface for Salesforce to S3 export."""

import json
from pathlib import Path
from typing import Optional

import click

from salesforce_to_s3 import SalesforceToS3Exporter
from salesforce_to_s3.core import ExportConfig


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    help="Path to JSON configuration file",
)
@click.option(
    "--username",
    "-u",
    help="Salesforce username",
    envvar="SF_USERNAME",
)
@click.option(
    "--password",
    "-p",
    help="Salesforce password",
    envvar="SF_PASSWORD",
)
@click.option(
    "--security-token",
    "-t",
    help="Salesforce security token",
    envvar="SF_SECURITY_TOKEN",
)
@click.option(
    "--domain",
    "-d",
    default="login",
    help="Salesforce domain (login or test)",
    envvar="SF_DOMAIN",
)
@click.option(
    "--s3-bucket",
    "-b",
    help="S3 bucket name",
    envvar="S3_BUCKET",
)
@click.option(
    "--s3-prefix",
    default="salesforce-exports",
    help="S3 prefix for files",
    envvar="S3_PREFIX",
)
@click.option(
    "--aws-region",
    default="us-east-1",
    help="AWS region",
    envvar="AWS_REGION",
)
@click.option(
    "--sobjects",
    "-s",
    multiple=True,
    help="SObject types to export (can be specified multiple times)",
)
@click.option(
    "--batch-size",
    default=10000,
    help="Number of records per batch",
    type=int,
)
@click.option(
    "--compression",
    default="snappy",
    help="Parquet compression type",
    type=click.Choice(["snappy", "gzip", "brotli", "lz4"]),
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def main(
    config: Optional[Path],
    username: Optional[str],
    password: Optional[str],
    security_token: Optional[str],
    domain: str,
    s3_bucket: Optional[str],
    s3_prefix: str,
    aws_region: str,
    sobjects: tuple[str, ...],
    batch_size: int,
    compression: str,
    verbose: bool,
) -> None:
    """Export Salesforce SObjects to S3 as Parquet files."""

    # Load configuration from file if provided
    if config:
        with open(config) as f:
            config_data = json.load(f)
        export_config = ExportConfig(**config_data)
    else:
        # Create configuration from CLI arguments
        if not all([username, password, security_token, s3_bucket]):
            click.echo(
                "Error: Must provide either --config file or all required arguments "
                "(username, password, security-token, s3-bucket)"
            )
            raise click.Abort()

        if not sobjects:
            click.echo("Error: Must specify at least one SObject type with --sobjects")
            raise click.Abort()

        export_config = ExportConfig(
            username=username,  # type: ignore[arg-type]
            password=password,  # type: ignore[arg-type]
            security_token=security_token,  # type: ignore[arg-type]
            domain=domain,
            s3_bucket=s3_bucket,  # type: ignore[arg-type]
            s3_prefix=s3_prefix,
            aws_region=aws_region,
            sobjects=list(sobjects),
            batch_size=batch_size,
            compression=compression,
        )

    if verbose:
        click.echo(f"Exporting SObjects: {', '.join(export_config.sobjects)}")
        click.echo(f"Target S3 bucket: {export_config.s3_bucket}")
        click.echo(f"S3 prefix: {export_config.s3_prefix}")

    # Create exporter and run export
    exporter = SalesforceToS3Exporter(export_config)

    try:
        results = exporter.export_all()

        # Display results
        total_records = 0
        total_files = 0
        errors = []

        for result in results:
            if "error" in result:
                errors.append(f"{result['sobject']}: {result['error']}")
            else:
                total_records += result["records_exported"]
                total_files += result["files_created"]
                if verbose:
                    click.echo(
                        f"✓ {result['sobject']}: {result['records_exported']} records → "
                        f"{result['s3_location']}"
                    )

        if errors:
            click.echo("Errors occurred:")
            for error in errors:
                click.echo(f"  ✗ {error}")

        click.echo(
            f"\nExport completed: {total_records} records in {total_files} files"
        )

    except Exception as e:
        click.echo(f"Export failed: {e}")
        raise click.Abort() from e


if __name__ == "__main__":
    main()
