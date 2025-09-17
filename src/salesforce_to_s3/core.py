"""Core functionality for Salesforce to S3 export."""

from typing import Any, Optional

import boto3
import pandas as pd
from pydantic import BaseModel, Field
from simple_salesforce import Salesforce


class ExportConfig(BaseModel):
    """Configuration for Salesforce to S3 export."""

    # Salesforce connection
    username: str = Field(..., description="Salesforce username")
    password: str = Field(..., description="Salesforce password")
    security_token: str = Field(..., description="Salesforce security token")
    domain: str = Field(
        default="login", description="Salesforce domain (login or test)"
    )

    # S3 configuration
    s3_bucket: str = Field(..., description="S3 bucket name")
    s3_prefix: str = Field(
        default="salesforce-exports", description="S3 prefix for files"
    )
    aws_access_key_id: Optional[str] = Field(
        default=None, description="AWS access key ID"
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None, description="AWS secret access key"
    )
    aws_region: str = Field(default="us-east-1", description="AWS region")

    # Export options
    sobjects: list[str] = Field(..., description="List of SObject types to export")
    batch_size: int = Field(default=10000, description="Number of records per batch")
    compression: str = Field(default="snappy", description="Parquet compression type")


class SalesforceToS3Exporter:
    """Main class for exporting Salesforce data to S3 as Parquet files."""

    def __init__(self, config: ExportConfig) -> None:
        """Initialize the exporter with configuration."""
        self.config = config
        self._sf: Optional[Salesforce] = None
        self._s3_client: Optional[Any] = None

    @property
    def sf(self) -> Salesforce:
        """Get or create Salesforce connection."""
        if self._sf is None:
            self._sf = Salesforce(
                username=self.config.username,
                password=self.config.password,
                security_token=self.config.security_token,
                domain=self.config.domain,
            )
        return self._sf

    @property
    def s3_client(self) -> Any:
        """Get or create S3 client."""
        if self._s3_client is None:
            session = boto3.Session(
                aws_access_key_id=self.config.aws_access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key,
                region_name=self.config.aws_region,
            )
            self._s3_client = session.client("s3")
        return self._s3_client

    def export_sobject(self, sobject_type: str) -> dict[str, Any]:
        """Export a single SObject type to S3."""
        # Query all records (simplified - real implementation would handle pagination)
        query = f"SELECT * FROM {sobject_type}"
        results = self.sf.query_all(query)

        if not results["records"]:
            return {"sobject": sobject_type, "records_exported": 0, "files_created": 0}

        # Convert to DataFrame
        records = []
        for record in results["records"]:
            # Remove Salesforce metadata
            record.pop("attributes", None)
            records.append(record)

        df = pd.DataFrame(records)

        # Upload to S3 as Parquet
        s3_key = f"{self.config.s3_prefix}/{sobject_type}.parquet"
        parquet_buffer: bytes = df.to_parquet(  # type: ignore[call-overload]
            path=None,
            engine="pyarrow",
            compression=self.config.compression,
            index=False,
        )

        self.s3_client.put_object(
            Bucket=self.config.s3_bucket,
            Key=s3_key,
            Body=parquet_buffer,
        )

        return {
            "sobject": sobject_type,
            "records_exported": len(records),
            "files_created": 1,
            "s3_location": f"s3://{self.config.s3_bucket}/{s3_key}",
        }

    def export_all(self) -> list[dict[str, Any]]:
        """Export all configured SObjects to S3."""
        results = []
        for sobject_type in self.config.sobjects:
            try:
                result = self.export_sobject(sobject_type)
                results.append(result)
            except Exception as e:
                results.append(
                    {
                        "sobject": sobject_type,
                        "error": str(e),
                        "records_exported": 0,
                        "files_created": 0,
                    }
                )
        return results
