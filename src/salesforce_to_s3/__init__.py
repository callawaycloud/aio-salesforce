"""Salesforce to S3 export utility.

A modern Python library for exporting Salesforce SObjects to S3 as Parquet files.
"""

__version__ = "0.1.0"
__author__ = "Callaway Cloud"
__email__ = "support@callawaycloud.com"

from salesforce_to_s3.core import SalesforceToS3Exporter

__all__ = ["SalesforceToS3Exporter"]
