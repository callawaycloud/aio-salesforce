"""
Parquet writer module for converting Salesforce QueryResult to Parquet format.
"""

import logging
from typing import Any, Dict, List, Optional, Callable
from pathlib import Path
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime

from ..api.describe.types import FieldInfo

from .bulk_export import QueryResult, batch_records_async


def salesforce_to_arrow_type(
    sf_type: str, type_mapping_overrides: Optional[Dict[str, pa.DataType]] = None
) -> pa.DataType:
    """Convert Salesforce data types to Arrow data types.

    :param sf_type: Salesforce field type
    :param type_mapping_overrides: Optional dict to override default type mappings
    """
    default_type_mapping = {
        "string": pa.string(),
        "boolean": pa.bool_(),
        "int": pa.int64(),
        "double": pa.float64(),
        "date": pa.date32(),  # Store as proper date type
        "datetime": pa.timestamp("us", tz="UTC"),
        "currency": pa.float64(),
        "reference": pa.string(),
        "picklist": pa.string(),
        "multipicklist": pa.string(),
        "textarea": pa.string(),
        "phone": pa.string(),
        "url": pa.string(),
        "email": pa.string(),
        "combobox": pa.string(),
        "percent": pa.float64(),
        "id": pa.string(),
        "base64": pa.string(),
        "anyType": pa.string(),
    }

    # Apply overrides if provided
    if type_mapping_overrides:
        type_mapping = {**default_type_mapping, **type_mapping_overrides}
    else:
        type_mapping = default_type_mapping

    return type_mapping.get(sf_type.lower(), pa.string())


def create_schema_from_metadata(
    fields_metadata: List[FieldInfo],
    column_formatter: Optional[Callable[[str], str]] = None,
    type_mapping_overrides: Optional[Dict[str, pa.DataType]] = None,
) -> pa.Schema:
    """
    Create a PyArrow schema from Salesforce field metadata.

    :param fields_metadata: List of field metadata dictionaries from Salesforce
    :param column_formatter: Optional function to format column names
    :param type_mapping_overrides: Optional dict to override default type mappings
    :returns: PyArrow schema
    """
    arrow_fields = []
    for field in fields_metadata:
        field_name = field.get("name", "")
        if column_formatter:
            field_name = column_formatter(field_name)
        sf_type = field.get("type", "string")
        arrow_type = salesforce_to_arrow_type(sf_type, type_mapping_overrides)
        # All fields are nullable since Salesforce can return empty values
        arrow_fields.append(pa.field(field_name, arrow_type, nullable=True))

    return pa.schema(arrow_fields)


class ParquetWriter:
    """
    Writer class for converting Salesforce QueryResult to Parquet format.
    Supports streaming writes and optional schema from field metadata.
    """

    def __init__(
        self,
        file_path: str,
        schema: Optional[pa.Schema] = None,
        batch_size: int = 10000,
        convert_empty_to_null: bool = True,
        column_formatter: Optional[Callable[[str], str]] = None,
        type_mapping_overrides: Optional[Dict[str, pa.DataType]] = None,
    ):
        """
        Initialize ParquetWriter.

        :param file_path: Path to output parquet file
        :param schema: Optional PyArrow schema. If None, will be inferred from first batch
        :param batch_size: Number of records to process in each batch
        :param convert_empty_to_null: Convert empty strings to null values
        :param column_formatter: Optional function to format column names. If None, no formatting is applied
        :param type_mapping_overrides: Optional dict to override default type mappings
        """
        self.file_path = file_path
        self.schema = schema
        self.batch_size = batch_size
        self.convert_empty_to_null = convert_empty_to_null
        self.column_formatter = column_formatter
        self.type_mapping_overrides = type_mapping_overrides
        self._writer = None
        self._schema_finalized = False

        # Ensure parent directory exists
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    async def write_query_result(self, query_result: QueryResult) -> None:
        """
        Write all records from a QueryResult to the parquet file (async version).

        :param query_result: QueryResult to write
        """
        try:
            async for batch in batch_records_async(query_result, self.batch_size):
                self._write_batch(batch)
        finally:
            self.close()

    def _write_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Write a batch of records to the parquet file."""
        if not batch:
            return

        # Apply column formatting if specified
        converted_batch = []
        for record in batch:
            if self.column_formatter:
                converted_record = {
                    self.column_formatter(k): v for k, v in record.items()
                }
            else:
                converted_record = record.copy()
            converted_batch.append(converted_record)

        # Create DataFrame
        df = pd.DataFrame(converted_batch)

        # If schema not finalized, create it from first batch
        if not self._schema_finalized:
            if self.schema is None:
                self.schema = self._infer_schema_from_dataframe(df)
            else:
                # Filter schema to only include fields that are actually in the data
                self.schema = self._filter_schema_to_data(self.schema, list(df.columns))
            self._schema_finalized = True

        # Apply data type conversions based on schema
        self._convert_dataframe_types(df)

        # Create Arrow table
        table = pa.Table.from_pandas(df, schema=self.schema)

        # Initialize writer if needed
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.file_path, self.schema)

        # Write the table
        self._writer.write_table(table)

    def _infer_schema_from_dataframe(self, df: pd.DataFrame) -> pa.Schema:
        """Infer schema from the first DataFrame."""
        fields = []
        for col_name, dtype in df.dtypes.items():
            if dtype == "object":
                arrow_type = pa.string()
            elif dtype == "bool":
                arrow_type = pa.bool_()
            elif dtype in ["int64", "int32"]:
                arrow_type = pa.int64()
            elif dtype in ["float64", "float32"]:
                arrow_type = pa.float64()
            else:
                arrow_type = pa.string()

            fields.append(pa.field(col_name, arrow_type, nullable=True))

        return pa.schema(fields)

    def _filter_schema_to_data(
        self, schema: pa.Schema, data_columns: List[str]
    ) -> pa.Schema:
        """Filter schema to only include fields that are present in the data."""
        # Convert data columns to set for faster lookup
        data_columns_set = set(data_columns)

        # Filter schema fields to only those present in data
        filtered_fields = []
        for field in schema:
            if field.name in data_columns_set:
                filtered_fields.append(field)

        if len(filtered_fields) != len(data_columns_set):
            # Log fields that are in data but not in schema (shouldn't happen normally)
            missing_in_schema = data_columns_set - {f.name for f in filtered_fields}
            if missing_in_schema:
                logging.warning(
                    f"Fields in data but not in schema: {missing_in_schema}"
                )

        return pa.schema(filtered_fields)

    def _convert_dataframe_types(self, df: pd.DataFrame) -> None:
        """Convert DataFrame types based on the schema."""
        if self.schema is None:
            return
        for field in self.schema:
            field_name = field.name
            if field_name not in df.columns:
                continue

            # Convert empty strings to null if requested
            if self.convert_empty_to_null:
                df[field_name] = df[field_name].replace({"": None})

            # Apply type-specific conversions
            if pa.types.is_boolean(field.type):
                # Convert string 'true'/'false' to boolean, keeping original values for others
                original_series = df[field_name]
                mapped_series = original_series.map(
                    {"true": True, "false": False, None: None}
                )
                # For values that weren't mapped, keep the original values
                # This avoids the fillna FutureWarning by using boolean indexing instead
                mask = mapped_series.notna()
                result_series = original_series.copy()
                result_series.loc[mask] = mapped_series.loc[mask]
                df[field_name] = result_series
            elif pa.types.is_integer(field.type):
                df[field_name] = pd.to_numeric(df[field_name], errors="coerce").astype(
                    "Int64"
                )  # Nullable integer
            elif pa.types.is_floating(field.type):
                df[field_name] = pd.to_numeric(df[field_name], errors="coerce")
            elif pa.types.is_timestamp(field.type):
                # Convert Salesforce ISO datetime strings to timestamps
                datetime_series = df[field_name]
                if isinstance(datetime_series, pd.Series):
                    df[field_name] = self._convert_datetime_strings_to_timestamps(
                        datetime_series
                    )
            elif pa.types.is_date(field.type):
                # Convert Salesforce ISO date strings to dates
                date_series = df[field_name]
                if isinstance(date_series, pd.Series):
                    df[field_name] = self._convert_date_strings_to_dates(date_series)

            # Replace empty strings with None for non-string fields
            if not pa.types.is_string(field.type):
                df[field_name] = df[field_name].replace("", pd.NA)

    def _convert_datetime_strings_to_timestamps(self, series: pd.Series) -> pd.Series:
        """
        Convert Salesforce ISO datetime strings to pandas datetime objects.

        Salesforce returns datetime in ISO format like '2023-12-25T10:30:00.000+0000'
        or '2023-12-25T10:30:00Z'. This method handles various ISO formats.
        """

        def parse_sf_datetime(dt_str):
            if pd.isna(dt_str) or dt_str == "" or dt_str is None:
                return pd.NaT

            try:
                # Handle common Salesforce datetime formats
                dt_str = str(dt_str).strip()

                # Convert +0000 to Z for pandas compatibility
                if dt_str.endswith("+0000"):
                    dt_str = dt_str[:-5] + "Z"
                elif dt_str.endswith("+00:00"):
                    dt_str = dt_str[:-6] + "Z"

                # Use pandas to_datetime with UTC parsing
                return pd.to_datetime(dt_str, utc=True)

            except (ValueError, TypeError) as e:
                logging.warning(f"Failed to parse datetime string '{dt_str}': {e}")
                return pd.NaT

        # Apply the conversion function to the series
        result = series.apply(parse_sf_datetime)
        if isinstance(result, pd.Series):
            return result
        else:
            # This shouldn't happen, but handle it gracefully
            return pd.Series(result, index=series.index)

    def _convert_date_strings_to_dates(self, series: pd.Series) -> pd.Series:
        """
        Convert Salesforce ISO date strings to pandas date objects.

        Salesforce returns date in ISO format like '2025-10-01'.
        """

        def parse_sf_date(date_str):
            if pd.isna(date_str) or date_str == "" or date_str is None:
                return pd.NaT

            try:
                # Handle Salesforce date format (YYYY-MM-DD)
                date_str = str(date_str).strip()

                # Use pandas to_datetime for date parsing, then convert to date
                return pd.to_datetime(date_str, format="%Y-%m-%d").date()

            except (ValueError, TypeError) as e:
                logging.warning(f"Failed to parse date string '{date_str}': {e}")
                return pd.NaT

        # Apply the conversion function to the series
        result = series.apply(parse_sf_date)
        if isinstance(result, pd.Series):
            return result
        else:
            # This shouldn't happen, but handle it gracefully
            return pd.Series(result, index=series.index)

    def close(self) -> None:
        """Close the parquet writer."""
        if self._writer:
            self._writer.close()
            self._writer = None


async def write_query_to_parquet(
    query_result: QueryResult,
    file_path: str,
    fields_metadata: Optional[List[FieldInfo]] = None,
    schema: Optional[pa.Schema] = None,
    batch_size: int = 10000,
    convert_empty_to_null: bool = True,
    column_formatter: Optional[Callable[[str], str]] = None,
    type_mapping_overrides: Optional[Dict[str, pa.DataType]] = None,
) -> None:
    """
    Convenience function to write a QueryResult to a parquet file (async version).

    :param query_result: QueryResult to write
    :param file_path: Path to output parquet file
    :param fields_metadata: Optional Salesforce field metadata for schema creation
    :param schema: Optional pre-created PyArrow schema (takes precedence over fields_metadata)
    :param batch_size: Number of records to process in each batch
    :param convert_empty_to_null: Convert empty strings to null values
    :param column_formatter: Optional function to format column names
    :param type_mapping_overrides: Optional dict to override default type mappings
    """
    effective_schema = None
    if schema:
        effective_schema = schema
    elif fields_metadata:
        effective_schema = create_schema_from_metadata(
            fields_metadata, column_formatter, type_mapping_overrides
        )

    writer = ParquetWriter(
        file_path=file_path,
        schema=effective_schema,
        batch_size=batch_size,
        convert_empty_to_null=convert_empty_to_null,
        column_formatter=column_formatter,
        type_mapping_overrides=type_mapping_overrides,
    )

    await writer.write_query_result(query_result)
