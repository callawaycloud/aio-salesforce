"""
Parquet writer module for converting Salesforce QueryResult to Parquet format.
"""

import logging
from typing import Any, Dict, List, Optional, Callable
from pathlib import Path
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq

from ..api.describe.types import FieldInfo
from .bulk_export import QueryResult, batch_records_async
from .arrow import (
    salesforce_to_arrow_type,
    create_schema_from_metadata,
    infer_schema_from_dataframe,
    filter_schema_to_data,
    convert_dataframe_types,
)


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
        :param column_formatter: Optional function to format column names
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

        converted_batch = []
        for record in batch:
            if self.column_formatter:
                converted_record = {
                    self.column_formatter(k): v for k, v in record.items()
                }
            else:
                converted_record = record.copy()
            converted_batch.append(converted_record)

        df = pd.DataFrame(converted_batch)

        if not self._schema_finalized:
            if self.schema is None:
                if self.type_mapping_overrides:
                    logging.warning(
                        "ParquetWriter: type_mapping_overrides has no effect when schema "
                        "is inferred from data (no fields_metadata provided). Pass a "
                        "pre-built schema or supply fields_metadata via write_query_to_parquet."
                    )
                self.schema = infer_schema_from_dataframe(df)
            else:
                self.schema = filter_schema_to_data(self.schema, list(df.columns))
            self._schema_finalized = True

        convert_dataframe_types(df, self.schema, self.convert_empty_to_null)
        table = pa.Table.from_pandas(df, schema=self.schema)

        if self._writer is None:
            self._writer = pq.ParquetWriter(self.file_path, self.schema)

        self._writer.write_table(table)

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
    effective_schema = schema or (
        create_schema_from_metadata(
            fields_metadata, column_formatter, type_mapping_overrides
        )
        if fields_metadata
        else None
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
