"""
Arrow streaming module for Salesforce data.

Provides shared schema/type helpers and async generators for streaming Salesforce
QueryResult data as PyArrow RecordBatches. ParquetWriter imports from here so all
type-conversion logic lives in one place.
"""

import logging
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional

import pandas as pd
import pyarrow as pa

from ..api.describe.types import FieldInfo
from .bulk_export import QueryResult, batch_records_async


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------

def _default_type_mapping() -> Dict[str, pa.DataType]:
    return {
        "string": pa.string(),
        "boolean": pa.bool_(),
        "int": pa.int64(),
        "double": pa.float64(),
        "date": pa.date32(),
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


def salesforce_to_arrow_type(
    sf_type: str,
    type_mapping_overrides: Optional[Dict[str, pa.DataType]] = None,
) -> pa.DataType:
    """Convert a Salesforce field type string to a PyArrow DataType.

    :param sf_type: Salesforce field type (e.g. "reference", "datetime")
    :param type_mapping_overrides: Optional overrides for specific type mappings
    :returns: Corresponding PyArrow DataType
    """
    mapping = _default_type_mapping()
    if type_mapping_overrides:
        mapping = {**mapping, **type_mapping_overrides}
    return mapping.get(sf_type.lower(), pa.string())


def create_schema_from_metadata(
    fields_metadata: List[FieldInfo],
    column_formatter: Optional[Callable[[str], str]] = None,
    type_mapping_overrides: Optional[Dict[str, pa.DataType]] = None,
) -> pa.Schema:
    """Create a PyArrow schema from Salesforce field metadata.

    :param fields_metadata: List of FieldInfo dicts from a Salesforce describe call
    :param column_formatter: Optional function to transform column names
    :param type_mapping_overrides: Optional overrides for Salesforce→Arrow type mapping
    :returns: PyArrow Schema
    """
    arrow_fields = []
    for field in fields_metadata:
        field_name = field.get("name", "")
        if column_formatter:
            field_name = column_formatter(field_name)
        sf_type = field.get("type", "string")
        arrow_type = salesforce_to_arrow_type(sf_type, type_mapping_overrides)
        arrow_fields.append(pa.field(field_name, arrow_type, nullable=True))
    return pa.schema(arrow_fields)


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def infer_schema_from_dataframe(df: pd.DataFrame) -> pa.Schema:
    """Infer an Arrow schema from a pandas DataFrame's dtypes."""
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


def filter_schema_to_data(schema: pa.Schema, data_columns: List[str]) -> pa.Schema:
    """Return a copy of schema containing only fields present in data_columns."""
    data_columns_set = set(data_columns)
    filtered_fields = [f for f in schema if f.name in data_columns_set]

    missing_in_schema = data_columns_set - {f.name for f in filtered_fields}
    if missing_in_schema:
        logging.warning(f"Fields in data but not in schema: {missing_in_schema}")

    return pa.schema(filtered_fields)


# ---------------------------------------------------------------------------
# Type conversion helpers
# ---------------------------------------------------------------------------

def _convert_datetime_strings(series: pd.Series) -> pd.Series:
    """Parse Salesforce ISO datetime strings (e.g. '2023-12-25T10:30:00.000+0000')
    into a UTC-aware pandas datetime Series suitable for Arrow timestamp columns.

    Uses vectorized pandas operations — avoids per-row Python function calls which
    are orders of magnitude slower for large batches.
    """
    s = series.replace({"": None})
    # Normalise the two offset formats Salesforce uses to the standard +HH:MM form
    # so pandas can parse them in a single vectorised pass.
    s = s.str.replace(r"\+0000$", "+00:00", regex=True)
    s = s.str.replace(r"Z$", "+00:00", regex=True)
    return pd.to_datetime(s, utc=True, errors="coerce")


def _convert_date_strings(series: pd.Series) -> pd.Series:
    """Parse Salesforce ISO date strings (e.g. '2025-10-01') into a pandas
    datetime64 Series; PyArrow will cast that to date32."""
    series = series.replace({"": None})
    return pd.to_datetime(series, format="%Y-%m-%d", errors="coerce")


def convert_dataframe_types(
    df: pd.DataFrame,
    schema: pa.Schema,
    convert_empty_to_null: bool = True,
) -> None:
    """Apply Salesforce-to-Arrow type conversions to a DataFrame in place.

    Handles boolean, integer, float, timestamp, date, and string fields.
    Unknown types are left untouched.
    """
    for field in schema:
        col = field.name
        if col not in df.columns:
            continue

        if convert_empty_to_null:
            df[col] = df[col].replace({"": None})

        if pa.types.is_boolean(field.type):
            df[col] = df[col].map(
                {"true": True, "false": False, "True": True, "False": False}
            )
        elif pa.types.is_integer(field.type):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif pa.types.is_floating(field.type):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif pa.types.is_timestamp(field.type):
            df[col] = _convert_datetime_strings(df[col])
        elif pa.types.is_date(field.type):
            df[col] = _convert_date_strings(df[col])
        elif pa.types.is_string(field.type):
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else str(x))

        if not pa.types.is_string(field.type):
            df[col] = df[col].replace("", pd.NA)


# ---------------------------------------------------------------------------
# Public streaming API
# ---------------------------------------------------------------------------

def records_to_arrow_batch(
    records: List[Dict[str, Any]],
    schema: pa.Schema,
    convert_empty_to_null: bool = True,
) -> pa.RecordBatch:
    """Convert a list of Salesforce record dicts into a PyArrow RecordBatch.

    Record keys are lowercased before matching against the schema, so the schema
    passed here must also use lowercase field names (e.g. built with
    ``column_formatter=str.lower``).

    :param records: List of record dicts from Salesforce (keys may be any case)
    :param schema: PyArrow schema with **lowercase** field names
    :param convert_empty_to_null: Convert empty strings to null values
    :returns: PyArrow RecordBatch
    """
    converted = [{k.lower(): v for k, v in r.items()} for r in records]
    df = pd.DataFrame(converted)
    convert_dataframe_types(df, schema, convert_empty_to_null)
    table = pa.Table.from_pandas(df, schema=schema)
    if table.num_rows > 0:
        return table.to_batches()[0]
    return pa.RecordBatch.from_pydict(
        {f.name: [] for f in schema}, schema=schema
    )


async def query_result_to_batches(
    query_result: QueryResult,
    fields_metadata: Optional[List[FieldInfo]] = None,
    schema: Optional[pa.Schema] = None,
    batch_size: int = 10000,
    convert_empty_to_null: bool = True,
) -> AsyncGenerator[pa.RecordBatch, None]:
    """Async generator that streams a QueryResult as PyArrow RecordBatches.

    Field names are lowercased in each batch. The schema is derived from
    ``fields_metadata`` (using ``column_formatter=str.lower``) on the first
    call and then filtered to columns actually present in the data. If neither
    ``schema`` nor ``fields_metadata`` is provided the schema is inferred from
    the first batch.

    :param query_result: QueryResult from a bulk_query call
    :param fields_metadata: Salesforce field metadata for schema creation
    :param schema: Pre-created PyArrow schema (takes precedence over fields_metadata)
    :param batch_size: Number of records per batch
    :param convert_empty_to_null: Convert empty strings to null values
    :yields: PyArrow RecordBatch objects
    """
    effective_schema = schema
    if effective_schema is None and fields_metadata:
        effective_schema = create_schema_from_metadata(
            fields_metadata, column_formatter=str.lower
        )

    schema_finalized = False

    async for records in batch_records_async(query_result, batch_size):
        converted = [{k.lower(): v for k, v in r.items()} for r in records]
        df = pd.DataFrame(converted)

        if not schema_finalized:
            if effective_schema is None:
                effective_schema = infer_schema_from_dataframe(df)
            else:
                effective_schema = filter_schema_to_data(
                    effective_schema, df.columns.tolist()
                )
            schema_finalized = True

        convert_dataframe_types(df, effective_schema, convert_empty_to_null)
        table = pa.Table.from_pandas(df, schema=effective_schema)

        for batch in table.to_batches():
            yield batch
