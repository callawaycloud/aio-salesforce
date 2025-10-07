"""Record validation and preparation utilities for Collections API."""

from typing import Any, Dict, List, Optional, Tuple, Union

from ..types import GenericSalesforceRecord, SalesforceRecord


def detect_record_type_and_sobject(
    records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
    sobject_type: Optional[str] = None,
) -> Tuple[str, bool]:
    """
    Detect if records are SalesforceRecord type and extract/validate sobject_type.

    :param records: List of records to analyze
    :param sobject_type: Optional sobject_type parameter
    :returns: Tuple of (sobject_type, has_attributes) where has_attributes indicates if records have SalesforceAttributes
    :raises ValueError: If sobject_type is required but not provided, or if records have inconsistent types
    """
    if not records:
        if sobject_type:
            return sobject_type, False
        raise ValueError("sobject_type is required when records list is empty")

    # Check first record to determine type
    first_record = records[0]
    has_attributes = isinstance(first_record, dict) and "attributes" in first_record

    if has_attributes:
        extracted_type = _validate_records_with_attributes(records, sobject_type)
        return extracted_type, True
    else:
        _validate_records_without_attributes(records, sobject_type)
        if sobject_type is None:
            raise ValueError(
                "sobject_type is required when records do not have SalesforceAttributes (attributes field)"
            )
        return sobject_type, False


def _validate_records_with_attributes(
    records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
    sobject_type: Optional[str],
) -> str:
    """Validate all records have attributes and consistent sobject_type."""
    extracted_sobject_type = None

    for i, record in enumerate(records):
        if not isinstance(record, dict) or "attributes" not in record:
            raise ValueError(
                f"Record at index {i} is missing 'attributes' field, but other records have it. "
                f"All records must be consistent."
            )

        attributes = record.get("attributes", {})
        if not isinstance(attributes, dict) or "type" not in attributes:
            raise ValueError(
                f"Record at index {i} has invalid 'attributes' structure. "
                f"Expected dict with 'type' field."
            )

        record_type = attributes["type"]
        if extracted_sobject_type is None:
            extracted_sobject_type = record_type
        elif extracted_sobject_type != record_type:
            raise ValueError(
                f"Record at index {i} has sobject_type '{record_type}', "
                f"but previous records have '{extracted_sobject_type}'. "
                f"All records must have the same sobject_type."
            )

    # If sobject_type was provided, validate it matches extracted type
    if sobject_type is not None and sobject_type != extracted_sobject_type:
        raise ValueError(
            f"Provided sobject_type '{sobject_type}' does not match "
            f"sobject_type '{extracted_sobject_type}' found in record attributes."
        )

    assert extracted_sobject_type is not None
    return extracted_sobject_type


def _validate_records_without_attributes(
    records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
    sobject_type: Optional[str],
) -> None:
    """Validate that none of the records have attributes (consistency check)."""
    for i, record in enumerate(records):
        if isinstance(record, dict) and "attributes" in record:
            raise ValueError(
                f"Record at index {i} has 'attributes' field, but first record doesn't. "
                f"All records must be consistent."
            )


def prepare_records(
    records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
    sobject_type: str,
    has_attributes: bool = False,
) -> List[GenericSalesforceRecord]:
    """
    Prepare records for collection operations by adding attributes if needed.

    :param records: List of records to prepare
    :param sobject_type: Salesforce object type (e.g., 'Account', 'Contact')
    :param has_attributes: Whether records already have SalesforceAttributes
    :returns: Records with attributes added (if needed)
    """
    if has_attributes:
        return records  # type: ignore

    # Add attributes to records that don't have them
    prepared_records = []
    for record in records:
        prepared_record = {"attributes": {"type": sobject_type}, **record}
        prepared_records.append(prepared_record)
    return prepared_records


def validate_records_have_field(
    records: Union[List[Dict[str, Any]], List[SalesforceRecord]],
    field_name: str,
    operation: str,
) -> None:
    """
    Validate that all records have a required field.

    :param records: List of records to validate
    :param field_name: Name of the required field
    :param operation: Operation name for error messages (e.g., 'update', 'upsert')
    :raises ValueError: If any record is missing the field
    """
    for i, record in enumerate(records):
        if field_name not in record:
            raise ValueError(
                f"Record at index {i} is missing required '{field_name}' field "
                f"for {operation} operation"
            )
