"""
TypedDict definitions for Salesforce API responses.
"""

from typing import (
    Any,
    Dict,
    List,
    Optional,
    TypedDict,
    Union,
    Generic,
    TypeVar,
    NotRequired,
)


class SalesforceAttributes(TypedDict):
    """Standard Salesforce record attributes."""

    type: str
    url: NotRequired[str]  # Present in responses, not in requests


# Generic type variable for record data
RecordData = TypeVar("RecordData", bound=Dict[str, Any])


class SalesforceRecord(TypedDict, Generic[RecordData]):
    """
    Generic Salesforce record structure with enforced attributes.

    This type ensures that all Salesforce records have the required 'attributes' field
    while allowing flexibility for the actual record data.

    Usage:
        # Generic usage (any fields allowed)
        GenericRecord = SalesforceRecord[Dict[str, Any]]

        # Specific typed usage when you know the fields
        class AccountData(TypedDict, total=False):
            Id: str
            Name: str
            Type: str

        AccountRecord = SalesforceRecord[AccountData]
    """

    attributes: SalesforceAttributes


# Convenience type alias
GenericSalesforceRecord = SalesforceRecord[Dict[str, Any]]


class OrganizationInfo(TypedDict):
    """Organization information from SOQL query."""

    attributes: SalesforceAttributes
    Id: str
    Name: str
    OrganizationType: str
    InstanceName: str
    IsSandbox: bool


class LimitInfo(TypedDict):
    """Individual limit information."""

    Max: int
    Remaining: int


class OrganizationLimits(TypedDict, total=False):
    """Organization limits - using total=False since keys vary by org."""

    # API Limits
    DailyApiRequests: LimitInfo
    DailyBulkApiBatches: LimitInfo
    DailyBulkV2QueryJobs: LimitInfo
    DailyStreamingApiEvents: LimitInfo

    # Storage Limits
    DataStorageMB: LimitInfo
    FileStorageMB: LimitInfo

    # Email Limits
    DailyWorkflowEmails: LimitInfo
    MassEmail: LimitInfo
    SingleEmail: LimitInfo

    # Other common limits - there are many more, but these are the most common
    # Using Any for the rest since there are 60+ different limit types
    # and they vary by org type and features


class SObjectUrls(TypedDict):
    """URLs for various SObject operations."""

    sobject: str
    describe: str
    rowTemplate: str


class SObjectInfo(TypedDict):
    """Basic SObject information from list_sobjects."""

    activateable: bool
    associateEntityType: Optional[str]
    associateParentEntity: Optional[str]
    createable: bool
    custom: bool
    customSetting: bool
    deepCloneable: bool
    deletable: bool
    deprecatedAndHidden: bool
    feedEnabled: bool
    hasSubtypes: bool
    isInterface: bool
    isSubtype: bool
    keyPrefix: Optional[str]
    label: str
    labelPlural: str
    layoutable: bool
    mergeable: bool
    mruEnabled: bool
    name: str
    queryable: bool
    replicateable: bool
    retrieveable: bool
    searchable: bool
    triggerable: bool
    undeletable: bool
    updateable: bool
    urls: SObjectUrls


class ActionOverride(TypedDict):
    """Action override information."""

    formFactor: str
    isAvailableInTouch: bool
    name: str
    pageId: Optional[str]
    url: Optional[str]


class ChildRelationship(TypedDict):
    """Child relationship information."""

    cascadeDelete: bool
    childSObject: str
    deprecatedAndHidden: bool
    field: str
    junctionIdListNames: List[str]
    junctionReferenceTo: List[str]
    relationshipName: Optional[str]
    restrictedDelete: bool


class PicklistValue(TypedDict):
    """Picklist value information."""

    active: bool
    defaultValue: bool
    label: str
    validFor: Optional[str]
    value: str


class FilteredLookupInfo(TypedDict):
    """Filtered lookup information."""

    controllingFields: List[str]
    dependent: bool
    optionalFilter: bool


class RecordTypeInfo(TypedDict):
    """Record type information."""

    available: bool
    defaultRecordTypeMapping: bool
    developerName: str
    master: bool
    name: str
    recordTypeId: str
    urls: Dict[str, str]


class NamedLayoutInfo(TypedDict):
    """Named layout information."""

    name: str
    urls: Dict[str, str]


class ScopeInfo(TypedDict):
    """Scope information."""

    label: str
    name: str


class FieldInfo(TypedDict, total=False):
    """Field information from describe_sobject - using total=False due to many optional fields."""

    # Core field properties (always present)
    name: str
    type: str
    label: str

    # Common properties (usually present)
    length: int
    nillable: bool
    createable: bool
    updateable: bool
    custom: bool

    # Boolean flags
    aggregatable: bool
    aiPredictionField: bool
    autoNumber: bool
    calculated: bool
    caseSensitive: bool
    dependentPicklist: bool
    deprecatedAndHidden: bool
    encrypted: bool
    externalId: bool
    filterable: bool
    formulaTreatNullNumberAsZero: bool
    groupable: bool
    highScaleNumber: bool
    htmlFormatted: bool
    idLookup: bool
    nameField: bool
    namePointing: bool
    permissionable: bool
    polymorphicForeignKey: bool
    queryByDistance: bool
    restrictedPicklist: bool
    searchPrefilterable: bool
    sortable: bool
    unique: bool
    writeRequiresMasterRead: bool

    # Numeric properties
    byteLength: int
    digits: int
    precision: int
    scale: int

    # String properties
    calculatedFormula: Optional[str]
    compoundFieldName: Optional[str]
    controllerName: Optional[str]
    defaultValue: Optional[str]
    defaultValueFormula: Optional[str]
    extraTypeInfo: Optional[str]
    inlineHelpText: Optional[str]
    mask: Optional[str]
    maskType: Optional[str]
    referenceTargetField: Optional[str]
    relationshipName: Optional[str]
    soapType: str

    # Boolean properties with defaults
    cascadeDelete: bool
    defaultedOnCreate: bool
    displayLocationInDecimal: bool
    restrictedDelete: bool

    # Numeric properties
    relationshipOrder: Optional[int]

    # Array properties
    picklistValues: List[PicklistValue]
    referenceTo: List[str]

    # Object properties
    filteredLookupInfo: Optional[FilteredLookupInfo]


class SObjectDescribe(TypedDict):
    """Complete SObject describe information."""

    # Basic properties
    name: str
    label: str
    labelPlural: str
    keyPrefix: Optional[str]
    custom: bool

    # Capabilities
    activateable: bool
    createable: bool
    deletable: bool
    mergeable: bool
    queryable: bool
    replicateable: bool
    retrieveable: bool
    searchable: bool
    triggerable: bool
    undeletable: bool
    updateable: bool

    # Layout and UI properties
    compactLayoutable: bool
    feedEnabled: bool
    layoutable: bool
    listviewable: bool
    lookupLayoutable: bool
    mruEnabled: bool
    searchLayoutable: bool

    # Advanced properties
    customSetting: bool
    deepCloneable: bool
    deprecatedAndHidden: bool
    hasSubtypes: bool
    isInterface: bool
    isSubtype: bool

    # Optional properties
    associateEntityType: Optional[str]
    associateParentEntity: Optional[str]
    defaultImplementation: Optional[str]
    extendedBy: Optional[str]
    extendsInterfaces: Optional[str]
    implementedBy: Optional[str]
    implementsInterfaces: Optional[str]
    networkScopeFieldName: Optional[str]
    sobjectDescribeOption: Optional[str]

    # Array properties
    actionOverrides: List[ActionOverride]
    childRelationships: List[ChildRelationship]
    fields: List[FieldInfo]
    namedLayoutInfos: List[NamedLayoutInfo]
    recordTypeInfos: List[RecordTypeInfo]
    supportedScopes: List[ScopeInfo]

    # URLs
    urls: Dict[str, str]
