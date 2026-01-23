"""Type aliases for the core engine."""

from collections.abc import Callable, MutableMapping
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Queue as ProcessQueue
from pathlib import Path
from queue import Queue as ThreadQueue
from typing import TYPE_CHECKING, Any, List, Optional, TypeVar, Union  # pylint: disable=W1901

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from typing_extensions import Literal, ParamSpec, get_args

# TODO - cannot remove List from Typing. See L60 for details.

if TYPE_CHECKING:  # pragma: no cover
    from dve.core_engine.message import FeedbackMessage

Field = str
"""The name of a field within a record"""
ErrorValue = Any
"""The value contained in a specific field."""
Record = dict[Field, ErrorValue]
"""A record within an entity."""

PathStr = str
"""A filesystem path, as a string (cursed)."""
URI = str
"""A URI for some local or remote resource."""
FileURI = str
"""URI to the submission file"""
InfoURI = str
"""URI to submission info json file"""
Location = Union[PathStr, Path, URI]
"""
A filesystem or remote location. An annoying, difficult to resolve union
(see `parser.file_handling.service.resolve_location`).
"""

EntityName = str
"""The name of an entity (i.e. an inbound datasource)."""
Entity = DataFrame
"""An entity, with data stored in a Spark DataFrame."""
EntityLocations = MutableMapping[EntityName, URI]
"""
The locations of unparsed entities, which need to be read according to a
provided reader config.

"""
EntityParquetLocations = MutableMapping[EntityName, URI]
"""The locations of entities as Parquet."""
Messages = List["FeedbackMessage"]
"""A queue of messages returned by a process."""
# todo - issue ^^ where converting to list["FeedbackMessage"] breaks get_type_hints in
# todo - base/rules.py:113. Not sure entirely why this is the case atm. Will raise a ticket
# todo - to resolve in the future.

Alias = str
"""A column alias."""
Expression = str
"""An SQL expression."""
ExpressionMapping = dict[Expression, Union[Alias, list[Alias]]]
"""
A mapping of expression to alias. Some expressions (e.g. `posexplode`)
require multiple aliases, which can be passed as a list.

"""
ExpressionArray = list[Expression]
"""An array of expressions with aliases in SQL (e.g. using `expression AS alias`)."""
MultiExpression = str
"""
A sequence of SQL expressions, delimited by a comma.

For example, where a rule requires an ExpressionMapping like so:

```python
expressionmapping = {"UPPER(A)": "A", "MAX(B)": "MAX_B"}
```

This could also be denoted as the following MultiExpression:

```python
multiexpression = "UPPER(A) AS A, MAX(B) AS MAX_B"
```

"""
MultipleExpressions = Union[ExpressionArray, ExpressionMapping, MultiExpression]
"""
Multiple expressions, provided as either a mapping of expression to alias, a list
of expressions, or a string containing multiple comma-delimited SQL expressions.
"""
TransformName = str
"""A name representing a specific transformation."""
ParameterName = str
"""A named parameter for a business rule."""
ParameterDescription = str
"""A human-readable description of the type of value a parameter should contain."""
DeprecationMessage = str
"""A message indicating the reason/context for a rule's deprecation."""

ContractContents = dict[str, Any]
"""A JSON mapping containing the data contract for a dataset."""
SparkSchema = StructType
"""The Spark schema for a given dataset."""

KeyField = Optional[str]
"""The name of the field containing the record field."""
ReportingFields = list[Optional[str]]
"""Field(s) used to identify records without a single identifying field."""
Key = Union[str, dict[str, Any], None]
"""
If no record is attached to the message, then `None`. Otherwise, the value of the record in
`KeyField`, or the whole row as a dict or repr for the consumer to determine how to handle
if `KeyField` is None.
"""
FailureType = Literal["record", "integrity", "submission"]
"""A string indicating the type of failure."""
Status = Literal["informational", "error"]
"""A string indicating whether the error is informational or a true error."""
ErrorEmitValue = Literal[
    "info", "warning", "record_failure", "submission_failure", "critical_failure"
]
"""The new type of the error."""
ErrorType = Optional[str]
"""A string indicating the type of error."""
ErrorLocation = Optional[str]
"""A string indicating the source of the error."""
ErrorMessage = Optional[str]
"""A string indicating the error message."""
ErrorCode = Optional[str]
"""A string indicating the ETOS error code for the error."""
ReportingField = Optional[str]
"""A string indicating the field that the error pertains to."""
FieldValue = Optional[Any]
"""The value that caused the error."""
ErrorCategory = Literal["Blank", "Wrong format", "Bad value", "Bad file"]
"""A string indicating the category of the error."""

MessageTuple = tuple[
    Optional[EntityName],
    Key,
    FailureType,
    Status,
    ErrorType,
    ErrorLocation,
    ErrorMessage,
    ErrorCode,
    ReportingField,
    Optional[FieldValue],
    Optional[ErrorCategory],
]
"""A tuple representing the information from a message."""

MessageKeys = Literal[
    "Entity",
    "Key",
    "FailureType",
    "Status",
    "ErrorType",
    "ErrorLocation",
    "ErrorMessage",
    "Category",
]
"""A union of the keys that can be contained in a message.""" ""
MessageValues = Union[
    Optional[EntityName], Key, FailureType, Status, ErrorType, ErrorLocation, ErrorMessage
]
"""A union of the types of values that can be contained in a message.""" ""

MessageDict = dict[MessageKeys, MessageValues]
"""A dictionary representing the information from a message."""

JSONstring = str
"""Text which can be parsed as JSON."""
JSONBaseType = Union[str, int, float, bool, None]
"""The fundamental allowed types in JSON."""
# mypy doesn't support recursive type definitions.
JSONable = Union[dict[str, "JSONable"], list["JSONable"], JSONBaseType]  # type: ignore
"""A recursive description of the types that come from parsing JSON."""
JSONDict = dict[str, JSONable]  # type: ignore
"""A JSON dictionary."""

Source = DataFrame
"""The source DataFrame for a join."""
Target = DataFrame
"""The target DataFrame for a join."""
Joined = DataFrame
"""A joined DataFrame, consisting of data from `Source` and `Target`."""

TemplateVariableName = str
"""The name of a template variable."""
TemplateVariableValue = Any
"""The value of a template variable."""
TemplateVariables = dict[TemplateVariableName, TemplateVariableValue]
"""Variables for templating."""

FP = ParamSpec("FP")
"""A generic parameter specification for a function or method."""
RT = TypeVar("RT")
"""A generic return type for a function or method."""
ArbitraryFunction = Callable[FP, RT]  # type: ignore
"""A completely generic function or method."""
WrapDecorator = Callable[[ArbitraryFunction], ArbitraryFunction]
"""
A generic decorator which takes a function/method and returns a function/method
with the same signature.
"""

ExecutorType = Union[ProcessPoolExecutor, ThreadPoolExecutor]
"""A type hint for executor type dependent on whether threads or processes being used
to support parallelism"""

Failed = bool
"""Whether or not the submission has failed validation"""

QueueType = Union[ProcessQueue, ThreadQueue]
"""A type hint for queue type dependent on whether threads or processes being used
   to support parallelism"""

AuditTableNames = Literal["processing_status", "submission_info", "transfers"]

ProcessingStatus = Literal[
    "received",
    "running",
    "success",
    "failed",
    "archived",
    "resubmitted",
    "file_transformation",
    "data_contract",
    "business_rules",
    "error_report",
]
"""Allowed statuses for DVE submission"""

PROCESSING_STATUSES: tuple[ProcessingStatus, ...] = tuple(list(get_args(ProcessingStatus)))
"""List of all possible DVE submission statuses"""

SubmissionResult = Literal["success", "validation_failed", "archived", "processing_failed"]
"""Allowed DVE submission results"""

SUBMISSION_RESULTS: tuple[SubmissionResult, ...] = tuple(list(get_args(SubmissionResult)))
"""List of possible DVE submission results"""

BinaryComparator = Callable[[Any, Any], bool]
"""Type hint for operator functions"""
