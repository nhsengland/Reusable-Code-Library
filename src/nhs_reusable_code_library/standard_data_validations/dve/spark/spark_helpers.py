"""Helper functions for working with Spark types.

This is mostly focussed on converting Python type annotations to Spark
types.

"""

import datetime as dt
import logging
import time
from collections.abc import Callable, Generator, Iterator
from dataclasses import dataclass, is_dataclass
from decimal import Decimal
from functools import wraps
from typing import Any, ClassVar, Optional, TypeVar, Union, overload

from delta.exceptions import ConcurrentAppendException, DeltaConcurrentModificationException
from pydantic import BaseModel
from pydantic.types import ConstrainedDecimal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf
from pyspark.sql import types as st
from pyspark.sql.column import Column
from pyspark.sql.functions import lit, udf
from typing_extensions import Annotated, Protocol, TypedDict, get_args, get_origin, get_type_hints

from dve.core_engine.backends.base.utilities import _get_non_heterogenous_type
from dve.core_engine.type_hints import URI

# It would be really nice if there was a more parameterisable
# way of doing this.
OneArgWrappable = Callable[[Any], Any]
"""A wrappable function taking a single arg."""
TwoArgWrappable = Callable[[Any, Any], Any]
"""A wrappable function taking two args."""
ThreeArgWrappable = Callable[[Any, Any, Any], Any]
"""A wrappable function taking three args."""
FourArgWrappable = Callable[[Any, Any, Any, Any], Any]
"""A wrappable function taking four args."""
OneArgWrapped = Callable[[Column], Column]
"""A wrapped function (Spark UDF) taking a single arg."""
TwoArgWrapped = Callable[[Column, Column], Column]
"""A wrapped function (Spark UDF) taking two args."""
ThreeArgWrapped = Callable[[Column, Column, Column], Column]
"""A wrapped function (Spark UDF) taking three args."""
FourArgWrapped = Callable[[Column, Column, Column, Column], Column]
"""A wrapped function (Spark UDF) taking four args."""


@dataclass(frozen=True)
class DecimalConfig:
    """Configuration for a Python decimal to enable it to be mapped to a
    Spark `DecimalType`. This should be used with `typing_extensions.Annotated`
    (e.g. `typing_extensions.Annotated[decimal.Decimal, DecimalConfig(10, 0)]`)
    to specify a precision and scale for returned decimals.

    The defaults for the arguments are the defaults used by Spark when
    inferring a schema from Python objects.

    """

    precision: int = 38
    """
    The precision of the decimal. This is the total number of digits in the
    decimal.

    """
    scale: int = 18
    """
    The scale of the decimal. This is the number of digits to the right of the
    decimal point.

    """

    def __post_init__(self):
        if not 0 < self.precision <= 38:
            raise ValueError("Precision must be between 1 and 38 (inclusive)")
        if not 0 <= self.scale <= self.precision:
            raise ValueError("Scale must be between 0 and the precision (inclusive)")


DEFAULT_DECIMAL_CONFIG = DecimalConfig()
"""The defualt decimal precision/scale config."""


PYTHON_TYPE_TO_SPARK_TYPE: dict[type, st.DataType] = {
    str: st.StringType(),
    int: st.LongType(),
    bool: st.BooleanType(),
    float: st.DoubleType(),
    bytes: st.BinaryType(),
    dt.date: st.DateType(),
    dt.datetime: st.TimestampType(),
    Decimal: st.DecimalType(DEFAULT_DECIMAL_CONFIG.precision, DEFAULT_DECIMAL_CONFIG.scale),
}
"""A mapping of Python types to the equivalent Spark types."""


PydanticModel = TypeVar("PydanticModel", bound=BaseModel)
"""An Pydantic model."""
TypedDictSubclass = TypeVar("TypedDictSubclass", bound=type[TypedDict])  # type: ignore
"""A TypedDict subclass."""


class GenericDataclass(Protocol):  # pylint: disable=too-few-public-methods
    """A dataclass-like class."""

    __dataclass_fields__: dict[str, Any]  # `is_dataclass` checks for this field.


@overload
def get_type_from_annotation(
    type_annotation: Union[
        type[PydanticModel], type[GenericDataclass], GenericDataclass, TypedDictSubclass
    ],
) -> st.StructType:
    pass  # pragma: no cover


@overload
def get_type_from_annotation(type_annotation: Any) -> st.DataType:
    pass  # pragma: no cover


# pylint: disable=too-many-return-statements,too-many-branches
def get_type_from_annotation(type_annotation: Any) -> st.DataType:
    """Get a Spark type from a Python type annotation.

    Supported types  are any of the following (this definition is recursive):
    - Supported basic Python types. These are:
      * `str`: a Spark `StringType`
      * `int`: a Spark `LongType` (bigint)
      * `bool`: a Spark `BooleanType`
      * `float`: a Spark `DoubleType`
      * `bytes`: a Spark `BinaryType`
      * `datetime.date`: a Spark `DateType`
      * `datetime.datetime`: a Spark `TimestampType`
      * `decimal.Decimal`: a Spark `DecimalType` with precision of 38 and scale
        of 18
    - A list of supported types (e.g. `list[str]` or `typing.list[str]`).
      This will return a Spark `ArrayType` with the specified element type.
    - A `typing.Optional` type or a `typing.Union` of the type and `None` (e.g.
      `typing.Optional[str]`, `typing.Union[list[str], None]`). This will remove the
      'optional' wrapper and return the inner type (Spark types are all nullable).
    - A subclass of `typing.TypedDict` with values typed using supported types. This
      will parse the value types as Spark types and return a Spark `StructType`.
    - A dataclass or `pydantic.main.ModelMetaClass` with values typed using supported types.
      This will parse the field types as Spark types and return a Spark `StructType`.
    - Any supported type, with a `typing_extensions.Annotated` wrapper.
    - A `decimal.Decimal` wrapped with `typing_extensions.Annotated` with a `DecimalConfig`
      indicating precision and scale. This will return a Spark `DecimalType`
      with the specfied scale and precision.
    - A `pydantic.types.condecimal` created type.

    Any `ClassVar` types within `TypedDict`s, dataclasses, or `pydantic` models will be
    ignored.

    """
    type_origin = get_origin(type_annotation)

    # An `Optional` or `Union` type, check to ensure non-heterogenity.
    if type_origin is Union:
        python_type = _get_non_heterogenous_type(get_args(type_annotation))
        return get_type_from_annotation(python_type)

    # type hint is e.g. `list[str]`, check to ensure non-heterogenity.
    if type_origin is list or (isinstance(type_origin, type) and issubclass(type_origin, list)):
        element_type = _get_non_heterogenous_type(get_args(type_annotation))
        return st.ArrayType(get_type_from_annotation(element_type))

    # Possibly a decimal.
    if type_origin is Annotated:
        python_type, *other_args = get_args(type_annotation)
        if python_type is not Decimal:
            return get_type_from_annotation(python_type)

        try:  # Grab the decimal configuration from the list of other args.
            configuration: DecimalConfig = next(
                filter(lambda config: isinstance(config, DecimalConfig), other_args)
            )
        except StopIteration:
            configuration = DEFAULT_DECIMAL_CONFIG
        return st.DecimalType(configuration.precision, configuration.scale)

    # Ensure that we have a concrete type at this point.
    if not isinstance(type_annotation, type):
        raise ValueError(f"Unsupported type annotation {type_annotation!r}")

    if (
        # type hint is a dict subclass, but not dict. Possibly a `TypedDict`.
        (issubclass(type_annotation, dict) and type_annotation is not dict)
        # type hint is a dataclass.
        or is_dataclass(type_annotation)
        # type hint is a `pydantic` model.
        or (type_origin is None and issubclass(type_annotation, BaseModel))
    ):
        fields = []
        for field_name, field_annotation in get_type_hints(type_annotation).items():
            # Technically non-string keys are disallowed, but people are bad.
            if not isinstance(field_name, str):
                raise ValueError(
                    f"Dictionary/Dataclass keys must be strings, got {type_annotation!r}"
                )  # pragma: no cover
            if get_origin(field_annotation) is ClassVar:
                continue

            field = st.StructField(field_name, get_type_from_annotation(field_annotation))
            fields.append(field)

        if not fields:
            raise ValueError(
                f"No type annotations in dict/dataclass type (got {type_annotation!r})"
            )

        return st.StructType(fields)

    if issubclass(type_annotation, ConstrainedDecimal):
        precision = int(type_annotation.max_digits or 38)
        scale = int(type_annotation.decimal_places or precision)
        return st.DecimalType(precision, scale)

    if type_annotation is list:
        raise ValueError(
            f"list must have type annotation (e.g. `list[str]`), got {type_annotation!r}"
        )
    if type_annotation is dict or type_origin is dict:
        raise ValueError(f"dict must be `typing.TypedDict` subclass, got {type_annotation!r}")

    for type_ in type_annotation.mro():
        spark_type = PYTHON_TYPE_TO_SPARK_TYPE.get(type_)
        if spark_type:
            return spark_type
    raise ValueError(f"No equivalent Spark type for {type_annotation!r}")


@overload
def create_udf(function: OneArgWrappable) -> OneArgWrapped:  # pragma: no cover
    pass


@overload
def create_udf(function: TwoArgWrappable) -> TwoArgWrapped:  # pragma: no cover
    pass


@overload
def create_udf(function: ThreeArgWrappable) -> ThreeArgWrapped:  # pragma: no cover
    pass


@overload
def create_udf(function: FourArgWrappable) -> FourArgWrapped:  # pragma: no cover
    pass


def create_udf(function: Callable) -> Callable:
    """Get a UDF representing a specific function."""
    if not callable(function):
        raise ValueError("Supplied 'function' is not callable")

    try:
        annotations = get_type_hints(function)
        return_type_annotation = annotations["return"]
    except (AttributeError, KeyError) as err:
        raise ValueError(f"Function {function.__name__} is not annotated") from err

    return_type = get_type_from_annotation(return_type_annotation)
    return udf(function, returnType=return_type)


SupportedBaseType = Union[str, int, bool, float, Decimal, dt.date, dt.datetime]
"""Supported base types for Spark literals."""
SparkLiteralType = Union[  # type: ignore
    SupportedBaseType, dict[str, "SparkLiteralType"], list["SparkLiteralType"]  # type: ignore
]
"""Recursive definition of supported literal types."""


def object_to_spark_literal(obj: SparkLiteralType) -> Column:
    """Convert a Python object to a Spark literal `Column`.

    Where lists are provided, these _must_ be 'deeply' homogenous.
    Dicts within lists must contain all the same keys, and the values for the same
    key must be of the same type.

    """
    if obj is None:
        return sf.lit(None)
    if isinstance(obj, (str, int, bool, float, Decimal, dt.date, dt.datetime)):
        return sf.lit(obj)

    if not isinstance(obj, (dict, list)):
        raise TypeError(type(obj))

    if isinstance(obj, dict):
        struct_columns = []
        for key, value in obj.items():
            struct_columns.append(object_to_spark_literal(value).alias(key))
        return sf.struct(*struct_columns)

    array_elements = []
    arr_element_type: Optional[type] = None
    arr_element_keys: Optional[set[str]] = None

    for element in obj:
        element_type = type(element)

        if arr_element_type is None:
            arr_element_type = element_type
        elif element_type is not arr_element_type and element is not None:
            raise ValueError("Spark does not support heterogeneous array types")

        if element_type is dict:
            element_keys = set(element.keys())  # type: ignore

            if arr_element_keys is None:
                arr_element_keys = element_keys
            elif element_keys != arr_element_keys:
                raise ValueError("`dict`s within `list` have different sets of keys")

        array_elements.append(object_to_spark_literal(element))

    return sf.array(*array_elements)


def df_is_empty(dataframe: DataFrame) -> bool:
    """Return whether a DataFrame is empty."""
    return not dataframe.select(lit(1)).head(1)


def _spark_read_parquet(self, path: URI, **kwargs) -> DataFrame:
    """Read entity from a parquet file."""
    return self.spark_session.read.options(**kwargs).parquet(path)


def _spark_write_parquet(  # pylint: disable=unused-argument
    self, entity: Union[Iterator[dict[str, Any]], DataFrame], target_location: URI, **kwargs
) -> URI:
    """Method to write parquet files from type cast entities
    following data contract application
    """
    _options: dict[str, Any] = {**kwargs}
    if isinstance(entity, Generator):
        _writer = self.spark_session.createDataFrame(entity).write
    else:
        _options["schema"] = entity.schema  # type: ignore
        _writer = entity.write  # type: ignore

    (_writer.options(**_options).format("parquet").mode("overwrite").save(target_location))
    return target_location


def spark_read_parquet(cls):
    """Class decorator to add read_parquet method for spark implementations"""
    cls.read_parquet = _spark_read_parquet

    return cls


def spark_write_parquet(cls):
    """Class decorator to add write_parquet method for spark implementations"""
    cls.write_parquet = _spark_write_parquet
    return cls


@staticmethod  # type: ignore
def _spark_get_entity_count(entity: DataFrame) -> int:
    """Method to obtain entity count from a persisted parquet entity"""
    return entity.count()


def spark_get_entity_count(cls):
    """Class decorator to count records in an entity supplied"""
    cls.get_entity_count = _spark_get_entity_count
    return cls


def get_all_registered_udfs(spark: SparkSession) -> set[str]:
    """Function to supply the names of a registered functions stored in the supplied
    spark session.
    """
    return {rw.function for rw in spark.sql("SHOW USER FUNCTIONS").collect()}


def audit_retry(max_retries: int = 60) -> Callable:
    """Crude decorator to avoid audit table race conditions"""

    def _wrapper(func: Callable) -> Callable:
        """Wrapper"""
        _logger = logging.getLogger()

        @wraps(func)
        def _inner(*args, **kwargs):
            """Inner"""
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (DeltaConcurrentModificationException, ConcurrentAppendException) as err:
                    _logger.warning(f"Running {func.__name__}")
                    _logger.warning(f"Audit finalisation attempt {attempt} failed")
                    if attempt % 10 == 0 and attempt > 0:
                        _logger.warning(f"with err: {err}")
                    time.sleep(1)
            _logger.error(
                f"Failed to finalise audit entry for submission_id:"
                f" {kwargs.get('submission_id')}"
            )
            return None

        return _inner

    return _wrapper
