"""
A (mostly) drop in replacement for PySpark's `StructType` which is immutable.
Operations return new instances and there is better support for selecting column
subsets.

These also have a sort method, which is useful for UDF return types.

"""
import json
from textwrap import dedent
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    overload,
    TypeVar,
    Union,
)
from warnings import warn

import pyspark.sql.types
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit  # pylint: disable=no-name-in-module
from pyspark.sql.types import (
    ArrayType,
    DataType,
    StructType,
    StructField,
)

_parse_datatype_json_value: Callable[[str], DataType]
from pyspark.sql.types import _parse_datatype_json_value  # type: ignore

from nhs_reusable_code_library.resuable_codes.spark_helpers import (
    build_fq_table_name,
    datatypes_equal,
    table_exists_acl_safe,
)

SparkType = TypeVar("SparkType", bound=DataType)  # pylint: disable=invalid-name

# mypy doesn't yet support recursive type definitions, sadly.
JSONElement = Union[str, Dict[str, "JSONElement"], List["JSONElement"]]  # type: ignore
JSONDict = Dict[str, JSONElement]  # type: ignore


T = TypeVar("T")


def _iter_notify_last(iterable: Iterable[T]) -> Iterator[Tuple[T, bool]]:
    """
    Iterate through an iterable, yielding tuples with an element from the
    iterable and a boolean indicating whether the element is the last one.

    """
    iterator = iter(iterable)

    try:
        element = next(iterator)
    except StopIteration:  # pragma: no cover
        return

    for next_element in iterator:
        yield element, False
        element = next_element
    yield element, True


class Schema(StructType):  # pylint: disable=too-many-public-methods
    """
    A `StructType` compatible(ish) table schema with immutable semantics.

    This essentially is a drop in replacement for `StructType` which adds some
    functionality and changes modification operations to return new instances.

    These support some operators which standard `StructType` does not:

    The `+` operator can be used to concatenate two schemas end to end.
    This would be similar to something like this for two `StructTypes`:

    ```python
    StructType(structtype1.fields + structtype2.fields)
    # vs.
    schema1 + schema2
    ```

    The `|` operator can be used to merge fields from two schemas.

    ```python
    StructType(set(structtype1.fields) | set(structtype2.fields))
    # vs.
    schema1 | schema2  # Order not guaranteed.
    ```

    The `&` operator can be used to take only fields which are in
    both schemas.

    ```python
    StructType(set(structtype1.fields) & set(structtype2.fields))
    # vs.
    schema1 & schema2  # Order not guaranteed.
    ```

    The `-` operator can be used to remove fields from a schema which
    occur in another schema.

    ```python
    names = structtype2.names
    StructType([field for field in structtype1.fields if field.name not in names])
    # vs.
    schema1 - schema2
    ```

    """

    def __init__(  # pylint: disable=super-init-not-called
        self, fields: Optional[Union[StructType, Iterable[StructField]]] = None
    ):
        self._fields: Dict[str, StructField] = {}
        self._needConversion: List[bool] = []

        seen_fields = set()
        for field in fields or []:
            if not isinstance(field, StructField):
                raise TypeError(
                    "`fields` should be an iterable of `StructField`, "
                    + f"got {type(field)!r}"
                )

            upper_name = field.name.upper()
            if upper_name in seen_fields:
                raise ValueError(f"Duplicate entry for '{field.name!r}' in `fields`")
            seen_fields.add(upper_name)

            field = _convert_structs(field, self.__class__)
            self._fields[field.name] = field
            self._needConversion.append(field.needConversion())

        self._needSerializeAnyField = any(self._needConversion)
        self._register_type()

    def _register_type(self):
        """
        Register the schema type so that it can be used to create Spark
        DataFrames.

        This is probably considered a sin, but it's PySpark's fault for stupidly
        checking `type(x) in _acceptable_types` instead of `isinstance(x, DataType)`
        in a *DYNAMIC, OBJECT-ORIENTED PROGRAMMING LANGUAGE*.

        """
        # pylint: disable=protected-access
        if self.__class__ in pyspark.sql.types._acceptable_types:  # type: ignore
            return

        try:
            # pylint: disable=protected-access
            current_types = pyspark.sql.types._acceptable_types[StructType]  # type: ignore
            # pylint: disable=protected-access
            pyspark.sql.types._acceptable_types[self.__class__] = current_types  # type: ignore
        except AttributeError:  # pragma: no cover
            pass

    @property
    def fields(self) -> List[StructField]:  # type: ignore
        """A list of `StructField` types in the schema."""
        return list(self._fields.values())

    @property
    def names(self) -> List[str]:  # type: ignore
        """A list of field names in the schema."""
        return list(self._fields.keys())

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Schema):
            return self.normalise().fields == other.normalise().fields
        # Does it quack like a `StructType`?
        if hasattr(other, "fields"):
            fields = self.normalise().fields
            return (
                fields == _transform_identifiers(other, schema_type=self.__class__).fields
            )

        return NotImplemented  # pragma: no cover

    def __repr__(self) -> str:  # pragma: no cover
        fields_string = ", ".join(map(str, self))
        return f"Schema({fields_string})"

    def __iter__(self) -> Iterator[StructField]:
        return iter(self._fields.values())

    def __len__(self) -> int:
        return len(self._fields)

    def __contains__(self, key: str) -> bool:
        if not isinstance(key, str):
            raise TypeError(f"Key {key!r} must be string")

        try:
            self.get(key)
        except KeyError:
            return False
        return True

    # TODO: Make add/and/or/sub 'struct aware'.

    def __add__(self, other: Any) -> "Schema":
        try:
            other_fields: List[StructField] = other.fields
        except AttributeError:  # pragma: no cover
            return NotImplemented

        return self.__class__(self.fields + other_fields)

    def __radd__(self, other: Any) -> "Schema":
        try:
            other_fields: List[StructField] = other.fields
        except AttributeError:  # pragma: no cover
            return NotImplemented

        return self.__class__(other_fields + self.fields)

    def __and__(self, other: Any) -> "Schema":
        try:
            names: List[str] = other.names
        except AttributeError:  # pragma: no cover
            return NotImplemented

        return self.select(*filter(self._fields.__contains__, names))

    def __rand__(self, other: Any) -> "Schema":
        try:
            other_fields: List[StructField] = other.fields
        except AttributeError:  # pragma: no cover
            return NotImplemented

        field_dict = {field.name: field for field in other_fields}
        fields = [field_dict[key] for key in self.names if key in field_dict]
        return self.__class__(fields)

    def __or__(self, other: Any) -> "Schema":
        try:
            other_fields: List[StructField] = other.fields
        except AttributeError:  # pragma: no cover
            return NotImplemented

        all_fields = {field.name: field for field in other_fields}
        all_fields.update(self._fields)
        return self.__class__(all_fields.values())

    def __ror__(self, other: Any) -> "Schema":
        try:
            other_fields: List[StructField] = other.fields
        except AttributeError:  # pragma: no cover
            return NotImplemented

        all_fields = self._fields.copy()
        all_fields.update({field.name: field for field in other_fields})
        return self.__class__(all_fields.values())

    def __sub__(self, other: Any) -> "Schema":
        try:
            other_fields: List[StructField] = other.fields
        except AttributeError:  # pragma: no cover
            return NotImplemented

        field_names = {field.name for field in other_fields}
        fields = []
        for name, field in self._fields.items():
            if name not in field_names:
                fields.append(field)
        return self.__class__(fields)

    def __rsub__(self, other: Any) -> "Schema":
        try:
            other_fields: List[StructField] = other.fields
        except AttributeError:  # pragma: no cover
            return NotImplemented

        fields = [field for field in other_fields if field.name not in self._fields]
        return self.__class__(fields)

    @overload
    def __getitem__(self, key: Union[int, str]) -> StructField:
        ...  # pragma: no cover

    @overload
    def __getitem__(self, key: slice) -> List[StructField]:
        ...  # pragma: no cover

    @overload
    def __getitem__(
        self, key: Union[Tuple[int, ...], Tuple[str, ...]]
    ) -> List[StructField]:
        ...  # pragma: no cover

    @overload
    def __getitem__(self, key: Union[List[int], List[str]]) -> "Schema":
        ...  # pragma: no cover

    def __getitem__(self, key):
        if isinstance(key, (int, str)):
            return self.get(key)

        if isinstance(key, slice):
            fields: List[StructField] = self.fields[key]
            return fields

        if isinstance(key, (tuple, list)):
            is_string = lambda item: isinstance(item, str)  # pylint:disable=C3001
            is_int = lambda item: isinstance(item, int)  # pylint: disable=C3001

            if all(map(is_int, key)):
                current_fields = self.fields
                fields = [current_fields[index] for index in key]
            elif all(map(is_string, key)):
                fields = [self.get(item) for item in key]
            else:
                raise TypeError("Iterable key must not mix string and int types")

            if isinstance(key, tuple):
                return fields
            if isinstance(key, list):
                return self.__class__(fields)

        raise TypeError(
            "Key must be int, string, slice, tuple/list of ints or tuple/list of strings"
        )

    @staticmethod
    def _build_field(
        field: Union[StructField, str],
        data_type: Optional[Union[DataType, str]] = None,
        nullable: bool = True,
        metadata: Optional[Dict[str, str]] = None,
    ) -> StructField:
        """Return a `StructField` from params passed to add/replace."""
        if not isinstance(field, StructField):
            if data_type is None:
                raise ValueError(
                    "Must specify DataType if passing name of struct field to create"
                )

            if not isinstance(data_type, DataType):
                data_type = _parse_datatype_json_value(data_type)
            if isinstance(data_type, StructField):
                data_type = data_type.dataType

            field = StructField(field, data_type, nullable, metadata)
        return field

    def _get_nested_field(self, key: str) -> StructField:
        """Get a nested field from the schema, returning it as a `StructField`."""
        current_level: Schema = self
        visited_levels: List[str] = []
        error_extra = None

        # Navigate the tree.
        for next_level, is_top_level in _iter_notify_last(key.split(".")):
            try:
                field = current_level.get(next_level)
            except KeyError:
                reached = "root"
                if visited_levels:
                    reached = repr(".".join(visited_levels))
                error_extra = f"no {next_level!r} in {reached}"
                break

            if is_top_level:
                return field

            visited_levels.append(next_level)
            if not isinstance(field.dataType, Schema):
                reached = ".".join(visited_levels)
                error_extra = f"{reached!r} exists, but is not a schema"
                break

            current_level = field.dataType

        message = f"No `StructField` named {key!r}"
        if error_extra is not None:
            message = f"{message} ({error_extra})"

        raise KeyError(message)

    def get(self, key: Union[int, str]) -> StructField:
        """Get a field from the Schema as a `StructField`."""
        if isinstance(key, str):
            if "." not in key:
                try:
                    return self._fields[key]
                except KeyError as err:
                    raise KeyError(f"No `StructField` named {key!r}") from err
            return self._get_nested_field(key)

        if isinstance(key, int):
            try:
                return self.fields[key]
            except IndexError as err:
                raise IndexError("Schema index out of range") from err

        raise TypeError(f"Unsuported key type {type(key)}")

    def get_nested(self, key: Union[str, int]) -> "Schema":
        """Get the schema of a struct within the Schema, by key."""
        field = self.get(key)
        if not isinstance(field.dataType, Schema):
            raise ValueError(f"{key!r} is not a schema (is {type(field.dataType)})")
        return field.dataType

    def add(  # type: ignore
        self,
        field: Union[StructField, str],
        data_type: Optional[Union[DataType, str]] = None,
        nullable: bool = True,
        metadata: Optional[Dict[str, str]] = None,
    ) -> "Schema":
        """
        Add a field, returning a new schema with the field.

        Args:
         - field: the name of the new field, or the field itself.
         - data_type: the Spark DataType of the new field
         - nullable: a bool indicating whether the field should be nullable
         - metadata: optional field metadata (e.g. to add comments)

        The last three args are ignored if `field` is a `StructField`.

        """
        field = self._build_field(field, data_type, nullable, metadata)
        return self.__class__(self.fields + [field])

    def replace(
        self,
        field: Union[StructField, str],
        data_type: Optional[Union[DataType, str]] = None,
        nullable: bool = True,
        metadata: Optional[Dict[str, str]] = None,
    ) -> "Schema":
        """
        Replace a field, returning a new schema with the replaced field.

        Args:
         - field: the name of the new field, or the field itself.
         - data_type: the Spark DataType of the new field
         - nullable: a bool indicating whether the field should be nullable
         - metadata: optional field metadata (e.g. to add comments)

        The last three args are ignored if `field` is a `StructField`.

        """

        field = self._build_field(field, data_type, nullable, metadata)
        if field.name not in self.names:
            raise KeyError(f"Can't replace {field.name!r}, field not currently in schema")
        fields = []

        for field_name, current_field in self._fields.items():
            if field_name == field.name:
                fields.append(field)
            else:
                fields.append(current_field)

        return self.__class__(fields)

    def rename(self, **name_mapping: str) -> "Schema":
        """
        Rename fields in the schema, retaining their order, and return a new
        schema.

        This can be used to rename nested struct items.

        """
        # First need to grab the different levels from the name dict
        # E.g. {"META": "Metadata", "META.Event_ID": "Event_Key", "Name": "Forename"}
        # Should become:
        # {"META": {None: "Metadata", "Event_ID": "Event_Key"}, "Name": {None: "Forename"}}
        nested_mapping: Dict[str, Dict[Optional[str], str]] = {}
        for name, replacement in name_mapping.items():
            if "." in replacement:
                raise ValueError(
                    f"Replacement names must be unqualified (got {replacement}, "
                    f"but need e.g. {replacement.split('.')[-1]})"
                )

            level = None
            if "." in name:
                name, level = name.split(".", 1)

            if name not in nested_mapping:
                nested_mapping[name] = {}
            nested_mapping[name][level] = replacement

        fields = []
        for field_name, field in self._fields.items():
            replacements = nested_mapping.pop(field_name, None)
            if replacements is None:
                fields.append(field)
                continue

            new_name = replacements.pop(None, None)
            new_data_type = None
            if replacements:
                if not isinstance(field.dataType, Schema):
                    raise ValueError(
                        f"Can't replace names in {field_name!r}: not a `Schema`"
                    )
                new_data_type = field.dataType.rename(**replacements)  # type: ignore

            field = StructField(
                new_name or field.name,
                new_data_type or field.dataType,
                field.nullable,
                field.metadata,
            )
            fields.append(field)

        return self.__class__(fields)

    def select(self, *names: str) -> "Schema":
        """Select fields, returning a new `Schema`."""
        return self[list(names)]

    def drop(self, *names: str) -> "Schema":
        """Remove fields, returning a new schema without them."""
        to_delete = set()
        to_prune: Dict[str, Set[str]] = {}

        for name in names:
            if "." not in name:
                to_delete.add(name)
            else:
                name, level = name.split(".", 1)
                if name not in to_prune:
                    to_prune[name] = set()
                to_prune[name].add(level)

        fields = []
        for field_name, field in self._fields.items():
            if field_name in to_delete:
                continue
            if field_name in to_prune and isinstance(field.dataType, Schema):
                field = StructField(
                    field.name,
                    field.dataType.drop(*to_prune[field_name]),
                    field.nullable,
                    field.metadata,
                )
            fields.append(field)

        return self.__class__(fields)

    def sorted(self, sort_nested: bool = True) -> "Schema":
        """Sort the schema, returning a new schema."""
        fields = self.fields
        fields.sort(key=lambda field: field.name)

        if sort_nested:
            for index, field in enumerate(fields):
                if isinstance(field.dataType, Schema):
                    new_field = StructField(
                        field.name,
                        field.dataType.sorted(),
                        field.nullable,
                        field.metadata,
                    )
                    fields[index] = new_field

        return self.__class__(fields)

    def normalise(self, transform: Callable[[str], str] = str.upper) -> "Schema":
        """Return a new `Schema` with normalised identifiers."""
        return _transform_identifiers(self, transform, self.__class__)

    def validate(self, df: DataFrame) -> bool:  # pylint: disable=invalid-name
        """Validate a DataFrame schema."""
        return self == df.schema

    def coerce(self, df: DataFrame) -> DataFrame:  # pylint: disable=invalid-name
        """
        Coerce a DataFrame to match the schema.

        This does not currently work for complex types like structs or arrays
        of structs. The former would be possible to implement, the latter would
        be trickier (if at all possible).

        """
        # TODO: Implement coersion for structs, error for arrays of structs.
        current_schema = self.__class__(df.schema).normalise(str.upper)
        current_fields = {field.name: field for field in current_schema}

        columns, non_nullable = [], []
        for field in self.fields:
            field_name, field_type = field.name, field.dataType

            field_upper = field_name.upper()
            if field_upper in current_fields:
                if current_fields[field_upper].nullable and not field.nullable:
                    non_nullable.append(field_name)
                column = col(field.name)
            else:
                if not field.nullable:
                    raise ValueError(
                        f"Field {field_name!r} missing from df, not nullable in schema"
                    )
                column = lit(None)
            columns.append(column.cast(field_type).alias(field_name))

        df = df.select(*columns)
        if non_nullable:
            warn(
                f"Fields {non_nullable!r} nullable in source df, not nullable in "
                "schema. Dropping nulls in these columns"
            )
            df = df.dropna(how="any", subset=non_nullable)

        return df

    def create_as_table(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        spark_session: SparkSession,
        database: Optional[str] = None,
        table_name: str = "",
        table_format: str = "delta",
        exist_ok: bool = True,
        check_nullability: bool = False,
        auto_optimize: bool = False,
        auto_compact: bool = False,
    ):
        """
        Create an empty table with the schema.

        If `exist_ok` is True and the table exists, `check_nullability`
        controls whether field nullability is checked between the schema
        and the existing table. This is set to `False` by default as few
        Hive table formats actually preserve this information.

        `auto_optimize` and `auto_compact` are flags which support some
        automatic optimisation features for Delta tables. These usually
        incur penalties on write to make querying more efficient:
        https://docs.databricks.com/delta/optimizations/auto-optimize.html

        """
        try:
            fq_table_name = build_fq_table_name(database, table_name)
        except ValueError as err:
            raise ValueError("Unable to build fully qualified table name") from err

        mode = "error"
        if table_exists_acl_safe(spark_session, None, fq_table_name, False):
            if not exist_ok:
                raise ValueError(f"Table {fq_table_name!r} already exists")

            table_schema = self.__class__(
                spark_session.table(fq_table_name).schema
            ).normalise()

            if not datatypes_equal(self.normalise(), table_schema, check_nullability):
                raise ValueError(
                    f"Schema does not match existing schema of {fq_table_name!r}"
                )
            mode = "append"

        empty_df: DataFrame = spark_session.createDataFrame([], self)
        try:
            empty_df.write.saveAsTable(fq_table_name, mode=mode, format=table_format)
        except Exception as err:
            raise ValueError(
                f"Unable to create table {fq_table_name!r} with specified format"
            ) from err

        for flag, option in [
            (auto_optimize, "optimizeWrite"),
            (auto_compact, "autoCompact"),
        ]:
            if flag:
                if table_format.lower() != "delta":
                    warn(f"{option} has no effect for non-Delta tables.", UserWarning)
                    continue

                spark_session.sql(
                    dedent(
                        f"""\
                        ALTER TABLE {fq_table_name}
                        SET TBLPROPERTIES (delta.autoOptimize.{option} = true)
                        """
                    )
                )

    @classmethod
    def from_dict(cls, json_dict: JSONDict) -> "Schema":
        """Create a class from a parsed dict representation."""
        return cls.fromJson(json_dict)

    def to_dict(self) -> JSONDict:
        """
        Return a dict represenation of the string, which can be serialised
        to JSON.

        """
        return self.jsonValue()

    @classmethod
    def from_json_string(cls, json_string: str) -> "Schema":
        """Parse the `Schema` from a JSON string."""
        return cls.from_dict(json.loads(json_string))

    def to_json_string(self, indent: Optional[int] = 4) -> str:
        """Serialise the `Schema` to a JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_keys(cls, **columns_types: DataType) -> "Schema":
        """Create a `Schema` from names/types."""
        fields = []
        for name, field_type in columns_types.items():
            fields.append(StructField(name, field_type))
        return cls(fields)

    @classmethod
    def fromJson(
        cls, json: Dict[str, Any]  # pylint: disable=redefined-outer-name
    ) -> "Schema":
        # If they'd just _used `cls`_ we wouldn't need to do this.
        return cls(super().fromJson(json))

    @classmethod  # I hate this abuse of classmethod.
    def typeName(cls) -> str:
        return "struct"  # Otherwise this can't be parsed from JSON.


def _convert_structs(
    data_type: SparkType, schema_type: Type["Schema"] = Schema
) -> SparkType:
    """
    Convert a PySpark DataType to use Schema types instead of StructTypes.

    This needs to be done recursively, as complex types can be nested.

    """
    # mypy doesn't infer these types well, but in all cases the returned
    # types will be compatible with the supplied types.
    if isinstance(data_type, Schema):
        return data_type  # type: ignore

    if isinstance(data_type, StructType):
        return schema_type(data_type)  # type: ignore

    if isinstance(data_type, ArrayType):
        return ArrayType(  # type: ignore
            _convert_structs(data_type.elementType, schema_type), data_type.containsNull
        )

    if isinstance(data_type, StructField):
        return StructField(  # type: ignore
            data_type.name,
            _convert_structs(data_type.dataType, schema_type),
            data_type.nullable,
            data_type.metadata,
        )

    return data_type


def _transform_identifiers(
    data_type: SparkType,
    transform: Callable[[str], str] = str.upper,
    schema_type: Type[Schema] = Schema,
) -> SparkType:
    """Transform the identifiers in a Spark DataType. Default: uppercase."""
    if isinstance(data_type, StructType):
        return schema_type(  # type: ignore
            [
                _transform_identifiers(field, transform, schema_type)
                for field in data_type.fields
            ]
        )

    if isinstance(data_type, ArrayType):
        return ArrayType(  # type: ignore
            _transform_identifiers(data_type.elementType, transform, schema_type),
            data_type.containsNull,
        )

    if isinstance(data_type, StructField):
        return StructField(  # type: ignore
            transform(data_type.name),
            _transform_identifiers(data_type.dataType, transform, schema_type),
            data_type.nullable,
            data_type.metadata,
        )

    return data_type
