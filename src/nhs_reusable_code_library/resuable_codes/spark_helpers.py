import os
import json
import re
import string
import dateutil.parser
import pyspark.sql.utils
from datetime import datetime, date, time
from decimal import Decimal
from typing import AbstractSet, Any, Callable, Tuple, Union, Dict, Optional, List, Iterable, Set
from uuid import uuid4

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, lit, udf, expr, upper
from pyspark.sql.types import (
    DataType, ArrayType, StringType, TimestampType, DecimalType, StructType, StructField, Row, DateType, LongType
)

from dsp.common.relational import Table, Field, JoinParameters, JoinChain, TableField
from dsp.common.structured_model import Decimal_5_2, Decimal_10_2, Decimal_14_5, Decimal_15_2, Decimal_19_0, \
    Decimal_12_6, Decimal_16_6
from dsp.shared.logger import log_action
from dsp.shared.aws import local_mode, s3_cleanup, s3_split_path

_PY_TYPES_TO_SPARK_TYPES: Dict[type, Callable[[Any], DataType]] = {
    str: lambda _: StringType(),
    datetime: lambda _: TimestampType(),
    date: lambda _: DateType(),
    # DecimalType precision + scale must be < 20
    Decimal: lambda _: DecimalType(17),
    Decimal_5_2: lambda _: DecimalType(5, 2),
    Decimal_10_2: lambda _: DecimalType(10, 2),
    Decimal_14_5: lambda _: DecimalType(14, 5),
    Decimal_15_2: lambda _: DecimalType(15, 2),
    Decimal_19_0: lambda _: DecimalType(19, 0),
    Decimal_12_6: lambda _: DecimalType(12, 6),
    Decimal_16_6: lambda _: DecimalType(16, 6),
    int: lambda _: LongType(),
    time: lambda _: TimestampType(),
}


def dataframe_empty(dataframe: DataFrame) -> bool:
    """
    Check whether a dataframe contains any rows

    Args:
        dataframe: The dataframe to check for rows

    Returns:
        Whether the given dataframe is empty
    """
    return dataframe.select(lit(1)).head() is None


def to_spark_type(data_type: type, type_mapping: dict = None) -> DataType:
    """
    Retrieve the corresponding Spark datatype for a given Python type

    Args:
        data_type (type): The type to be converted
        type_mapping (dict): Dictionary of type conversions

    Returns:
        DataType: The corresponding Spark datatype

    Raises:
        ValueError: If there is no registered Spark datatype associated with the given Python datatype
    """
    if type_mapping is None:
        type_mapping = _PY_TYPES_TO_SPARK_TYPES

    try:
        return type_mapping[data_type](data_type)
    except KeyError:
        raise ValueError("No Spark type associated with Python type {}".format(data_type.__name__))


def to_struct_field(field: Union[Field, TableField], type_mapping: dict = None) -> StructField:
    """
    Adapt a table field to a Spark StructField

    Args:
        field (Union[Field, TableField]): The field to be converted
        type_mapping (dict): Dictionary of type conversions

    Returns:
        StructField: A StructField corresponding to the given field

    Raises:
        ValueError: If there is no registered Spark datatype associated with the Python datatype of the given field
    """
    if type_mapping is None:
        type_mapping = _PY_TYPES_TO_SPARK_TYPES

    return StructField(field.name, to_spark_type(field.data_type, type_mapping))


def to_struct_type(table: Table, type_mapping: dict = None) -> StructType:
    """
    Adapt a table to a Spark StructType

    Args:
        table (Table): The table to be converted
        type_mapping(dict): Dictionary of type conversions

    Returns:
        StructType: Spark StructType corresponding to the table

    Raises:
        ValueError: If the datatype of any field of the table has no associated Spark type
    """
    if type_mapping is None:
        type_mapping = _PY_TYPES_TO_SPARK_TYPES

    return StructType([to_struct_field(field, type_mapping) for field in table.qualified_fields])


def to_string_struct_type(table: Table) -> StructType:
    """
    Adapt a table to a Spark StructType. All fields will have StringType only

    Args:
        table (Table): The table to be converted

    Returns:
        StructType: Spark StructType corresponding to the table

    Raises:
        ValueError: If the datatype of any field of the table has no associated Spark type
    """
    return StructType([StructField(field.name, StringType()) for field in table.qualified_fields])


def chain_joins(spark: SparkSession, df: DataFrame, *join_chains: JoinChain) -> DataFrame:
    """
    Apply a sequence of joins to a dataframe with tables registered to the current context

    Args:
        spark (SparkSession): The current Spark session
        df (DataFrame): The dataframe to be joined
        *join_chains (JoinChain): Datatypes describing the joins to perform

    Returns:
        DataFrame: The dataframe following completion of all the joins
    """
    for join_chain in join_chains:
        df = chain_joins(spark, join_tables(spark, df, join_chain.join_parameters), *join_chain.child_joins)
    return df


def join_tables(spark: SparkSession, df: DataFrame, join_parameters: JoinParameters) -> DataFrame:
    """
    Join a dataframe according to given parameters

    Args:
        spark (SparkSession): The current Spark session, containing registered views
        df (DataFrame): The table representing the left hand side of the join
        join_parameters (JoinParameters): Parameters describing the right hand side of the join

    Returns:
        DataFrame: The DataFrame of the two tables joined
    """
    foreign_df = spark.table(join_parameters.right_table.name)

    if join_parameters.right_table.alias:
        foreign_df = foreign_df.alias(join_parameters.right_table.alias)

    conditions = join_parameters.conditions()
    if conditions:
        foreign_df = foreign_df.filter(*conditions)

    foreign_df = foreign_df.select(*(field.qualified_name for field in join_parameters.right_table.qualified_fields))

    if join_parameters.join_keys:
        clean_foreign_df = foreign_df.drop_duplicates(
            (foreign_key.name for _, foreign_key in join_parameters.join_keys))
        joined_df = df.join(clean_foreign_df, join_keys_expression(join_parameters.join_keys), 'left_outer')
    else:
        # Spark requires explicit cross join rather than implicit by trivial join condition
        joined_df = df.crossJoin(foreign_df.limit(1))

    return joined_df


def normalise(column: Union[str, Column]) -> Column:
    """
    Convert a string expression to a Column if necessary

    Args:
        column (Union[str, Column]): Either a string or Column expression

    Returns:
        Column: Either the Column given, or the string coerced to a Column
    """
    if isinstance(column, Column):
        return column

    return col(column)


def all_conditions(*conditions: Column) -> Column:
    """
    Union a series of boolean Column expressions

    Args:
        *conditions (Column): The Column expressions to union

    Returns:
        Column: A boolean expression that is passed only if all the given conditions are passed
    """
    if not conditions:
        return lit(True)

    combined_conditions = conditions[0]
    for condition in conditions[1:]:
        combined_conditions = combined_conditions & condition
    return combined_conditions


def join_keys_expression(join_keys: AbstractSet[Tuple[TableField, TableField]]) -> Column:
    """
    Create a Column expression for joining two DataFrames together by a the specified keys

    Args:
        left_df (DataFrame): The left hand side of the join
        right_df (DataFrame): The right hand side of the join
        join_keys (AbstractSet[Tuple[str, str]]): Collection of left and right key pairs to join against

    Returns:
        Column: A boolean expression for joining the tables by the given keys
    """
    return all_conditions(*(col(left_key.qualified_name) == col(right_key.qualified_name)
                            for left_key, right_key in join_keys))


def empty_array(element_type: DataType) -> Column:
    """
    Return an Column expression representing an empty array for the given element type

    Args:
        element_type (DataType): The type of elements in the array

    Returns:
        Column: An expression representing an empty array with the given element type
    """
    return udf(lambda: [], ArrayType(element_type))()


def typed_udf(return_type: DataType) -> Callable[[Callable], Callable]:
    """
    A helper method to be used as an annotation to define functions as Spark UDFs with a specified type

    Args:
        return_type (DataType): The return type of the resulting UDF

    Returns:
        Callable[[Callable], Callable]: A wrapper to adapt functions to typed UDFs
    """

    def wrapper(wrapped_function):
        return udf(wrapped_function, return_type)

    return wrapper


def schema_fill_values(schema: StructType, **kwargs) -> Row:
    """Loops through each field in schema and populated with values in available fields provided.

        Args:
            schema : StructType
            kwargs : List of fields to populate in row

        Returns:
           Row populated with values
        """

    row_data = []
    for field in schema.fieldNames():
        row_data.append(kwargs.get(field))
    return Row(*row_data)


def sort_struct_fields_alphabetically(struct_type: StructType) -> StructType:
    """
    Recursively sort the fields of this and all nested StructTypes to appear in alphabetical order, in keeping with
    Spark's default behaviour when initialising a Row instance directly with key-value arguments

    Args:
        struct_type: The StructType to be sorted

    Returns:
        An equivalent StructType with fields in alphabetical order
    """

    def recurse_on_field(struct_field: StructField) -> StructField:
        if isinstance(struct_field.dataType, StructType):
            return StructField(struct_field.name, sort_struct_fields_alphabetically(struct_field.dataType))

        if isinstance(struct_field.dataType, ArrayType) and isinstance(struct_field.dataType.elementType, StructType):
            return StructField(struct_field.name,
                               ArrayType(sort_struct_fields_alphabetically(struct_field.dataType.elementType)))

        return struct_field

    return StructType(
        list(sorted([recurse_on_field(field) for field in struct_type.fields], key=lambda field: field.name)))


def datatypes_equal(first: DataType, second: DataType, check_nullability: bool = True):
    """
    Determine whether a pair of Spark data types are equivalent

    Args:
        first: The first data type to compare
        second: The second data type to compare
        check_nullability: Whether to consider the nullability of struct fields when determining whether they are
            equivalent

    Returns:
        Whether the two data types given are equivalent
    """
    if isinstance(first, StructType) and isinstance(second, StructType):
        if check_nullability:
            return first == second

        if len(first.fields) != len(second.fields):
            return False

        for first_field, second_field in zip(first.fields, second.fields):
            if first_field.name != second_field.name \
                    or not datatypes_equal(first_field.dataType, second_field.dataType, check_nullability):
                return False

        return True

    if type(first) != type(second):  # pylint: disable=unidiomatic-typecheck
        return False

    if isinstance(first, ArrayType):
        return datatypes_equal(first.elementType, second.elementType, check_nullability)

    return first == second


def dataframes_equal(first: DataFrame, second: DataFrame, check_nullability: bool = True) -> bool:
    """
    Determine whether two dataframes have the same schema and contain the same record. At present, this method
    disregards the order that the records occur, only considering whether equivalent records are present.

    Args:
        first: The first data frame to compare
        second: The second data frame to compare
        check_nullability: Whether to consider the nullability of struct fields  of the schema of the dataframes
            when determining whether they are equivalent

    Returns:
        Whether the dataframes are equivalent
    """
    if not datatypes_equal(first.schema, second.schema, check_nullability):
        return False

    second_in_order = second.select(*first.columns)
    diff1 = first.subtract(second_in_order)
    diff2 = second_in_order.subtract(first)
    if dataframe_empty(diff1.union(diff2)):
        return True

    diff1.cache()
    diff2.cache()
    try:
        print("Records in first not in second:")
        diff1.show(truncate=False)
        print("Records in second not in first:")
        diff2.show(truncate=False)
    finally:
        diff1.unpersist()
        diff2.unpersist()

    return False


def dataframe_is_subset(first: DataFrame, second: DataFrame, check_nullability: bool = True) -> bool:
    """
    Determine whether two dataframes have the same schema the first dataframes records are a subset of the second dataframes records.
    At present, this method disregards the order that the records occur, only considering whether equivalent records are present.

    Args:
        first: The first data frame to compare
        second: The second data frame to compare
        check_nullability: Whether to consider the nullability of struct fields  of the schema of the dataframes
            when determining whether they are equivalent

    Returns:
        Whether the first dataframes records are a subset of the second dataframes records
    """
    if not datatypes_equal(first.schema, second.schema, check_nullability):
        return False

    diff = first.subtract(second.select(*first.columns))
    diff.show()

    return dataframe_empty(diff)


@log_action(log_args=['database', 'table', 'retain_hours'])
def vacuum_table(spark: SparkSession, database: Optional[str] = None, table: str = None,
                 retain_hours: int = 168) -> DataFrame:
    """database param is optional as some tables aren't in a DB and sometimes code submits db.table format for the
    table. Specified in database, table order for readability. Set to None to avoid mandatory arg errors.
    See https://docs.databricks.com/spark/latest/spark-sql/language-manual/vacuum.html.
    retain_hours defaults to 7 days - the same default as Databricks."""

    assert table, "Table not set"

    if database:
        return spark.sql('VACUUM {}.{} RETAIN {} HOURS'.format(database, table, retain_hours))

    return spark.sql('VACUUM {} RETAIN {} HOURS'.format(table, retain_hours))


@log_action(log_args=['database', 'table'])
def optimize_table(
        spark: SparkSession, database: Optional[str] = None, table: str = None, zorder_by: List[str] = None
) -> DataFrame:
    """database param is optional as some tables aren't in a DB and sometimes code submits db.table format for the
    table. Specified in database, table order for readability. Set to None to avoid mandatory arg errors."""
    assert table, "Table not set"

    target = f'{database}.{table}' if database else table

    zorder = f" ZORDER BY ({', '.join(zorder_by)})" if zorder_by else ''

    return spark.sql(f'OPTIMIZE {target}{zorder}')


def parse_table_name(fq_table_name: str) -> Tuple[Optional[str], str]:
    """
    Parse a fully qualified table name, returning (optionally) the database
    name and (definitely) the table name.

    This will raise a `ValueError` if the identifier is invalid.

    """
    fq_table_name = fq_table_name.strip()
    # Match alphanumeric + underscore, either enclosed in backticks or not.
    segment = r"`[A-Za-z0-9_]+?`|[A-Za-z0-9_]+?"
    # Using named capture patterns for database and table.
    database_patt, table_patt = rf"(?P<database>{segment})", rf"(?P<table>{segment})"
    match = re.match(rf"^({database_patt}\.)?{table_patt}$", fq_table_name)
    if not match:
        raise ValueError(f"{fq_table_name!r} is not a valid table identifier")

    database, table = match.group("database"), match.group("table")
    # Strip backticks in individual representations.
    if database:
        database = database.strip("`")
    return database, table.strip("`")


def build_fq_table_name(database: Optional[str], table: str) -> str:
    """
    Get the fully qualified table name from database and table names,
    raising a `ValueError` if the table name is not provided or the
    components are invalid.

    """
    if not table:
        raise ValueError("`table` must be provided.")

    if database:
        # Check that there's no database in the table name.
        table_name_database, parsed_table_name = parse_table_name(table)
        if table_name_database:
            raise ValueError(
                f"`database` provided, but database already in `table`: {table!r}"
            )
        table = parsed_table_name
    else:
        database, table = parse_table_name(table)

    if database:
        fq_table_name = f"`{database}`.`{table}`"
    else:
        fq_table_name = f"`{table}`"

    return fq_table_name


def table_exists(spark: SparkSession, table: str) -> bool:
    """
        check if the spark table exists
    Args:
        spark (SparkSession):
        table (str): this should be fully qualified for proper tables

    Returns:
        bool:
    """

    return spark.catalog._jcatalog.tableExists(table)


def database_exists(spark: SparkSession, database: str) -> bool:
    """
        check if the database exists
    Args:
        spark (SparkSession):
        database (str): database name

    Returns:
        bool:
    """

    return spark.catalog._jcatalog.databaseExists(database)


def database_exists_acl_safe(
        spark: SparkSession,
        database: Optional[str],
) -> bool:
    """
    Given a Spark session and database, return whether the database
    exists.

    Args:
     - `spark`: the active Spark session
     - `database`: the name of the database

    """
    show_dbs_df = spark.sql("SHOW DATABASES")
    if 'namespace' in show_dbs_df.columns:
        show_dbs_df = show_dbs_df.withColumn('databaseName', col('namespace'))
    db_exists_filter = upper(col("databaseName")) == upper(lit(database))
    entry = show_dbs_df.filter(db_exists_filter).first()

    return entry is not None


def table_exists_acl_safe(
        spark: SparkSession,
        database: Optional[str],
        table: str,
        allow_global_view: bool = True,
        raise_for_multiple: bool = True,
) -> bool:
    """
    Given a Spark session, a database (or `None`), and a table name, return
    whether the table exists. This will work as expected on ACL clusters.

    Args:
     - `spark`: the active Spark session
     - `database`: an optional database name. If the database is in the
       table name or there is no database, this should be `None`
     - `table`: the name of the table. If this is fully qualified,
       `database` should be `None`
     - `allow_global_view`: whether to allow global views to be considered
       'tables' when `database` is `None`. Default: `True`
     - `raise_for_multiple`: whether to raise a `ValueError` if more than
       one table exists with the given name: Default `True`

    This will raise a `ValueError` if the database is provided and does
    not exist.

    """
    # Get the separated database and table name from the provided values.
    database, table = parse_table_name(build_fq_table_name(database, table))

    if database:  # Ensure that the database actually exists first.
        if not database_exists_acl_safe(spark, database):
            raise ValueError(f"Database {database!r} does not exist")

        statement = f"SHOW TABLES IN {database}"
    else:
        statement = "SHOW TABLES"

    show_tables_df = spark.sql(statement)
    if database:
        specific_db_filter = upper(col("database")).eqNullSafe(upper(lit(database)))

        show_tables_df = show_tables_df.filter(specific_db_filter)

    elif not allow_global_view:
        non_null_db_filter = col("database").isNotNull() & (col("database") != lit(""))

        show_tables_df = show_tables_df.filter(non_null_db_filter)

    table_exists_filter = upper(col("tableName")) == upper(lit(table))
    count = show_tables_df.filter(table_exists_filter).count()

    if not count:
        return False

    if count > 1 and raise_for_multiple:
        raise ValueError(f"{count:,} tables found with the provided identifier")
    return True


def create_asset(spark: SparkSession, bucket: str, database: str, force_bucket: bool = False):
    assert bucket
    assert database
    assert type(bucket) == str
    assert type(database) == str
    if not force_bucket:
        assert bucket in ('ds', 'dms', 'ops', 'raw', 'curated', 'covid19-curated', 'covid19-analysis', 'collaboration',
                          'gp-data-curated')

    if any(r for r in spark.sql('show databases').collect() if r[0] == database):
        raise ValueError(f'Database {database} already exists')

    if not local_mode():

        target_path = f"s3://nhsd-dspp-core-{os.environ['env']}-{bucket}/assets/{database}"

        try:
            # check the target location does NOT exist
            spark.read.text(target_path)
            raise ValueError(f'Path already exists {target_path}')
        except pyspark.sql.utils.AnalysisException as e:
            if not e.desc.startswith('Path does not exist'):
                raise

        spark.sql(f"CREATE DATABASE {database} LOCATION '{target_path}'")

    else:
        os.makedirs('/tmp/temp_dbs', exist_ok=True)
        spark.sql(f"CREATE DATABASE {database} LOCATION '/tmp/temp_dbs/{uuid4().hex}/{database}'")


def dict_to_schema(json_dict, schema: StructType):
    def _coerce_field(value, dt: DataType):
        if value is None:
            return None

        if isinstance(dt, (TimestampType, DateType)):
            return dateutil.parser.isoparse(value)

        if isinstance(dt, DecimalType):
            return Decimal(str(value))

        if isinstance(dt, StructType):
            return dict_to_schema(value, dt)

        if type(value) != list or not isinstance(dt, ArrayType):
            return value

        return [_coerce_field(item, dt.elementType) for item in value]

    return Row(*[_coerce_field(json_dict.get(f.name), f.dataType) for f in schema.fields])


def jsonl_to_schema(json_file, schema: StructType):
    with open(json_file, 'r') as f:
        lines = [l.strip() for l in f.readlines()]
        for line in lines:
            if not line:
                continue
            line = json.loads(line)
            yield dict_to_schema(line, schema)


def sort_schema(schema: DataType):
    if schema is None:
        return None

    if isinstance(schema, ArrayType):
        return ArrayType(sort_schema(schema.elementType))

    if not isinstance(schema, StructType):
        return schema

    fields = []
    for field_name in sorted(schema.fieldNames()):
        field = schema[field_name]

        if isinstance(field.dataType, StructType):
            fields.append(StructField(field_name, sort_schema(field.dataType)))
            continue

        if isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            fields.append(StructField(field_name, ArrayType(sort_schema(field.dataType.elementType))))
            continue

        fields.append(field)
    return StructType(fields)


def modify_schema_fields(
        schema: DataType, modify_field: Callable[[StructField, StructField], Optional[StructField]],
        parent: Optional[StructField] = None
):
    if schema is None:
        return None

    if isinstance(schema, ArrayType):
        return ArrayType(modify_schema_fields(schema.elementType, modify_field))

    if not isinstance(schema, StructType):
        return schema

    fields = []
    for field in schema.fields:

        child_type = field.dataType

        if isinstance(child_type, StructType):
            child_type = modify_schema_fields(child_type, modify_field, parent=field)
        elif isinstance(child_type, ArrayType) and isinstance(child_type.elementType, StructType):
            child_type = ArrayType(modify_schema_fields(child_type.elementType, modify_field, parent=field))

        field = modify_field(StructField(field.name, child_type), parent)

        if not field:
            continue

        fields.append(field)

    return StructType(fields)


def collate_rows_by_key(rows: Iterable[Row], key_selector: Callable[[Row], str]) -> Dict[str, List[Row]]:
    result = dict()
    if not rows:
        return result

    for row in rows:
        key = key_selector(row)

        if key not in result:
            result[key] = []

        result[key].append(row)

    return result


def _generate_select_expressions(
        source_schema: StructType, target_schema: StructType,
        used_aliases: Set[str] = None,
        namespace: str = "",
        parent_field: StructField = None,
        custom_transforms: Callable[[StructField, StructField], Optional[str]] = None
):
    used_aliases = used_aliases or set()
    source_fields = source_schema.fields
    target_fields = target_schema.fields
    assert len(source_fields) == len(target_fields), (source_schema, target_schema)

    def _new_alias():
        for num in range(100):
            for char in string.ascii_lowercase:
                new_alias = f"{char}{num}"
                if new_alias in used_aliases:
                    continue
                used_aliases.add(new_alias)
                return new_alias
        raise ValueError('thats a lot of aliases!')

    for ix, source in enumerate(source_fields):
        target = target_fields[ix]

        source_col = f"{namespace}`{source.name}`"

        if custom_transforms:
            custom_transform = custom_transforms(source, parent_field)
            if custom_transform:
                custom_transform = custom_transform.format(field=f"{source_col}")
                yield f"{custom_transform} AS `{target.name}`"
                continue

        if source.dataType == target.dataType:
            yield f"{source_col} AS `{target.name}`"
            continue

        if isinstance(source.dataType, ArrayType):
            assert isinstance(target.dataType, ArrayType), (source.dataType, target.dataType)
            alias = _new_alias()
            source_et = source.dataType.elementType
            target_et = target.dataType.elementType

            if not isinstance(target_et, StructType):
                # simple cast
                yield f"TRANSFORM({source_col}, {alias} -> CAST({alias} AS {target_et.simpleString()})) AS `{target.name}`"
                continue

            assert isinstance(source_et, StructType), (source.dataType, target.dataType)

            sub_selects = ", ".join(
                _generate_select_expressions(
                    source_et, target_et, used_aliases, namespace=f"{alias}.",
                    parent_field=source, custom_transforms=custom_transforms
                )
            )
            yield f"TRANSFORM({source_col}, {alias} -> STRUCT({sub_selects})) AS `{target.name}`"
            continue

        if isinstance(source.dataType, StructType):
            assert isinstance(target.dataType, StructType), (source.dataType, target.dataType)
            sub_selects = ", ".join(
                _generate_select_expressions(
                    source.dataType, target.dataType, used_aliases, namespace=f"{source_col}.",
                    parent_field=source, custom_transforms=custom_transforms
                )
            )
            yield f"STRUCT({sub_selects}) AS `{target.name}`"
            continue

        yield f"CAST({source_col} AS {target.dataType.simpleString()}) AS `{target.name}`"


def transform_schema(
        df: DataFrame, target_schema: StructType,
        custom_transforms: Callable[[StructField, StructField], Optional[str]] = None
):
    """
        attempt to transform a dataframe to match an target schema,
        target schema and nested structs must have a compatible number of fields .. (it's not psychic!)
        check your implicit casts / transforms actually work, you may end up with null columns,
        you attempt an invalid cast

    Example::
            >>> df_before = spark.sql(
            ...    "select nhs_number, struct(parsed_record as parsed) as parsed_parent from pds.pds"
            ... )
            ...
            ... def change_fields(field, parent_field):
            ...    if field.name in ('from', 'to'):
            ...        return StructField(f"effective_{field.name}", DateType())
            ...
            ...    if field.name in ('dob', 'dod'):
            ...        return StructField(field.name, DateType())
            ...
            ...    if field.name == 'addr':
            ...        return StructField('lines', ArrayType(StringType()))
            ...
            ...     return field
            ...
            ...
            ... def transform_dates(field, parent_field):
            ...
            ...     if field.name in ('from', 'to', 'dob', 'dod'):
            ...         return "TO_DATE(CAST({field} AS STRING), 'yyyyMMdd')"
            ...
            ...     if field.name == 'addr':
            ...         return "FILTER(TRANSFORM({field}, x -> UPPER(x.line)), y -> LENGTH(y) > 0)"
            ...
            ...     return None
            ...
            ... target_schema = modify_schema_fields(df_before.schema, change_fields)
            ... df_after = transform_schema(df_before, target_schema, custom_transforms=transform_dates)
    Args:
        df (DataFrame): source data frame
        target_schema (StructType): schema to try and match
        custom_transforms (Callable[[StructField, StructField], Optional[str]]): function to provide custom transforms

    Returns:
        DataFrame:  the transformed dataframe
    """

    select_expressions = _generate_select_expressions(df.schema, target_schema, custom_transforms=custom_transforms)

    df_result = df.select(*(expr(ex) for ex in select_expressions))
    return df_result


def drop_table_or_view(spark, db_name, table):
    if table.tableType == "VIEW":
        drop_view_script = f"DROP VIEW {db_name}.{table.name}"
        print(drop_view_script)
        spark.sql(drop_view_script)
        return True

    location = (
        spark.sql(f"describe extended {db_name}.{table.name}")
        .where(col("col_name") == lit("Location"))
        .select("data_type")
        .collect()[0][0]
    )

    drop_table_script = f"DROP TABLE {db_name}.{table.name}"
    print(drop_table_script)
    spark.sql(drop_table_script)
    if table.tableType == "MANAGED":
        return True

    _scheme, bucket, prefix = s3_split_path(location)
    s3_cleanup(bucket, prefix)


def add_fields_to_dataframe(df: DataFrame, **kwargs: Column):
    """
    Add or update fields to the provided dataframe

    Args:
    df: dataframe to add / update fields
    **kwargs (col): provide in the format of <col name> = <column expression>
    """
    assert all(isinstance(vals, Column) for vals in kwargs.values())
    _passthrough_cols = [flds for flds in df.columns if not flds in kwargs.keys()]
    return df.select(*_passthrough_cols, *[v.alias(k) for k, v in kwargs.items()])
