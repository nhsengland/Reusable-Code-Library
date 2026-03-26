from datetime import datetime
from decimal import Decimal
from typing import Iterator, List, Tuple, Union
from uuid import uuid4

import pytest
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DataType, StringType, TimestampType, DecimalType, StructType, StructField, ArrayType, \
    IntegerType
from pyspark.sql.functions import col, length as ps_length, concat_ws, lit, initcap

from dsp.common.relational import Field, TableField, Table
from dsp.common.schema import Schema
from nhs_reusable_code_library.resuable_codes.spark_helpers import table_exists_acl_safe, to_spark_type, to_struct_field, to_struct_type, schema_fill_values, \
    sort_struct_fields_alphabetically, datatypes_equal, dataframes_equal, dataframe_is_subset, \
    parse_table_name, build_fq_table_name, add_fields_to_dataframe
from dsp.validations.conftest import spark


@pytest.fixture(scope="function")
def view_name(spark: SparkSession) -> Iterator[str]:
    """Get a view name and clean it up after the test is complete."""
    view_name_str = uuid4().hex

    drop_view_stmt = f"DROP VIEW IF EXISTS {view_name_str}"
    spark.sql(drop_view_stmt)
    yield view_name_str
    spark.sql(drop_view_stmt)


@pytest.mark.parametrize(['python_type', 'expected'], [
    (str, StringType()),
    (datetime, TimestampType()),
    (Decimal, DecimalType(17)),
])
def test_to_spark_type(python_type: type, expected: DataType):
    assert to_spark_type(python_type) == expected


@pytest.mark.parametrize(['field', 'expected'], [
    (Field("Field1", str), StructField("Field1", StringType())),
    (Table("Table", Field("Field2", datetime))["Field2"], StructField("Field2", TimestampType()))
])
def test_to_struct_field(field: Union[Field, TableField], expected: StructField):
    assert to_struct_field(field) == expected


def test_to_struct_type():
    table = Table("Table", Field("Field1", str), Field("Field2", datetime))
    assert to_struct_type(table) == StructType([
        StructField("Field1", StringType()),
        StructField("Field2", TimestampType())
    ])


def test_schema_fill_values():
    schema = StructType([
        StructField('a', StringType(), False),
        StructField('b', StringType(), False),
        StructField('c', StringType(), False)])

    row = schema_fill_values(schema, a='A', c='C')
    assert row[0] == 'A'
    assert not row[1]
    assert row[2] == 'C'


def test_struct_fields_sorted_alphabetically():
    schema = StructType([
        StructField('Zeta', StringType()),
        StructField('Alpha', StringType()),
        StructField('Gamma', StringType()),
        StructField('Beta', StringType()),
    ])

    sorted_schema = sort_struct_fields_alphabetically(schema)

    assert sorted_schema == StructType([
        StructField('Alpha', StringType()),
        StructField('Beta', StringType()),
        StructField('Gamma', StringType()),
        StructField('Zeta', StringType()),
    ])


@pytest.mark.parametrize(['first', 'second', 'check_nullability', 'expected'], [
    # Simple equality
    (StringType(), StringType(), False, True),
    # Nullability does not affect simple types
    (StringType(), StringType(), True, True),
    # Arrays match if elements match
    (ArrayType(StringType()), ArrayType(StringType()), False, True),
    (ArrayType(StringType()), ArrayType(IntegerType()), False, False),
    # Structs match if fields match
    (StructType([StructField('A', StringType())]), StructType([StructField('A', StringType())]), False, True),
    (StructType([StructField('A', StringType())]), StructType([StructField('B', StringType())]), False, False),
    (StructType([StructField('A', StringType())]), StructType([StructField('A', IntegerType())]), False, False),
    (StructType([StructField('A', StringType())]),
     StructType([StructField('A', StringType()), StructField('B', IntegerType())]), False, False),
    # Strict nullability
    (StructType([StructField('A', StringType(), nullable=True)]),
     StructType([StructField('A', StringType(), nullable=True)]), True, True),
    (StructType([StructField('A', StringType(), nullable=False)]),
     StructType([StructField('A', StringType(), nullable=True)]), True, False),
    # Lax nullability
    (StructType([StructField('A', StringType(), nullable=False)]),
     StructType([StructField('A', StringType(), nullable=True)]), False, True),
    # Field order
    (StructType([StructField('A', StringType()), StructField('B', StringType())]),
     StructType([StructField('B', StringType()), StructField('A', StringType())]), False, False)
])
def test_datatypes_equal(first: DataType, second: DataType, check_nullability: bool, expected: bool):
    assert datatypes_equal(first, second, check_nullability) == expected


@pytest.mark.parametrize(
    ['first_data', 'first_schema', 'second_data', 'second_schema', 'check_nullability', 'expected'], [
        # Simple comparison
        ([('Hello',)], StructType([StructField('A', StringType())]), [('Hello',)],
         StructType([StructField('A', StringType())]), False, True),
        # Different data
        ([('Hello',)], StructType([StructField('A', StringType())]), [('World',)],
         StructType([StructField('A', StringType())]), False, False),
        # Different schema
        ([('Hello',)], StructType([StructField('A', StringType())]), [('Hello',)],
         StructType([StructField('B', StringType())]), False, False),
        # Nullability
        ([('Hello',)], StructType([StructField('A', StringType(), nullable=True)]), [('Hello',)],
         StructType([StructField('A', StringType(), nullable=False)]), False, True),
        ([('Hello',)], StructType([StructField('A', StringType(), nullable=True)]), [('Hello',)],
         StructType([StructField('A', StringType(), nullable=False)]), True, False),
        # Data in first dataframe that is not in second dataframe
        (
            [('Hello',), ('World',)],
            StructType([StructField('A', StringType())]),
            [('World',)],
            StructType([StructField('A', StringType())]),
            False,
            False,
        ),
        # Data in second dataframe that is not in first dataframe
        (
            [('Hello',)],
            StructType([StructField('A', StringType())]),
            [('Hello',), ('World',)],
            StructType([StructField('A', StringType())]),
            False,
            False,
        ),
    ])
# def test_dataframes_equal(spark: SparkSession, first_data: List[tuple], first_schema: StructType, 
#                           second_data: List[tuple], second_schema: StructType, check_nullability: bool, expected: bool):
#     input_df = spark.createDataFrame(first_data, first_schema)
#     expected_df =spark.createDataFrame(second_data, second_schema)
    
#     assert dataframes_equal(expected_df, input_df)
#     # assert dataframes_equal(, 
#     #                         , check_nullability) == expected
def test_dataframes_equal(spark: SparkSession, first_data: List[tuple], first_schema: StructType,
                          second_data: List[tuple], second_schema: StructType, check_nullability: bool, expected: bool):
    assert dataframes_equal(spark.createDataFrame(first_data, first_schema),
                            spark.createDataFrame(second_data, second_schema), check_nullability) == expected

@pytest.mark.parametrize(
    ['first_data', 'first_schema', 'second_data', 'second_schema', 'check_nullability', 'expected'], [
        # Simple comparison
        ([('Hello',)], StructType([StructField('A', StringType())]), [('Hello',)],
         StructType([StructField('A', StringType())]), False, True),
        # Different data
        ([('Hello',)], StructType([StructField('A', StringType())]), [('World',)],
         StructType([StructField('A', StringType())]), False, False),
        # Different schema
        ([('Hello',)], StructType([StructField('A', StringType())]), [('Hello',)],
         StructType([StructField('B', StringType())]), False, False),
        # Nullability
        ([('Hello',)], StructType([StructField('A', StringType(), nullable=True)]), [('Hello',)],
         StructType([StructField('A', StringType(), nullable=False)]), False, True),
        ([('Hello',)], StructType([StructField('A', StringType(), nullable=True)]), [('Hello',)],
         StructType([StructField('A', StringType(), nullable=False)]), True, False),
        # Data in first dataframe that is not in second dataframe
        (
            [('Hello',), ('World',)],
            StructType([StructField('A', StringType())]),
            [('World',)],
            StructType([StructField('A', StringType())]),
            False,
            False,
        ),
        # Data in second dataframe that is not in first dataframe
        (
            [('Hello',)],
            StructType([StructField('A', StringType())]),
            [('Hello',), ('World',)],
            StructType([StructField('A', StringType())]),
            False,
            True,
        ),
    ])
def test_dataframe_is_subset(spark: SparkSession, first_data: List[tuple], first_schema: StructType,
                          second_data: List[tuple], second_schema: StructType, check_nullability: bool, expected: bool):
    assert dataframe_is_subset(
        spark.createDataFrame(first_data, first_schema),
        spark.createDataFrame(second_data, second_schema),
        check_nullability
    ) == expected


@pytest.mark.parametrize(
    ['input', 'expected'], 
    [
        ("db.table", ("db", "table")),
        ("`db`.`table`", ("db", "table")),
        ("`db`.table", ("db", "table")),
        ("db.table", ("db", "table")),
        ("`table`", (None, "table")),
        ("  table  ", (None, "table")),
        (" db.table", ("db", "table")),
    ]
)
def test_parse_table_name(input: str, expected: Tuple[str, str]):
    """Test that table names can be parsed correctly."""
    assert parse_table_name(input) == expected


@pytest.mark.parametrize(
    ["input"],
    [
        ("table_with.multiple.levels",),
        ("`mismatched_back`ticks`",),
        ("`outside_`backticks.`table`",),
        ("db .table",),
        ("`db `.table",),
        ("$l!cK_t4bl3_n4m3",),
    ]
)
def test_parse_table_name_failures(input: str):
    """Test that junk table names are rejected."""
    with pytest.raises(ValueError):
        parse_table_name(input)


def test_fq_table_name_validation():
    """Test that FQ table names can be correctly validated."""
    assert build_fq_table_name(None, "database.table") == "`database`.`table`"
    assert build_fq_table_name("database", "table") == "`database`.`table`"

    with pytest.raises(ValueError):
        build_fq_table_name(None, None)

    with pytest.raises(ValueError):
        build_fq_table_name("database", "database.table")


def test_table_exists_view_names(spark: SparkSession, view_name: str):
    """Test that `_table_exists` works with views."""
    assert table_exists_acl_safe(spark, None, view_name) is False
    schema = Schema.from_keys(Name=StringType())
    spark.createDataFrame([], schema).createOrReplaceTempView(view_name)
    assert table_exists_acl_safe(spark, None, view_name) is True
    #assert table_exists_acl_safe(spark, None, view_name, False) is False
    

def test_add_fields_to_dataframe(spark: SparkSession):
    _test_rows = [dict(pet_name='freddie', pet_type='dog', pet_age=7),
                  dict(pet_name='angus', pet_type='cat', pet_age=3)]
    
    _test_schema = StructType([StructField('pet_name', StringType()),
                               StructField('pet_type', StringType()),
                               StructField('pet_age', IntegerType())])
    
    test_df = spark.createDataFrame(_test_rows, _test_schema)
    
    new_df = add_fields_to_dataframe(test_df,
                                     name_len=ps_length(col('pet_name')), 
                                     type_age_concat=concat_ws("-", col("pet_type"), col("pet_age").cast(StringType())),
                                     pet_name=initcap(col("pet_name")))
    
    assert all(flds in new_df.columns for flds in test_df.columns + ["name_len", "type_age_concat"])
    freddie_res = new_df.filter(col('pet_name')==lit('Freddie')).collect()[0].asDict()
    assert freddie_res.get("name_len")==7 and freddie_res.get("type_age_concat")=="dog-7"
