from typing import List, Tuple
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.nhs_reusable_code_library.resuable_codes.xml_parser import XmlParser

@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark

file_path = "dummy/file/path"

multi_row_tag_a = "multi_row_a"
multi_row_tag_b = "multi_row_b"
multi_row_tag_a_ns = "mr:multi_row_a"
multi_row_tag_b_ns = "mr:multi_row_b"

multi_row_schema = StructType(
    [
        StructField("id", LongType()),
        StructField("name", StringType()),
        StructField("label", StringType()),
        StructField("count", LongType()),
    ]
)
multi_row_schema_ns = StructType(
    [
        StructField("mr:id", LongType()),
        StructField("mr:name", StringType()),
        StructField("mr:label", StringType()),
        StructField("mr:count", LongType()),
    ]
)
single_row_schema = StructType([StructField("msg", StringType()), StructField("time", LongType())])
single_row_schema_ns = StructType([StructField("sr:msg", StringType()), StructField("sr:time", LongType())])

single_row_extract_a = XmlParser.SingleRowExtract("single_row_a", "single_row_column_a", single_row_schema)
single_row_extract_b = XmlParser.SingleRowExtract("single_row_b", "single_row_column_b", single_row_schema)
single_row_extract_a_ns = XmlParser.SingleRowExtract("sr:single_row_a", "single_row_column_a", single_row_schema)
single_row_extract_b_ns = XmlParser.SingleRowExtract("sr:single_row_b", "single_row_column_b", single_row_schema)

multi_row_data_a = [
    (1, "foo", None, None),
    (2, "bar", None, None),
    (3, "baz", None, None),
]
multi_row_data_b = [
    (None, None, "tic", 23),
    (None, None, "tac", 45),
    (None, None, "toe", 67),
]
single_row_data_a = [("ping", 14)]
single_row_data_b = [("pong", 25)]


@pytest.fixture()
def get_xml_parser(spark):
    parsed_rows = {
        "multi_row_a": spark.createDataFrame(multi_row_data_a, multi_row_schema),
        "multi_row_b": spark.createDataFrame(multi_row_data_b, multi_row_schema),
        "single_row_a": spark.createDataFrame(single_row_data_a, single_row_schema),
        "single_row_b": spark.createDataFrame(single_row_data_b, single_row_schema),
        "mr:multi_row_a": spark.createDataFrame(multi_row_data_a, multi_row_schema_ns),
        "mr:multi_row_b": spark.createDataFrame(multi_row_data_b, multi_row_schema_ns),
        "sr:single_row_a": spark.createDataFrame(single_row_data_a, single_row_schema_ns),
        "sr:single_row_b": spark.createDataFrame(single_row_data_b, single_row_schema_ns),
    }
    XmlParser._parse_rows = Mock(wraps=lambda _spark, _file_path, row_tag, _schema: parsed_rows[row_tag])
    return XmlParser


def test_one_set_of_rows(spark, get_xml_parser):
    XmlParser = get_xml_parser

    # ARRANGE
    multi_row_extracts = XmlParser.MultiRowExtracts([multi_row_tag_a], multi_row_schema)
    single_row_extracts = []

    # ACT
    result = XmlParser.parse(spark, file_path, multi_row_extracts, single_row_extracts)

    # ASSERT
    expected_data = [
        (1, "foo", None, None),
        (2, "bar", None, None),
        (3, "baz", None, None),
    ]
    expected_result = get_expected_result(spark, expected_data, single_row_extracts)
    assert_df(result, expected_result)


def test_multiple_sets_of_rows(spark, get_xml_parser):
    XmlParser = get_xml_parser

    # ARRANGE
    multi_row_extracts = XmlParser.MultiRowExtracts([multi_row_tag_a, multi_row_tag_b], multi_row_schema)
    single_row_extracts = []

    # ACT
    result = XmlParser.parse(spark, file_path, multi_row_extracts, single_row_extracts)

    # ASSERT
    expected_data = [
        (1, "foo", None, None),
        (2, "bar", None, None),
        (3, "baz", None, None),
        (None, None, "tic", 23),
        (None, None, "tac", 45),
        (None, None, "toe", 67),
    ]
    expected_result = get_expected_result(spark, expected_data, single_row_extracts)
    assert_df(result, expected_result)


def test_one_set_of_rows_and_one_single_row(spark, get_xml_parser):
    XmlParser = get_xml_parser

    # ARRANGE
    multi_row_extracts = XmlParser.MultiRowExtracts([multi_row_tag_a], multi_row_schema)
    single_row_extracts = [single_row_extract_a]

    # ACT
    result = XmlParser.parse(spark, file_path, multi_row_extracts, single_row_extracts)

    # ASSERT
    expected_single_row_data = ({"msg": "ping", "time": 14},)
    expected_data = [
        (1, "foo", None, None) + expected_single_row_data,
        (2, "bar", None, None) + expected_single_row_data,
        (3, "baz", None, None) + expected_single_row_data,
    ]
    expected_result = get_expected_result(spark, expected_data, single_row_extracts)
    assert_df(result, expected_result)


def test_one_set_of_rows_and_multiple_single_rows(spark, get_xml_parser):
    XmlParser = get_xml_parser

    # ARRANGE
    multi_row_extracts = XmlParser.MultiRowExtracts([multi_row_tag_a], multi_row_schema)
    single_row_extracts = [single_row_extract_a, single_row_extract_b]

    # ACT
    result = XmlParser.parse(spark, file_path, multi_row_extracts, single_row_extracts)

    # ASSERT
    expected_single_row_data = (
        {"msg": "ping", "time": 14},
        {"msg": "pong", "time": 25},
    )
    expected_data = [
        (1, "foo", None, None) + expected_single_row_data,
        (2, "bar", None, None) + expected_single_row_data,
        (3, "baz", None, None) + expected_single_row_data,
    ]
    expected_result = get_expected_result(spark, expected_data, single_row_extracts)
    assert_df(result, expected_result)


def test_multiple_sets_of_rows_and_one_single_row(spark, get_xml_parser):
    XmlParser = get_xml_parser

    # ARRANGE
    multi_row_extracts = XmlParser.MultiRowExtracts([multi_row_tag_a, multi_row_tag_b], multi_row_schema)
    single_row_extracts = [single_row_extract_a]

    # ACT
    result = XmlParser.parse(spark, file_path, multi_row_extracts, single_row_extracts)

    # ASSERT
    expected_single_row_data = ({"msg": "ping", "time": 14},)
    expected_data = [
        (1, "foo", None, None) + expected_single_row_data,
        (2, "bar", None, None) + expected_single_row_data,
        (3, "baz", None, None) + expected_single_row_data,
        (None, None, "tic", 23) + expected_single_row_data,
        (None, None, "tac", 45) + expected_single_row_data,
        (None, None, "toe", 67) + expected_single_row_data,
    ]
    expected_result = get_expected_result(spark, expected_data, single_row_extracts)
    assert_df(result, expected_result)


def test_multiple_sets_of_rows_and_multiple_single_rows(spark, get_xml_parser):
    XmlParser = get_xml_parser

    # ARRANGE
    multi_row_extracts = XmlParser.MultiRowExtracts([multi_row_tag_a, multi_row_tag_b], multi_row_schema)
    single_row_extracts = [single_row_extract_a, single_row_extract_b]

    # ACT
    result = XmlParser.parse(spark, file_path, multi_row_extracts, single_row_extracts)

    # ASSERT
    expected_single_row_data = (
        {"msg": "ping", "time": 14},
        {"msg": "pong", "time": 25},
    )
    expected_data = [
        (1, "foo", None, None) + expected_single_row_data,
        (2, "bar", None, None) + expected_single_row_data,
        (3, "baz", None, None) + expected_single_row_data,
        (None, None, "tic", 23) + expected_single_row_data,
        (None, None, "tac", 45) + expected_single_row_data,
        (None, None, "toe", 67) + expected_single_row_data,
    ]
    expected_result = get_expected_result(spark, expected_data, single_row_extracts)
    assert_df(result, expected_result)


def test_removes_tag_namespaces(spark, get_xml_parser):
    XmlParser = get_xml_parser

    # ARRANGE
    multi_row_extracts = XmlParser.MultiRowExtracts([multi_row_tag_a_ns, multi_row_tag_b_ns], multi_row_schema)
    single_row_extracts = [
        single_row_extract_a_ns,
        single_row_extract_b_ns,
    ]

    # ACT
    result = XmlParser.parse(spark, file_path, multi_row_extracts, single_row_extracts)

    # ASSERT
    expected_single_row_data = (
        {"msg": "ping", "time": 14},
        {"msg": "pong", "time": 25},
    )
    expected_data = [
        (1, "foo", None, None) + expected_single_row_data,
        (2, "bar", None, None) + expected_single_row_data,
        (3, "baz", None, None) + expected_single_row_data,
        (None, None, "tic", 23) + expected_single_row_data,
        (None, None, "tac", 45) + expected_single_row_data,
        (None, None, "toe", 67) + expected_single_row_data,
    ]
    expected_result = get_expected_result(spark, expected_data, single_row_extracts)
    assert_df(result, expected_result)


def test_too_few_sets_of_rows(spark, get_xml_parser):
    XmlParser = get_xml_parser

    # ARRANGE
    multi_row_extracts = XmlParser.MultiRowExtracts([], multi_row_schema)
    single_row_extracts = [single_row_extract_a]

    # ACT
    with pytest.raises(Exception) as context:
        XmlParser.parse(spark, file_path, multi_row_extracts, single_row_extracts)

    # ASSERT
    assert "Insufficient number of multi-row tags provided (0)." in str(context.value)


def test_too_many_rows_for_single_row(spark, get_xml_parser):
    XmlParser = get_xml_parser

    # ARRANGE
    multi_row_extracts = XmlParser.MultiRowExtracts([multi_row_tag_a], multi_row_schema)
    single_row_extracts = [XmlParser.SingleRowExtract("multi_row_b", "single_row_col_b", single_row_schema)]

    # ACT
    with pytest.raises(Exception) as context:
        XmlParser.parse(spark, file_path, multi_row_extracts, single_row_extracts)

    # ASSERT
    assert "Unexpected number of rows extracted (3) for single-row extract 'multi_row_b'." in str(context.value)


def test_column_name_clash_between_single_rows(spark, get_xml_parser):
    XmlParser = get_xml_parser

    # ARRANGE
    multi_row_extracts = XmlParser.MultiRowExtracts([multi_row_tag_a, multi_row_tag_b], multi_row_schema)
    single_row_extracts = [
        XmlParser.SingleRowExtract("single_row_a", "single_col", single_row_schema),
        XmlParser.SingleRowExtract("single_row_b", "single_col", single_row_schema),
    ]

    # ACT
    with pytest.raises(Exception) as context:
        XmlParser.parse(spark, file_path, multi_row_extracts, single_row_extracts)

    # ASSERT
    assert ("Found clashes between single-row columns: ['single_col', 'single_col']" in str(context.value))


def test_column_name_clash_between_multi_and_single_rows(spark, get_xml_parser):
    XmlParser = get_xml_parser

    # ARRANGE
    multi_row_extracts = XmlParser.MultiRowExtracts([multi_row_tag_a, multi_row_tag_b], multi_row_schema)
    single_row_extracts = [XmlParser.SingleRowExtract("single_row_a", "id", single_row_schema)]

    # ACT
    with pytest.raises(Exception) as context:
        XmlParser.parse(spark, file_path, multi_row_extracts, single_row_extracts)

    # ASSERT
    assert "Found clashes between multi-row and single-row columns: {'id'}" in str(context.value)


def assert_df(actual_df: DataFrame, expected_df: DataFrame) -> None:
    assert actual_df.schema == expected_df.schema

    actual_rows = [row.asDict(recursive=True) for row in actual_df.collect()]
    expected_rows = [row.asDict(recursive=True) for row in expected_df.collect()]

    assert actual_rows == expected_rows


def get_expected_result(
    spark,
    expected_data: List[Tuple],
    single_row_extracts: List[XmlParser.SingleRowExtract],
) -> DataFrame:
    single_row_fields = [StructField(extract.dest_column, extract.schema) for extract in single_row_extracts]

    return spark.createDataFrame(expected_data, StructType(multi_row_schema.fields + single_row_fields))
