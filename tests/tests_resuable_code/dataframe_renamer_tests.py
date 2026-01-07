
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import explode, expr
from pyspark.sql.types import ArrayType, DecimalType, MapType, StringType, StructField, StructType

from nhs_reusable_code_library.resuable_codes.dataframe_renamer import canonical_dataframe, strip_xml_namespaces_in_dataframe

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.sql.functions import count

# from pre_test import pre_test, spark
# #tests.pre_test import spark

# Create a SparkSession
#spark = SparkSession.builder.getOrCreate()

#spark = LocalSpark().spark

import pytest

@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.getOrCreate()
    yield spark



@pytest.fixture()
def test_df(spark_fixture) -> DataFrame:

    schema = StructType(
        [
            StructField("first name", StringType()),
            StructField("middle name", StringType()),
            StructField("last name", StringType()),
            StructField(
                "addresses",
                ArrayType(
                    StructType(
                        [
                            StructField("house name/number", StringType()),
                            StructField("post code", StringType()),
                        ]
                    )
                ),
            ),
            StructField(
                "vital statistics",
                StructType(
                    [
                        StructField("chest size", DecimalType(), False),
                        StructField("height", DecimalType(), False),
                    ]
                ),
                False,
            ),
        ]
    )

    data = [
        Row(
            "Kate",
            "R",
            "Fleming",
            [Row("22", "E3 6QQ")],
            Row(Decimal(108.2), Decimal(1.83)),
        ),
        Row(
            "Steve",
            "",
            "Arnott",
            [Row("283", "NE6 3BJ"), Row("The Willows", "TF6 3LQ")],
            Row(Decimal(136), Decimal(2.07)),
        ),
    ]

    df = spark_fixture.createDataFrame(data, schema)
    return canonical_dataframe(df)


def test_renames_non_nested_columns(test_df):
    rows = test_df.select("FIRST_NAME", "MIDDLE_NAME", "LAST_NAME").count()
    assert rows == 2


def test_renames_nested_arrays(test_df):
    rows = test_df.select(explode(expr("ADDRESSES.HOUSE_NAME_NUMBER"))).count()
    assert rows == 3
    rows = test_df.select(explode(expr("ADDRESSES.POST_CODE"))).count()
    assert rows == 3


def test_renames_nested_structs(test_df):
    rows = test_df.selectExpr("VITAL_STATISTICS.CHEST_SIZE", "VITAL_STATISTICS.HEIGHT").count()
    assert rows == 2


def test_fails_when_dataframe_contains_maptype(spark_fixture):
    # ARRANGE
    schema = StructType(
        [
            StructField("acceptable", StringType()),
            StructField("very-naughty-field", MapType(StringType(), StringType())),
        ]
    )
    data = [Row("naughty", {"k": "v"})]
    df = spark_fixture.createDataFrame(data, schema)

    # ACT
    with pytest.raises(Exception) as context:
        df = canonical_dataframe(df)

    # ASSERT
    assert context.value is not None
    assert "support for MapType not implemented" in str(context.value)


def test_fails_when_duplicates_at_top_level(spark_fixture):
    # ARRANGE
    schema = StructType(
        [
            StructField("naughty*field", StringType()),
            StructField("naughty-field", StringType()),
        ]
    )
    data = [Row("naughty", "naughty")]
    df = spark_fixture.createDataFrame(data, schema)

    # ACT
    with pytest.raises(Exception) as context:
        df = canonical_dataframe(df)

    # ASSERT
    assert context.value is not None
    assert "'naughty*field' is already renamed to 'NAUGHTY_FIELD'" in str(context.value)


def test_fails_when_duplicates_at_same_nested_level(spark_fixture):
    # ARRANGE
    schema = StructType(
        [
            StructField("acceptable-field", StringType()),
            StructField(
                "nested",
                StructType(
                    [
                        StructField("naughty*field", StringType()),
                        StructField("naughty-field", StringType()),
                    ]
                ),
                False,
            ),
        ]
    )
    data = [Row("acceptable", Row("naughty", "naughty"))]
    df = spark_fixture.createDataFrame(data, schema)

    # ACT
    with pytest.raises(Exception) as context:
        canonical_dataframe(df)

    # ASSERT
    assert context.value is not None
    assert "'naughty*field' is already renamed to 'NAUGHTY_FIELD'" in str(context.value)


def test_can_rename_when_duplicates_across_different_nesting_levels(spark_fixture):
    # ARRANGE
    schema = StructType(
        [
            StructField("naughty-field", StringType()),
            StructField(
                "nested",
                StructType(
                    [
                        StructField("naughty-field", StringType()),
                        StructField(
                            "nested-again",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("naughty-field", StringType()),
                                    ]
                                )
                            ),
                            False,
                        ),
                    ]
                ),
                False,
            ),
        ]
    )
    data = [Row("acceptable", Row("acceptable", [Row("acceptable")]))]
    df = spark_fixture.createDataFrame(data, schema)

    # ACT
    df = canonical_dataframe(df)

    # ASSERT
    assert df.count() == 1


@pytest.fixture()
def xml_test_df(spark_fixture):
    schema = StructType(
        [
            StructField("ns:firstName", StringType()),
            StructField("dss:middleName", StringType()),
            StructField("inxs:lastName", StringType()),
            StructField(
                "addresses",
                ArrayType(
                    StructType(
                        [
                            StructField("houseNameNumber", StringType()),
                            StructField("postCode", StringType()),
                        ]
                    )
                ),
            ),
            StructField(
                "vitalStatistics",
                StructType(
                    [
                        StructField("chestSize", DecimalType(), False),
                        StructField("ns:height", DecimalType(), False),
                    ]
                ),
                False,
            ),
        ]
    )

    data = [
        Row(
            "Kate",
            "R",
            "Fleming",
            [Row("22", "E3 6QQ")],
            Row(Decimal(108.2), Decimal(1.83)),
        ),
        Row(
            "Steve",
            "",
            "Arnott",
            [Row("283", "NE6 3BJ"), Row("The Willows", "TF6 3LQ")],
            Row(Decimal(136), Decimal(2.07)),
        ),
    ]

    df = spark_fixture.createDataFrame(data, schema)
    return strip_xml_namespaces_in_dataframe(df)


def test_renames_non_nested_columns(xml_test_df):
    rows = xml_test_df.select("firstName", "middleName", "lastName").count()
    assert rows == 2


def test_does_not_rename_non_prefixed_fields(xml_test_df):
    # a = xml_test_df.select(explode(expr("addresses.houseNameNumber")))
    # #a.createOrReplaceTempView('xml_test_db')
    # rows = a.select(count("addresses.houseNameNumber"))
    rows = xml_test_df.select(explode(expr("addresses.houseNameNumber"))).count()
    assert rows == 3
    rows = xml_test_df.select(explode(expr("addresses.postCode"))).count()
    assert rows == 3
    rows = xml_test_df.selectExpr("vitalStatistics.chestSize").count()
    assert rows == 2


def test_renames_nested_structs(xml_test_df):
    rows = xml_test_df.selectExpr("vitalStatistics.height").count()
    assert rows == 2


def test_fails_when_column_name_contains_mutiple_colons(spark_fixture):
    # ARRANGE
    schema = StructType([StructField("is:a:veryNaughty:Field", StringType())])
    data = [Row("naughty")]
    df = spark_fixture.createDataFrame(data, schema)

    # ACT
    with pytest.raises(Exception) as context:
        df = strip_xml_namespaces_in_dataframe(df)

    # ASSERT
    assert context.value
    assert "Unsupported: cannot rename xml tag 'is:a:veryNaughty:Field'" in str(context.value)


def test_fails_when_column_name_begins_with_colon(spark_fixture):
    # ARRANGE
    schema = StructType([StructField(":naughtyField", StringType())])
    data = [Row("naughty")]
    df = spark_fixture.createDataFrame(data, schema)

    # ACT
    with pytest.raises(Exception) as context:
        df = strip_xml_namespaces_in_dataframe(df)

    # ASSERT
    assert context.value
    assert "Unsupported: cannot rename xml tag ':naughtyField'" in str(context.value)


def test_fails_when_duplicates_at_top_level(spark_fixture):
    # ARRANGE
    schema = StructType(
        [
            StructField("naughtyField", StringType()),
            StructField("hms:naughtyField", StringType()),
        ]
    )
    data = [Row("naughty", "naughty")]
    df = spark_fixture.createDataFrame(data, schema)

    # ACT
    with pytest.raises(Exception) as context:
        df = strip_xml_namespaces_in_dataframe(df)

    # ASSERT
    assert context.value
    assert "'naughtyField' already exists" in str(context.value)


def test_fails_when_duplicates_at_same_nested_level(spark_fixture):
    # ARRANGE
    schema = StructType(
        [
            StructField("acceptableField", StringType()),
            StructField(
                "nested",
                StructType(
                    [
                        StructField("ns:naughtyField", StringType()),
                        StructField("naughtyField", StringType()),
                    ]
                ),
                False,
            ),
        ]
    )
    data = [Row("acceptable", Row("naughty", "naughty"))]
    df = spark_fixture.createDataFrame(data, schema)

    # ACT
    with pytest.raises(Exception) as context:
        strip_xml_namespaces_in_dataframe(df)

    # ASSERT
    assert context.value
    assert "'ns:naughtyField' is already renamed to 'naughtyField'" in str(context.value)


def test_can_rename_when_duplicates_across_different_nesting_levels(spark_fixture):
    # ARRANGE
    schema = StructType(
        [
            StructField("ns:naughtyField", StringType()),
            StructField(
                "nested",
                StructType(
                    [
                        StructField("ns:naughtyField", StringType()),
                        StructField(
                            "nestedAgain",
                            ArrayType(StructType([StructField("ns:naughtyField", StringType())])),
                            False,
                        ),
                    ]
                ),
                False,
            ),
        ]
    )
    data = [Row("acceptable", Row("acceptable", [Row("acceptable")]))]
    df = spark_fixture.createDataFrame(data, schema)

    # ACT
    df = strip_xml_namespaces_in_dataframe(df)

    # ASSERT
    assert df.count() == 1
