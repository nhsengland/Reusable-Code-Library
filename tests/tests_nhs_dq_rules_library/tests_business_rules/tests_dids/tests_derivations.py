from nhs_dq_rules_library.dids.derivations import (
    days_between_dates,
    first_non_null_col,
    time_between_timestamps_hms,
    timestamp_to_date,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    IntegerType,
    StringType,
    DateType,
)
from pyspark.sql import functions as F
from datetime import datetime, timezone, timedelta, date
from pyspark.sql import SparkSession
import pytest

@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark

# test_days_between_dates
@pytest.mark.parametrize(
    "SEQ, start_dt,end_dt,expected",
    [
        (
            1,
            date(2024, 12, 31),
            date(2025, 1, 1),
            1,
        ),
        (
            2,
            date(2024, 2, 28),
            date(2024, 3, 1),
            2,
        ),
        (
            3,
            date(2024, 2, 29),
            date(2025, 3, 1),
            366,
        ),
        (
            4,
            date(2024, 3, 31),
            None,
            None,
        ),
        (
            5,
            None,
            None,
            None,
        ),
        (
            6,
            date(2025, 3, 1),
            date(2025, 3, 1),
            0,
        ),
        (
            7,
            date(2025, 1, 1),
            date(2024, 12, 31),
            -1,
        ),
    ],
)
def test_days_between_dates(spark, SEQ, start_dt, end_dt, expected):
    # Arrange
    df = spark.createDataFrame(
        [(SEQ, start_dt, end_dt)],
        schema=StructType(
            [
                StructField("SEQ", IntegerType(), True),
                StructField("start_dt", DateType(), True),
                StructField("end_dt", DateType(), True),
            ]
        ),
    )

    df = df.withColumn(
        "duration", days_between_dates(F.col("end_dt"), F.col("start_dt"))
    )

    # Act
    actual = df.collect()[0]["duration"]

    assert (
        actual == expected
    ), f"Test SEQ {SEQ} failed: expected {expected}, got {actual}"


# test_first_non_null_col
@pytest.mark.parametrize(
    "SEQ, column1, column2, expected, col_type",
    [
        # StringType cases
        (1, "Column1", "Column2", "Column1", "string"),
        (2, None, "Column2", "Column2", "string"),
        (3, "Column1", None, "Column1", "string"),
        (4, None, None, None, "string"),
        # IntegerType cases
        (5, 10, 20, 10, "int"),
        (6, None, 20, 20, "int"),
        (7, 10, None, 10, "int"),
        (8, None, None, None, "int"),
        (9, -20, 10, -20, "int"),
    ],
)
def test_first_non_null_col(spark, SEQ, column1, column2, expected, col_type):
    # Arrange
    if col_type == "string":
        schema = StructType(
            [
                StructField("SEQ", IntegerType(), True),
                StructField("column1", StringType(), True),
                StructField("column2", StringType(), True),
            ]
        )
    else:
        schema = StructType(
            [
                StructField("SEQ", IntegerType(), True),
                StructField("column1", IntegerType(), True),
                StructField("column2", IntegerType(), True),
            ]
        )

    df = spark.createDataFrame(
        [(SEQ, column1, column2)],
        schema=schema,
    )

    df = df.withColumn(
        "duration", first_non_null_col(F.col("column1"), F.col("column2"))
    )

    # Act
    actual = df.collect()[0]["duration"]

    assert (
        actual == expected
    ), f"Test SEQ {SEQ} failed: expected {expected}, got {actual}"


# test_time_between_timestamps_hms
@pytest.mark.parametrize(
    "SEQ, start_ts,end_ts,expected",
    [
        (
            1,
            datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "00:00:01",
        ),
        (
            2,
            datetime(2024, 2, 28, 00, 00, 00, tzinfo=timezone.utc),
            datetime(2024, 3, 1, 0, 0, 0, tzinfo=timezone.utc),
            "48:00:00",
        ),
        (
            3,
            datetime(2024, 2, 28, 00, 00, 00, tzinfo=timezone.utc),
            datetime(2024, 3, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=+2))),
            "46:00:00",
        ),
        (
            4,
            datetime(2024, 3, 31, 1, 59, 59, tzinfo=timezone.utc),
            None,
            None,
        ),
        (
            5,
            None,
            None,
            None,
        ),
        (
            6,
            datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            "-00:00:01",
        ),
        (
            7,
            datetime(2024, 2, 28, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 2, 28, 1, 0, 0, tzinfo=timezone(timedelta(hours=+1))),
            "00:00:00",
        ),
        (
            8,
            datetime(2024, 2, 28, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 2, 28, 0, 10, 0, tzinfo=timezone(timedelta(hours=+1))),
            "-00:50:00",
        ),
    ],
)
def test_time_between_timestamps_hms(spark, SEQ, start_ts, end_ts, expected):
    # Arrange
    df = spark.createDataFrame(
        [(SEQ, start_ts, end_ts)],
        schema=StructType(
            [
                StructField("SEQ", IntegerType(), True),
                StructField("start_ts", TimestampType(), True),
                StructField("end_ts", TimestampType(), True),
            ]
        ),
    )

    df = df.withColumn(
        "duration", time_between_timestamps_hms(F.col("end_ts"), F.col("start_ts"))
    )

    # Act
    actual = df.collect()[0]["duration"]

    assert (
        actual == expected
    ), f"Test SEQ {SEQ} failed: expected {expected}, got {actual}"


# test_timestamp_to_date
@pytest.mark.parametrize(
    "SEQ, timestamp_col, expected",
    [
        (
            1,
            datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            date(2024, 12, 31),
        ),
        (2, datetime(2024, 2, 28, 00, 00, 00, tzinfo=timezone.utc), date(2024, 2, 28)),
        (
            3,
            None,
            None,
        ),
    ],
)
def test_timestamp_to_date(spark, SEQ, timestamp_col, expected):
    # Arrange
    df = spark.createDataFrame(
        [(SEQ, timestamp_col)],
        schema=StructType(
            [
                StructField("SEQ", IntegerType(), True),
                StructField("timestamp_col", TimestampType(), True),
            ]
        ),
    )

    df = df.withColumn("date_col", timestamp_to_date(F.col("timestamp_col")))

    # Act
    actual = df.collect()[0]["date_col"]

    assert (
        actual == expected
    ), f"Test SEQ {SEQ} failed: expected {expected}, got {actual}"
