from pyspark.sql.functions import col, trim
from pyspark.sql.types import StructType, StructField, StringType

from dsp.pipeline.stages.cleanse import CleanseDataFrameStage


def test_whitespace_stripping(sc, spark):
    rdd = sc.parallelize([' ', ' ', ' ']).map(lambda x: (x,))

    df = spark.createDataFrame(
        rdd,
        StructType(
            [
                StructField('test', StringType())
            ]
        )
    )

    cleansed = CleanseDataFrameStage.strip_whitespace(df, ['test'])

    result = cleansed.collect()

    assert all(r[0] == '' for r in result)


def test_no_whitespace_stripping(sc, spark):
    rdd = sc.parallelize([' ', ' ', ' ']).map(lambda x: (x,))

    df = spark.createDataFrame(
        rdd,
        StructType(
            [
                StructField('test', StringType())
            ]
        )
    )

    cleansed = CleanseDataFrameStage.strip_whitespace(df, [])

    result = cleansed.collect()

    assert all(r[0] == ' ' for r in result)


def test_extra_cleansing(sc, spark):
    rdd = sc.parallelize([' ', ' ', ' ']).map(lambda x: (x,))

    df = spark.createDataFrame(
        rdd,
        StructType(
            [
                StructField('test', StringType())
            ]
        )
    )

    cleansed = CleanseDataFrameStage.apply_extra_cleansing(
        df, [('test', lambda x: trim(col(x)))]
    )

    result = cleansed.collect()

    assert all(r[0] == '' for r in result)