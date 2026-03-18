from datetime import datetime
from typing import Any, Iterable, List

import pytest
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (DateType, FloatType, IntegerType, Row,
                               ShortType, StringType, StructField, StructType,
                               TimestampType)

from dsp.datasets.ingestions.sgss.ingestion import (add_traceability_columns)


@pytest.fixture
def test_data_df(spark: SparkSession) -> DataFrame:
    row = ([["48146848-156", ], "Jean Claude", "Basques", "F", "2001-01-12", "LS1 4HR", "2020-01-16", "9018979845"],)

    schema = StructType([
        StructField("META", StructType([
            StructField("EVENT_ID", StringType(), True),
        ]), True),
        StructField("Patient_Forename", StringType(), True),
        StructField("FAMILY_NAME", StringType(), True),
        StructField("Patient_Sex", StringType(), True),
        StructField("Patient_Date_Of_Birth", StringType(), True),
        StructField("Patient_PostCode", StringType(), True),
        StructField("Lab_Report_Date", StringType(), True),
        StructField("NHS_NO", StringType(), True),
    ])
    return spark.createDataFrame(row, schema=schema)


def test_add_traceability_columns(spark: SparkSession):
    row: Iterable[List[Any]] = ([], )
    schema: List[Any] = []
    df = spark.createDataFrame(row, schema=schema)
    metadata = {"submission_id": "65461686813138", "timestamp": datetime(2020, 9, 30)}
    df_all = add_traceability_columns(spark, df, metadata)
    assert df_all.schema == StructType([
        StructField("META", StructType([
            StructField("DATASET_VERSION", StringType(), False),
            StructField("EVENT_ID", StringType(), False),
            StructField("EVENT_RECEIVED_TS", TimestampType(), False),
            StructField("RECORD_INDEX", IntegerType(), False),
            StructField("RECORD_VERSION", IntegerType(), False)
        ]), False)
    ])
    assert df_all.collect()[0].asDict() == {
        'META': Row(
            DATASET_VERSION='0',
            EVENT_ID='65461686813138:0',
            EVENT_RECEIVED_TS=datetime(2020, 9, 30),
            RECORD_INDEX=0,
            RECORD_VERSION=0)}
