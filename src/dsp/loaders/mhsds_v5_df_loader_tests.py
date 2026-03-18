import os
from datetime import datetime, date
from decimal import Decimal

from pyspark import SparkContext, Row

from dsp.common.spark_helpers import to_struct_type
from dsp.datasets.common import Fields as Common
from dsp.datasets.definitions.mhsds_v5.submission_constants import MHS001, MHS701, MHS702
from dsp.datasets.definitions.mhsds_v5.type_mappings import PY_TYPES_TO_SPARK_TYPES_LOADING
from dsp.datasets.loaders import mhsds_v5_df_loader
from dsp.datasets.loaders.mhsds_v5_df_loader import _get_validation_codes


def test_loads_submission_successfully(spark: SparkContext):
    test_file = os.path.join(os.path.dirname(__file__),
                             "../../../../testdata/mhsds_v5/accdb/simple_access_ingestion/input.accdb")
    mhsds_v5_df_loader.load_access_db_to_spark_context(spark, test_file)

    mhs001_df = spark.table(MHS001.name)
    MPI = Row('RowNumber', *[field.name for field in MHS001.qualified_fields])
    assert mhs001_df.collect() == [
        MPI(0, "LPI00000000000000001", "RA9", "EE140277", Decimal(5558005858), "01",
            date(1977, 1, 1), "BB12 6LP", "1", "N", "1", "8", '99', None, 'aa', date(2018, 5, 15)),
    ]


def test_get_validation_codes(spark: SparkContext):
    df = spark.createDataFrame([
        (1, []),
        (2, ["code1", "code2"]),
        (3, ["code2", "code3"])
    ], ["ID", Common.DQ])

    assert _get_validation_codes(df) == {"code1", "code2", "code3"}


def test_collates_children(spark: SparkContext):
    spark.createDataFrame([
        ("CPA1", "LPI1", datetime(2018, 12, 4, 0, 0, 0), None),
        ("CPA2", "LPI2", datetime(2018, 12, 1, 0, 0, 0), None)
    ], to_struct_type(MHS701, PY_TYPES_TO_SPARK_TYPES_LOADING)).createOrReplaceTempView(MHS701.name)

    spark.createDataFrame([
        ("CPA1", datetime(2018, 12, 4, 0, 0, 0), "CPI1"),
        ("CPA1", datetime(2018, 12, 5, 0, 0, 0), "CPI2"),
    ], to_struct_type(MHS702, PY_TYPES_TO_SPARK_TYPES_LOADING)).createOrReplaceTempView(MHS702.name)

    mhs701_denorm_df = mhsds_v5_df_loader.collate_children(spark, MHS701)

    CPAEpisode = Row(*[field.name for field in MHS701.qualified_fields], "CPAReviews")
    CPAReview = Row(*[field.name for field in MHS702.qualified_fields])

    assert sorted(mhs701_denorm_df.collect()) == sorted([
        CPAEpisode("CPA1", "LPI1", datetime(2018, 12, 4, 0, 0, 0), None, [
            CPAReview("CPA1", datetime(2018, 12, 4, 0, 0, 0), "CPI1"),
            CPAReview("CPA1", datetime(2018, 12, 5, 0, 0, 0), "CPI2")
        ]),
        CPAEpisode("CPA2", "LPI2", datetime(2018, 12, 1, 0, 0, 0), None, [])
    ])
