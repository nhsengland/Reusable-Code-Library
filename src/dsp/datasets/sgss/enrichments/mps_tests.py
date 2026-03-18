from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType

from dsp.datasets.sgss.ingestion.sgss.config import Fields
from dsp.datasets.common import Fields as CommonFields
from dsp.validation.validator import compare_results
from dsp.datasets.fields.mps.response import Fields as MpsResponseFields
from .mps import mps_request

META_SCHEMA_FOR_PDS_MPS = StructType([
        StructField(CommonFields.EVENT_ID, StringType())
])

SGSS_SCHEMA_FOR_PDS_MPS = StructType([
    StructField(CommonFields.META, META_SCHEMA_FOR_PDS_MPS),
    StructField(Fields.Patient_NHS_Number, StringType()),
    StructField(Fields.Patient_Surname, StringType()),
    StructField(Fields.Patient_Forename, StringType()),
    StructField(Fields.Patient_Date_Of_Birth, StringType()),
    StructField(Fields.Lab_Report_Date, StringType()),
    StructField(Fields.Patient_PostCode, StringType()),
    StructField(Fields.Patient_Sex, StringType()),
    StructField(Fields.P2_email, StringType()),
    StructField(Fields.P2_mobile, StringType())
])

MPS_RESPONSE_SCHEMA = StructType([
    StructField(MpsResponseFields.UNIQUE_REFERENCE, StringType()),
    StructField(MpsResponseFields.MATCHED_NHS_NO, StringType()),
    StructField(MpsResponseFields.MPS_ID, StringType()),
    StructField(MpsResponseFields.MATCHED_ALGORITHM_INDICATOR, IntegerType(), True),
    StructField(MpsResponseFields.MATCHED_CONFIDENCE_PERCENTAGE, DecimalType(5, 2), True),
    StructField(MpsResponseFields.ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC, DecimalType(5, 2), True),
    StructField(MpsResponseFields.ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC, DecimalType(5, 2), True),
    StructField(MpsResponseFields.ALGORITHMIC_TRACE_DOB_SCORE_PERC, DecimalType(5, 2), True),
    StructField(MpsResponseFields.ALGORITHMIC_TRACE_GENDER_SCORE_PERC, DecimalType(5, 2), True),
    StructField(MpsResponseFields.ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC, DecimalType(5, 2), True),
])


def test_mps_request(spark: SparkSession):
    input_data = [
        (('1:1',), '9018978914', 'REVEN', 'RAD', '28/08/1898', '01/03/2021', 'BD20 5RB', 'Male', 'email@example.com',
         '01234567890')
    ]
    input_df = spark.createDataFrame(spark.sparkContext.parallelize(input_data), SGSS_SCHEMA_FOR_PDS_MPS)

    expected_data = [
        ('1-1', '9018978914', 'REVEN', 'RAD', '', '1', None, 'BD20 5RB', None, '', '', '', '', '', None, '', '', None,
         '', '', '', '01234567890', 'email@example.com')
    ]

    mps_request_df = mps_request(input_df)
    expected_df = spark.createDataFrame(spark.sparkContext.parallelize(expected_data), mps_request_df.schema)

    assert compare_results(mps_request_df, expected_df, join_columns=['UNIQUE_REFERENCE'])


def test_mps_request_normalises_postcode(spark: SparkSession):
    input_data = [
        (('1:1',), '9018978914', 'REVEN', 'RAD', '28/08/1898', '01/03/2021', 'BD205RB', 'Male', 'email@example.com',
         '01234567890')
    ]
    input_df = spark.createDataFrame(spark.sparkContext.parallelize(input_data), SGSS_SCHEMA_FOR_PDS_MPS)

    expected_data = [
        ('1-1', '9018978914', 'REVEN', 'RAD', '', '1', None, 'BD20 5RB', None, '', '', '', '', '', None, '', '', None,
         '', '', '', '01234567890', 'email@example.com')
    ]

    mps_request_df = mps_request(input_df)
    expected_df = spark.createDataFrame(spark.sparkContext.parallelize(expected_data), mps_request_df.schema)

    assert compare_results(mps_request_df, expected_df, join_columns=['UNIQUE_REFERENCE'])


def test_mps_request_unknown_gender(spark: SparkSession):
    input_data = [
        (('1:1',), '9018978914', 'REVEN', 'RAD', '28/08/1898', '01/03/2021', 'BD20 5RB', 'Unknown', 'email@example.com',
         '01234567890')
    ]
    input_df = spark.createDataFrame(spark.sparkContext.parallelize(input_data), SGSS_SCHEMA_FOR_PDS_MPS)

    expected_data = [
        ('1-1', '9018978914', 'REVEN', 'RAD', '', '0', None, 'BD20 5RB', None, '', '', '', '', '', None, '', '', None,
         '', '', '', '01234567890', 'email@example.com')
    ]

    mps_request_df = mps_request(input_df)
    expected_df = spark.createDataFrame(spark.sparkContext.parallelize(expected_data), mps_request_df.schema)

    assert compare_results(mps_request_df, expected_df, join_columns=['UNIQUE_REFERENCE'])
