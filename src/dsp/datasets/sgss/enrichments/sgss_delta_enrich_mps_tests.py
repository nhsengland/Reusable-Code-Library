from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, BooleanType

from dsp.datasets.sgss.ingestion.sgss.config import Fields
from dsp.datasets.common import Fields as CommonFields
from dsp.validation.validator import compare_results
from dsp.datasets.fields.mps.response import Fields as MpsResponseFields
from .sgss_delta_enrich_mps import enrich_sgss_with_pds_mps

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
    StructField(MpsResponseFields.GIVEN_NAME, StringType()),
    StructField(MpsResponseFields.FAMILY_NAME, StringType()),
    StructField(MpsResponseFields.GENDER, StringType()),
    StructField(MpsResponseFields.DATE_OF_BIRTH, IntegerType()),
    StructField(MpsResponseFields.POSTCODE, StringType()),
    StructField(MpsResponseFields.EMAIL_ADDRESS, StringType()),
    StructField(MpsResponseFields.MOBILE_NUMBER, StringType()),
    StructField(MpsResponseFields.MATCHED_ALGORITHM_INDICATOR, IntegerType(), True),
    StructField(MpsResponseFields.MATCHED_CONFIDENCE_PERCENTAGE, DecimalType(5, 2), True),
    StructField(MpsResponseFields.ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC, DecimalType(5, 2), True),
    StructField(MpsResponseFields.ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC, DecimalType(5, 2), True),
    StructField(MpsResponseFields.ALGORITHMIC_TRACE_DOB_SCORE_PERC, DecimalType(5, 2), True),
    StructField(MpsResponseFields.ALGORITHMIC_TRACE_GENDER_SCORE_PERC, DecimalType(5, 2), True),
    StructField(MpsResponseFields.ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC, DecimalType(5, 2), True),
    StructField(MpsResponseFields.ERROR_SUCCESS_CODE, StringType()),
])


def test_enrich_sgss_with_pds_mps_and_pdscrosscheckcondition(spark: SparkSession):
    input_data = [
        (('1:1',), '', 'MATCH', 'NO', '28/08/1891', '01/03/2021', 'LS20 5RB', 'Male', '', ''),
        (('1:2',), '', 'MATCH', 'NO', '28/08/1890', '01/03/2021', 'LS21 5RB', 'Male', '', ''),
        (('1:3',), '9018954892', 'REVEN', 'RAD', '28/08/1898', '01/03/2021', 'BD20 5RB', 'Male', '', '')
    ]

    input_df = spark.createDataFrame(spark.sparkContext.parallelize(input_data), SGSS_SCHEMA_FOR_PDS_MPS)

    mps_response_data = [
        ('1:1', '', 'MPS_ID1', '', '', '', None, '', '', '', 4, Decimal(0.0), Decimal(0.0), Decimal(0.0), Decimal(0.0), Decimal(0.0), Decimal(0.0), '00', 'default'),
        ('1:2', '', '', '', '', '', None, '', '', '', 4, Decimal(0.0), Decimal(0.0), Decimal(0.0), Decimal(0.0), Decimal(0.0), Decimal(0.0), '00', 'j'),
        ('1:3', '9018954892', '', 'RAD', 'REVEN', 'Male', 18980828, 'BD20 5RB', '', '', 1, Decimal(100.0), None, None, None, None, None, '00', 'k')
    ]

    mps_response_schema_with_pdscrosscheckcondition = StructType(
        MPS_RESPONSE_SCHEMA.fields + [StructField('PdsCrossCheckCondition', StringType()),
                                      ])

    pds_mps_df = spark.createDataFrame(spark.sparkContext.parallelize(mps_response_data), mps_response_schema_with_pdscrosscheckcondition)

    expected_schema = StructType([
        StructField('EVENT_ID', StringType()),
        StructField('PERSON_ID', StringType()),
        StructField('MPSGIVEN_NAME', StringType()),
        StructField('MPSFAMILY_NAME', StringType()),
        StructField('MPSGENDER', StringType()),
        StructField('MPSDATE_OF_BIRTH', IntegerType()),
        StructField('MPSPOSTCODE', StringType()),
        StructField('MPSEMAIL_ADDRESS', StringType()),
        StructField('MPSMOBILE_NUMBER', StringType()),
        StructField('MPS_TRACE_SUCCESSFUL', BooleanType()),
        StructField('MATCHED_CONFIDENCE_PERCENTAGE', DecimalType(5, 2)),
        StructField('ERROR_SUCCESS_CODE', StringType()),
        StructField('PdsCrossCheckCondition', StringType())
    ])

    expected_data = [
        ('1:1', 'MPS_ID1', '', '', '', None, '', '', '', False, Decimal(0.0), '00', 'default'),
        ('1:2', 'U00005YC1U', '', '', '', None, '', '', '', False, Decimal(0.0), '00',  'j'),
        ('1:3', '9018954892', 'RAD', 'REVEN', 'Male', 18980828, 'BD20 5RB', '', '', True, Decimal(100.0), '00', 'k')
    ]

    expected_df = spark.createDataFrame(spark.sparkContext.parallelize(expected_data), expected_schema)

    enriched_df = enrich_sgss_with_pds_mps(0, input_df, pds_mps_df)
    enriched_df = enriched_df.select('META.EVENT_ID', 'PERSON_ID', 'MPSGIVEN_NAME', 'MPSFAMILY_NAME', 'MPSGENDER', 'MPSDATE_OF_BIRTH', 'MPSPOSTCODE', 'MPSEMAIL_ADDRESS', 'MPSMOBILE_NUMBER', 'MPS_TRACE_SUCCESSFUL',
                                     'MPSConfidence.MATCHED_CONFIDENCE_PERCENTAGE', 'ERROR_SUCCESS_CODE', 'PdsCrossCheckCondition')

    assert compare_results(enriched_df, expected_df, join_columns=['EVENT_ID'])


def test_enrich_sgss_with_pds_mps_and_no_pdscrosscheckcondition(spark: SparkSession):
    input_data = [
        (('1:1',), '', 'MATCH', 'NO', '28/08/1891', '01/03/2021', 'LS20 5RB', 'Male', '', ''),
        (('1:2',), '', 'MATCH', 'NO', '28/08/1890', '01/03/2021', 'LS21 5RB', 'Male', '', ''),
        (('1:3',), '9018954892', 'REVEN', 'RAD', '28/08/1898', '01/03/2021', 'BD20 5RB', 'Male', '', '')
    ]

    input_df = spark.createDataFrame(spark.sparkContext.parallelize(input_data), SGSS_SCHEMA_FOR_PDS_MPS)

    mps_response_data = [
        ('1:1', '', 'MPS_ID1', '', '', '', None, '', '', '', 4, Decimal(0.0), Decimal(0.0), Decimal(0.0), Decimal(0.0), Decimal(0.0), Decimal(0.0), '00'),
        ('1:2', '', '', '', '', '', None, '', '', '', 4, Decimal(0.0), Decimal(0.0), Decimal(0.0), Decimal(0.0), Decimal(0.0), Decimal(0.0), '00'),
        ('1:3', '9018954892', '', 'RAD', 'REVEN', 'Male', 18980828, 'BD20 5RB', '', '', 1, Decimal(100.0), None, None, None, None, None, '00')
    ]
    pds_mps_df = spark.createDataFrame(spark.sparkContext.parallelize(mps_response_data), MPS_RESPONSE_SCHEMA)

    expected_schema = StructType([
        StructField('EVENT_ID', StringType()),
        StructField('PERSON_ID', StringType()),
        StructField('MPSGIVEN_NAME', StringType()),
        StructField('MPSFAMILY_NAME', StringType()),
        StructField('MPSGENDER', StringType()),
        StructField('MPSDATE_OF_BIRTH', IntegerType()),
        StructField('MPSPOSTCODE', StringType()),
        StructField('MPSEMAIL_ADDRESS', StringType()),
        StructField('MPSMOBILE_NUMBER', StringType()),
        StructField('MPS_TRACE_SUCCESSFUL', BooleanType()),
        StructField('MATCHED_CONFIDENCE_PERCENTAGE', DecimalType(5, 2)),
        StructField('ERROR_SUCCESS_CODE', StringType()),
    ])

    expected_data = [
        ('1:1', 'MPS_ID1', '', '', '', None, '', '', '', False, Decimal(0.0), '00'),
        ('1:2', 'U00005YC1U', '', '', '', None, '', '', '', False, Decimal(0.0), '00'),
        ('1:3', '9018954892', 'RAD', 'REVEN', 'Male', 18980828, 'BD20 5RB', '', '', True, Decimal(100.0), '00')
    ]
    expected_df = spark.createDataFrame(spark.sparkContext.parallelize(expected_data), expected_schema)

    enriched_df = enrich_sgss_with_pds_mps(0, input_df, pds_mps_df)
    enriched_df = enriched_df.select('META.EVENT_ID', 'PERSON_ID', 'MPSGIVEN_NAME', 'MPSFAMILY_NAME', 'MPSGENDER', 'MPSDATE_OF_BIRTH', 'MPSPOSTCODE', 'MPSEMAIL_ADDRESS', 'MPSMOBILE_NUMBER', 'MPS_TRACE_SUCCESSFUL',
                                     'MPSConfidence.MATCHED_CONFIDENCE_PERCENTAGE', 'ERROR_SUCCESS_CODE')

    assert compare_results(enriched_df, expected_df, join_columns=['EVENT_ID'])
