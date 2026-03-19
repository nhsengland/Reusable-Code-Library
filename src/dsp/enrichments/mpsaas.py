from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DecimalType

from dsp.datasets.common import Fields as CommonFields
from dsp.enrichments import generate_unique_ids, get_person_id
from dsp.datasets.mps.request import Fields as MpsRequestFields
from dsp.datasets.mpsaas.output import Fields as Output
from dsp.datasets.mpsaas.submitted import Fields as SourceFields
from dsp.integration.mps import FieldConstraints

mpsaas_response_schema = StructType([
    StructField(Output.UNIQUE_REFERENCE, StringType(), True),
    StructField(Output.REQ_NHS_NUMBER, StringType(), True),
    StructField(Output.FAMILY_NAME, StringType(), True),
    StructField(Output.GIVEN_NAME, StringType(), True),
    StructField(Output.OTHER_GIVEN_NAME, StringType(), True),
    StructField(Output.GENDER, StringType(), True),
    StructField(Output.DATE_OF_BIRTH, IntegerType(), True),
    StructField(Output.DATE_OF_DEATH, IntegerType(), True),
    StructField(Output.ADDRESS_LINE1, StringType(), True),
    StructField(Output.ADDRESS_LINE2, StringType(), True),
    StructField(Output.ADDRESS_LINE3, StringType(), True),
    StructField(Output.ADDRESS_LINE4, StringType(), True),
    StructField(Output.ADDRESS_LINE5, StringType(), True),
    StructField(Output.POSTCODE, StringType(), True),
    StructField(Output.GP_PRACTICE_CODE, StringType(), True),
    StructField(Output.NHAIS_POSTING_ID, StringType(), True),
    StructField(Output.AS_AT_DATE, IntegerType(), True),
    StructField(Output.LOCAL_PATIENT_ID, StringType(), True),
    StructField(Output.INTERNAL_ID, StringType(), True),
    StructField(Output.TELEPHONE_NUMBER, StringType(), True),
    StructField(Output.MOBILE_NUMBER, StringType(), True),
    StructField(Output.EMAIL_ADDRESS, StringType(), True),
    StructField(Output.SENSITIVTY_FLAG, StringType(), True),
    StructField(Output.MPS_ID, StringType(), True),
    StructField(Output.ERROR_SUCCESS_CODE, StringType(), True),
    StructField(Output.MATCHED_NHS_NO, StringType(), True),
    StructField(Output.MATCHED_ALGORITHM_INDICATOR, IntegerType(), True),
    StructField(Output.MATCHED_CONFIDENCE_PERCENTAGE, DecimalType(5, 2), True),
    StructField(Output.ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC, DecimalType(5, 2), True),
    StructField(Output.ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC, DecimalType(5, 2), True),
    StructField(Output.ALGORITHMIC_TRACE_DOB_SCORE_PERC, DecimalType(5, 2), True),
    StructField(Output.ALGORITHMIC_TRACE_GENDER_SCORE_PERC, DecimalType(5, 2), True),
    StructField(Output.ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC, DecimalType(5, 2), True),
    StructField(Output.PERSON_ID, StringType(), True),
])


def mps_request(df_unmatched: DataFrame) -> DataFrame:
    df_mps_request = df_unmatched.select(
        col(SourceFields.UNIQUE_REFERENCE).alias(MpsRequestFields.UNIQUE_REFERENCE),
        col(SourceFields.NHS_NO).substr(1, FieldConstraints.NHS_NO_LENGTH).alias(MpsRequestFields.NHS_NO),
        col(SourceFields.POSTCODE).substr(1, FieldConstraints.POSTCODE_LENGTH).alias(MpsRequestFields.POSTCODE),
        col(SourceFields.GIVEN_NAME).alias(MpsRequestFields.GIVEN_NAME),
        col(SourceFields.OTHER_GIVEN_NAME).alias(MpsRequestFields.OTHER_GIVEN_NAME),
        col(SourceFields.FAMILY_NAME).alias(MpsRequestFields.FAMILY_NAME),
        col(SourceFields.GENDER).alias(MpsRequestFields.GENDER),
        col(SourceFields.ADDRESS_LINE1).alias(MpsRequestFields.ADDRESS_LINE1),
        col(SourceFields.ADDRESS_LINE2).alias(MpsRequestFields.ADDRESS_LINE2),
        col(SourceFields.ADDRESS_LINE3).alias(MpsRequestFields.ADDRESS_LINE3),
        col(SourceFields.ADDRESS_LINE4).alias(MpsRequestFields.ADDRESS_LINE4),
        col(SourceFields.ADDRESS_LINE5).alias(MpsRequestFields.ADDRESS_LINE5),
        col(SourceFields.LOCAL_PATIENT_ID).alias(MpsRequestFields.LOCAL_PATIENT_ID),
        col(SourceFields.DATE_OF_BIRTH).cast('int').alias(MpsRequestFields.DATE_OF_BIRTH),
        col(SourceFields.DATE_OF_DEATH).cast('int').alias(MpsRequestFields.DATE_OF_DEATH),
        col(SourceFields.AS_AT_DATE).cast('int').alias(MpsRequestFields.AS_AT_DATE),
        col(SourceFields.GP_PRACTICE_CODE).alias(MpsRequestFields.GP_PRACTICE_CODE),
        col(SourceFields.NHAIS_POSTING_ID).alias(MpsRequestFields.NHAIS_POSTING_ID),
    )
    return df_mps_request


def enrich_mpsaas_with_person_id(submission_id: int, df_mpsaas: DataFrame) -> DataFrame:
    rdd_mpsaas = df_mpsaas.rdd
    rdd_mpsaas_with_uniqids = generate_unique_ids(submission_id, rdd_mpsaas)

    def add_person_id(row: Row) -> Row:
        row_values = row.asDict()
        row_values[Output.PERSON_ID] = get_person_id(row)
        del row_values[CommonFields.UNIQ_ID]
        return Row(**row_values)

    rdd_result = rdd_mpsaas_with_uniqids.map(add_person_id)
    return rdd_result.toDF(mpsaas_response_schema)
