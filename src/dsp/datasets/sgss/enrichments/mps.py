from typing import List

from pyspark.sql.types import Row
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, regexp_extract, col, expr, when, upper, substring, date_format, length, \
    regexp_replace

from dsp.datasets.postcode.common import normalise_postcode
from dsp.enrichments import get_person_id
from dsp.datasets.mps.request import Fields as MPSRequest, Order as MPSRequestOrder
from dsp.datasets.sgss.ingestion.sgss.config import Fields
from dsp.integration.mps import FieldConstraints
from dsp.shared.common import base36encode



def mps_request(df: DataFrame) -> DataFrame:
    date_expression = date_format(col(Fields.Lab_Report_Date), FieldConstraints.AS_AT_DATE_FORMAT).cast("int")

    return (
        df.select(
            regexp_replace(col('META.EVENT_ID').cast('string'), ':', '-').alias(MPSRequest.UNIQUE_REFERENCE),
            col(Fields.Patient_NHS_Number).substr(1, FieldConstraints.NHS_NO_LENGTH).alias(MPSRequest.NHS_NO),
            regexp_extract(Fields.Patient_Surname, r'^(\p{ASCII}+)$', 1).substr(1,
                                                                                FieldConstraints.FAMILY_NAME_LENGTH).alias(
                MPSRequest.FAMILY_NAME),
            regexp_extract(expr(f"split({Fields.Patient_Forename}, ' ')[0]"), r'^(\p{ASCII}+)$', 1).substr(1,
                                                                                                           FieldConstraints.GIVEN_NAME_LENGTH).alias(
                MPSRequest.GIVEN_NAME),
            expr(f"array_join(slice(split({Fields.Patient_Forename}, ' '), 2, 1000), ' ')").substr(1,
                                                                                                   FieldConstraints.OTHER_GIVEN_NAME_LENGTH).alias(
                MPSRequest.OTHER_GIVEN_NAME),
            regexp_extract(normalise_postcode(Fields.Patient_PostCode), r'^(\p{ASCII}+)$', 1).substr(1,
                                                                                 FieldConstraints.POSTCODE_LENGTH).alias(
                MPSRequest.POSTCODE),
            when(substring(upper(col(Fields.Patient_Sex)), 1, 1) == lit('M'), lit('1')).
                when(substring(upper(col(Fields.Patient_Sex)), 1, 1) == lit('F'), lit('2')).
                when(col(Fields.Patient_Sex).isin('1', '2', '9'), col(Fields.Patient_Sex)).
                otherwise(lit('0')).  # Something has been specified, but not one of our recognised ones so Unknown
                alias(MPSRequest.GENDER),
            date_format(col(Fields.Patient_Date_Of_Birth), FieldConstraints.DATE_OF_BIRTH_FORMAT).cast('int').alias(
                MPSRequest.DATE_OF_BIRTH),
            lit(None).alias(MPSRequest.DATE_OF_DEATH),
            lit('').alias(MPSRequest.ADDRESS_LINE1),
            lit('').alias(MPSRequest.ADDRESS_LINE2),
            lit('').alias(MPSRequest.ADDRESS_LINE3),
            lit('').alias(MPSRequest.ADDRESS_LINE4),
            lit('').alias(MPSRequest.ADDRESS_LINE5),
            lit(None).cast('int').alias(MPSRequest.ADDRESS_DATE),
            lit('').alias(MPSRequest.GP_PRACTICE_CODE),
            lit('').alias(MPSRequest.NHAIS_POSTING_ID),
            when(length(date_expression) == 8, date_expression).otherwise(lit(None)).alias(MPSRequest.AS_AT_DATE),
            lit('').alias(MPSRequest.LOCAL_PATIENT_ID),
            lit('').alias(MPSRequest.INTERNAL_ID),
            lit('').alias(MPSRequest.TELEPHONE_NUMBER),
            col(Fields.P2_email).alias(MPSRequest.EMAIL_ADDRESS),
            col(Fields.P2_mobile).alias(MPSRequest.MOBILE_NUMBER)
        ).select(*MPSRequestOrder)
    )


def add_uniq_id(row_and_index: Row) -> Row:
    row, index = row_and_index
    row_dict = row.asDict(True)

    unique_ref = row_dict['UNIQUE_REFERENCE']
    submission_id, index = unique_ref.split(':')

    submission_and_index = str(submission_id) + str(index).zfill(7)
    uniqid_base36 = 'U' + base36encode(int(submission_and_index)).zfill(9)
    row_dict['UNIQ_ID'] = uniqid_base36
    return Row(**row_dict)


def add_person_id(row: Row, schema) -> Row:
    row_dict = row.asDict(True)
    row_dict["PERSON_ID"] = get_person_id(row)

    return Row(*[row_dict[n] for n in schema.names])


def get_mps_fields(pds_matched_df: DataFrame, mps_fields: List[str]) -> DataFrame:
    if Fields.PdsCrossCheckCondition in pds_matched_df.schema.names:
        if Fields.PdsCrossCheckCondition not in mps_fields:
            mps_fields = mps_fields + [Fields.PdsCrossCheckCondition]

    df_matched_with_confidence = (
        pds_matched_df
            .withColumn('MPSConfidence', expr(
            "struct(ALGORITHMIC_TRACE_DOB_SCORE_PERC, ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC, ALGORITHMIC_TRACE_GENDER_SCORE_PERC, ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC, MATCHED_ALGORITHM_INDICATOR, MATCHED_CONFIDENCE_PERCENTAGE, ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC)"))
            .withColumn('PERSON_ID', lit(''))
            .withColumn('UNIQ_ID', lit(''))
    )
    matched_schema = df_matched_with_confidence.schema
    df_matched_with_confidence_and_person_id = df_matched_with_confidence.rdd.zipWithIndex().map(
        lambda x: add_uniq_id(x)).map(
        lambda x: add_person_id(x, matched_schema)).toDF(matched_schema)

    return df_matched_with_confidence_and_person_id.select(*mps_fields)
