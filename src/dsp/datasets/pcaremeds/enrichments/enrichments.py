from datetime import datetime

from dsp.enrichments import join, generate_unique_ids, get_mps_confidence_dict, get_person_id
from dsp.datasets.mps.request import Fields as MpsRequestFields
from dsp.datasets.mps.response import Fields as MpsResponseFields
from dsp.datasets.models.pcaremeds import PrimaryCareMedicineModel
from dsp.integration.mps import FieldConstraints
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql.functions import length, col, when, lit, date_format, to_date
from pyspark.sql.types import StringType

from dsp.datasets.mps.response import Fields as MpsResponseFields
from dsp.integration.mps.constants import (
    RESULT_INDICATING_NHS_NUMBERS
)


def mps_request(df: DataFrame) -> DataFrame:
    date_expression = date_format(to_date(col("ProcessedPeriod").cast(StringType()), "yyyyMM"),
                                  FieldConstraints.AS_AT_DATE_FORMAT
                                  ).cast("int")

    df_mps_request = df.select(
        col('META.EVENT_ID').cast('string').alias(MpsRequestFields.UNIQUE_REFERENCE),
        col('NHSNumber').substr(1, FieldConstraints.NHS_NO_LENGTH).alias(MpsRequestFields.NHS_NO),
        lit(None).alias(MpsRequestFields.FAMILY_NAME),
        lit(None).alias(MpsRequestFields.GIVEN_NAME),
        lit(None).alias(MpsRequestFields.POSTCODE),
        when(col('PatientGender').isin('1', '2', '9'), col('PatientGender')).
            otherwise(lit('0')).  # Something has been specified, but not one of our recognised ones so Unknown
            alias(MpsRequestFields.GENDER),
        lit(None).alias(MpsRequestFields.LOCAL_PATIENT_ID),
        date_format(col('PatientDoB'), FieldConstraints.DATE_OF_BIRTH_FORMAT).cast('int').alias(
            MpsRequestFields.DATE_OF_BIRTH),
        lit(None).alias(MpsRequestFields.DATE_OF_DEATH),
        when(length(date_expression) == 8, date_expression).otherwise(lit(None)).alias(MpsRequestFields.AS_AT_DATE),
        lit(None).alias(MpsRequestFields.EMAIL_ADDRESS),
        lit(None).alias(MpsRequestFields.MOBILE_NUMBER),
    )
    return df_mps_request


def enrich_pcaremeds_with_pds(submission_id: int, mps_input: DataFrame, df_pds_mps: DataFrame) -> DataFrame:
    """
    Enriches a dataframe of Referrals with respective values from df_pds_mps.

    Will be matched on Referral.META.EVENT_ID / UNIQUE_REFERENCE.

    Args:
        mps_input: a dataframe of input pid
        df_pds_mps: a dataframe of dsp.integration.mps.mps_schema

    Returns:
        A dataframe of Referrals enriched with values from df_pds_mps.
    """

    def _is_valid_nhs_no(nhs_no: str):
        return nhs_no and nhs_no not in RESULT_INDICATING_NHS_NUMBERS

    def pcaremeds_person_id(mps_response_row: Row) -> str:
        nhs_no = mps_response_row[MpsResponseFields.MATCHED_NHS_NO]

        if _is_valid_nhs_no(nhs_no):
            return nhs_no

        return None

    rdd__mps_input = mps_input.rdd
    rdd_pds_mps = df_pds_mps.rdd

    rdd_joined = join(
        rdd__mps_input, rdd_pds_mps,
        ['META.EVENT_ID'],
        [MpsResponseFields.UNIQUE_REFERENCE]
    )

    def merge(row: Row) -> Row:
        _, (row_left, row_right) = row

        if row_right is None:
            return row_left

        row_left['Person_ID'] = pcaremeds_person_id(row_right)
        row_left['PatientGPODS'] = row_right[MpsResponseFields.GP_PRACTICE_CODE]
        row_left['MPSGender'] = row_right[MpsResponseFields.GENDER]
        row_left['MPSPostcode'] = row_right[MpsResponseFields.POSTCODE]

        date_of_birth = None
        if row_right[MpsResponseFields.DATE_OF_BIRTH]:
            date_of_birth = datetime.strptime(str(row_right[MpsResponseFields.DATE_OF_BIRTH]), '%Y%m%d')

        row_left['MPSDateOfBirth'] = date_of_birth

        return row_left

    rdd_merged = rdd_joined.map(merge)
    rdd_rehydrated = rdd_merged.map(lambda r: PrimaryCareMedicineModel(r).as_row(force_derivation=False))

    return rdd_rehydrated.toDF(PrimaryCareMedicineModel.get_struct(include_derived_attr=False))
