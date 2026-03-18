from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, to_date, lit

from dsp.enrichments import join, generate_unique_ids, get_person_id, get_mps_confidence_dict
from dsp.datasets.mps.request import Fields as MpsRequestFields
from dsp.datasets.mps.response import Fields as MpsResponseFields
from dsp.datasets.models.dids_mps import DIDSMPSModel
from dsp.integration.mps import FieldConstraints


def mps_request(df: DataFrame) -> DataFrame:
    df_mps_request = df.select(
        col('META.EVENT_ID').cast('string').alias(MpsRequestFields.UNIQUE_REFERENCE),
        col('NHS_NUMBER').substr(1, FieldConstraints.NHS_NO_LENGTH).alias(MpsRequestFields.NHS_NO),
        col('PERSON_GENDER_CODE').alias(MpsRequestFields.GENDER),
        col('POSTCODE').alias(MpsRequestFields.POSTCODE),
        date_format(to_date('PERSON_BIRTH_DATE', 'yyyy-MM-dd'), FieldConstraints.DATE_OF_BIRTH_FORMAT).cast(
            'int').alias(MpsRequestFields.DATE_OF_BIRTH),
        date_format(to_date('DIAGNOSTIC_TEST_DATE', 'yyyy-MM-dd'), FieldConstraints.AS_AT_DATE_FORMAT).cast('int').alias(MpsRequestFields.AS_AT_DATE),
        lit(None).alias(MpsRequestFields.LOCAL_PATIENT_ID),
    )
    return df_mps_request


def enrich_with_pds_mps(submission_id: int, mps_input: DataFrame, df_pds_mps: DataFrame) -> DataFrame:
    """
    Enriches a dataframe of with respective values from df_pds_mps.

    Will be matched on UNIQUE_REFERENCE.

    Args:
        mps_input: a dataframe of input pid
        df_pds_mps: a dataframe of dsp.integration.mps.mps_schema

    Returns:
        A dataframe enriched with values from df_pds_mps.
    """

    rdd_mps_input = mps_input.rdd
    rdd_pds_mps = df_pds_mps.rdd
    rdd_pds_mps_with_uniqids = generate_unique_ids(submission_id, rdd_pds_mps)

    rdd_joined = join(
        rdd_mps_input, rdd_pds_mps_with_uniqids,
        ['META.EVENT_ID'],
        [MpsResponseFields.UNIQUE_REFERENCE]
    )

    def merge(row: Row) -> Row:
        _, (row_left, row_right) = row

        if row_right is None:
            return row_left

        row_left['MPSConfidence'] = get_mps_confidence_dict(row_right)
        row_left['Person_ID'] = get_person_id(row_right)
        return row_left

    rdd_merged = rdd_joined.map(merge)

    return rdd_merged.toDF(DIDSMPSModel.get_struct())
