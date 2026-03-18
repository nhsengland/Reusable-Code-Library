from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from dsp.datasets.mps.request import Fields as MPSRequest
from dsp.datasets.sgss.ingestion.sgss.config import SGSS_DELTA_MPS_FIELDS, Fields, SGSS_MPS_FIELDS
from dsp.datasets.sgss.enrichments.mps import get_mps_fields

from dsp.shared.logger import add_fields, log_action


def get_pds_mps_cleaned_df(pds_mps_df: DataFrame) -> DataFrame:
    mps_df = get_mps_fields(pds_mps_df, SGSS_MPS_FIELDS+SGSS_DELTA_MPS_FIELDS)
    for column in SGSS_DELTA_MPS_FIELDS:
        mps_df = mps_df.withColumnRenamed(column, f'MPS{column}')
    return mps_df


@log_action()
def enrich_sgss_with_pds_mps(_submission_id: int, input_df: DataFrame, pds_mps_df: DataFrame):
    pds_mps_df = get_pds_mps_cleaned_df(pds_mps_df)

    enriched_df = input_df.alias('lhs').join(pds_mps_df.alias('rhs'),
                                             on=col('lhs.META.EVENT_ID') == col('rhs.UNIQUE_REFERENCE'),
                                             how='left_outer').drop(MPSRequest.UNIQUE_REFERENCE)
    enriched_df = enriched_df.withColumn(Fields.MPS_TRACE_SUCCESSFUL, col(Fields.PERSON_ID).rlike('^\\d{10}$'))

    matched_count = enriched_df.where(col(Fields.MPS_TRACE_SUCCESSFUL)).count()
    unmatched_count = enriched_df.where(~col(Fields.MPS_TRACE_SUCCESSFUL)).count()

    add_fields(matched_count=matched_count, unmatched_count=unmatched_count)

    return enriched_df
