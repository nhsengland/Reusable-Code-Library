from typing import Set

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType

from dsp.datasets.common import PDSCohortFields
from dsp.model.pds_record import PDSRecord, PDSRecordPaths


def get_latest_nhs_number(pds_df: DataFrame, nhs_no: str):
    def walk_to_the_top(walking_nhs_no: str):
        res = pds_df.where(col(PDSCohortFields.NHS_NUMBER) == lit(walking_nhs_no)).collect()
        assert len(res) == 1
        rec = PDSRecord.from_json(res[0][PDSCohortFields.RECORD])
        replaced_by = rec.replaced_by_nhs_number()
        if replaced_by:
            return walk_to_the_top(replaced_by)

        return walking_nhs_no

    top = walk_to_the_top(nhs_no)

    return top


def get_all_related_nhs_numbers(replaced_by_df: DataFrame, nhs_no: str) -> Set[str]:
    latest = get_latest_nhs_number(replaced_by_df, nhs_no)

    affected_nhs_nos = {latest}

    def walk_to_the_bottom(walking_nhs_no: str):
        res = replaced_by_df.where(col(PDSRecordPaths.REPLACED_BY) == lit(walking_nhs_no)).collect()
        for row in res:
            prev_nhs_no = row[PDSCohortFields.NHS_NUMBER]
            affected_nhs_nos.add(prev_nhs_no)
            walk_to_the_bottom(prev_nhs_no)

    walk_to_the_bottom(latest)

    return affected_nhs_nos


def get_replaced_by_pds_df(pds_df: DataFrame) -> DataFrame:
    def get_replaced_by(record_json: str) -> str:
        record = PDSRecord.from_json(record_json)
        return record.replaced_by_nhs_number()

    get_replaced_by = udf(get_replaced_by, StringType())

    replaced_by_df = pds_df.withColumn(PDSRecordPaths.REPLACED_BY, get_replaced_by(PDSCohortFields.RECORD))
    replaced_by_df.cache()

    return replaced_by_df
