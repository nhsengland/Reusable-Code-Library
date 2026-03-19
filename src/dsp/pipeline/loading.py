from typing import Any, Dict, Callable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, Row

from dsp.dam import current_record_version, current_dataset_version
from dsp.datasets.common import Fields as CommonFields
from dsp.pipeline import Metadata
from dsp.pipeline.pipeline import PipelineRejection
from dsp.shared.common import parse_timestamp
from dsp.shared.logger import log_action

META_SCHEMA = StructType(
    [
        StructField(CommonFields.DATASET_VERSION, StringType(), False),
        StructField(CommonFields.EVENT_ID, StringType(), False),
        StructField(CommonFields.EVENT_RECEIVED_TS, TimestampType(), False),
        StructField(CommonFields.RECORD_INDEX, IntegerType(), False),
        StructField(CommonFields.RECORD_VERSION, IntegerType(), False),
    ]
)


@log_action(log_args=['metadata'])
def add_traceability_columns(spark: SparkSession, raw_df: DataFrame, metadata: Metadata) -> DataFrame:
    """ ensure record traceability columns are present in the dataset
        e.g. add in our submission_id, and record_number
    Args:
        spark (SparkSession): the spark session
        raw_df (DataFrame): the freshly loaded dataset, 'unmolested'
        metadata (dict): submission metadata

    Returns:
        DataFrame: dataframe with the extra required columns for traceability
    """

    current_schema = raw_df.schema  # type: StructType

    if CommonFields.META in current_schema.names:
        raise PipelineRejection(f"Column name '{CommonFields.META}' is a reserved name.")

    target_schema = StructType(
        [
            StructField(CommonFields.META, META_SCHEMA, False)
        ] + current_schema.fields
    )
    zipped_rdd = raw_df.rdd.zipWithIndex()

    traceability_meta_for_index_provider = get_traceability_meta_for_index_provider(metadata)

    def _add_fields(existing_row_and_index):
        row, index = existing_row_and_index

        return Row(traceability_meta_for_index_provider(index), *row)

    indexed_rdd = zipped_rdd.map(_add_fields)

    df = spark.createDataFrame(indexed_rdd, target_schema)

    return df


def get_traceability_meta_for_index_provider(metadata: Dict[str, Any]) -> Callable[[int], Row]:
    submission_id = metadata['submission_id']
    submitted_timestamp = parse_timestamp(metadata['submitted_timestamp'])
    dataset_id = metadata['dataset_id']
    dataset_version = current_dataset_version(dataset_id)
    record_version = current_record_version(dataset_id)

    # Retain record index to keep consistency with shared meta data structure.
    return lambda index: Row(
        dataset_version, '{}:{}'.format(submission_id, index), submitted_timestamp, index, record_version
    )



def get_traceability_meta(index: int, metadata: Dict[str, Any]) -> Row:
    return get_traceability_meta_for_index_provider(metadata)(index)
