import os
from typing import Dict, List, Tuple

from botocore.exceptions import ClientError
from dsp.datasets.common import Fields as CommonFields
from dsp.datasets.sgss.ingestion.sgss.config import META_SCHEMA
from dsp.integration.mps.generate_mps_file import generate_request_files
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row, StructField, StructType
from shared.common import format_submission_id
from shared.logger import add_fields, log_action
from shared.store.mesh_upload_jobs import MeshUploadJobs


def update_sender_mailbox(transfer_id: str, sender_mailbox: str) -> bool:
    args = dict(
        Key=dict(transfer_id=transfer_id),
        UpdateExpression="SET mailbox_from = :val1",
        ExpressionAttributeValues={":val1": sender_mailbox},
    )

    try:
        response = MeshUploadJobs.table.update_item(**args)
    except ClientError as e:
        print(f"Failed to update sender mailbox: {e.response['Error']['Code']}")
        return False

    return response["ResponseMetadata"]["HTTPStatusCode"] == 200


def add_traceability_columns(spark: SparkSession, raw_df: DataFrame, metadata: Dict[str, str]) -> DataFrame:
    current_schema: StructType = raw_df.schema

    target_schema = StructType([StructField(CommonFields.META, META_SCHEMA, False)] + current_schema.fields)
    zipped_rdd = raw_df.rdd.zipWithIndex()

    submission_id = metadata["submission_id"]
    submitted_timestamp = metadata["timestamp"]

    def _add_fields(existing_row_and_index):
        row, index = existing_row_and_index

        return Row(Row("0", "{}:{}".format(submission_id, index), submitted_timestamp, index, 0), *row)

    indexed_rdd = zipped_rdd.map(_add_fields)

    df = spark.createDataFrame(indexed_rdd, target_schema)

    return df


@log_action(log_args=["pds_only_mps_core_sender_mailbox", "submission_id"])
def create_mps_request(
    spark: SparkSession, pds_only_mps_core_sender_mailbox: str, submission_id: int, df_unmatched: DataFrame
) -> Tuple[List[str], List[str]]:
    mps_path = f"s3://nhsd-dspp-core-{os.environ['env']}-raw/submissions/{format_submission_id(submission_id)}/mps"

    response = generate_request_files(
        spark,
        df_unmatched,
        storage_path=mps_path,
        submission_id=str(9999999999),
        chunk_size_rows=500000,
        local_id_prefix="ADHOC",
    )

    local_ids = []
    request_ids, transfer_ids = response
    for transfer_id in transfer_ids:
        # Send from a different mailbox so it only runs PDS checks (not MPS)
        update_sender_mailbox(transfer_id, pds_only_mps_core_sender_mailbox)
        MeshUploadJobs.mark_ready(transfer_id)
        job = MeshUploadJobs.get_by_key(transfer_id=transfer_id)
        local_ids.append(job.mesh_metadata.local_id)

    add_fields(local_ids=local_ids, transfer_ids=transfer_ids)
    return local_ids, transfer_ids
