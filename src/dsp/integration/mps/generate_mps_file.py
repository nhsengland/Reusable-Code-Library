import os
from collections import namedtuple
from glob import glob
from typing import List, Optional, Union, Tuple
from uuid import uuid4

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import regexp_replace, col, when, row_number
from pyspark.sql.session import SparkSession

from dsp.datasets.mps.request import Fields as RequestFields
from dsp.datasets.mps.request_header import Fields as RequestHeaderFields
from dsp.integration.mps.mps_schema import generate_mps_request_header_fields
from dsp.shared.aws import s3_split_path, s3_bucket, ssm_parameter
from dsp.shared.constants import FT, MESH_WORKFLOW_ID, MESH_MAILBOXES
from dsp.shared.logger import log_action, add_fields
from dsp.shared.models import MeshMetadata, MeshTransferStatus, MeshTransferJobPriority
from dsp.shared.store import Submissions
from dsp.shared.store.mesh_upload_jobs import MeshUploadJobs
from dsp.shared.store.mps_manifests import MpsManifests

GeneratorConfig = namedtuple('GeneratorConfig', ['fields', 'reference_key', 'file_path'])
GeneratedFileInfo = namedtuple('GeneratedFileInfo', ['id', 'header_file_path', 'body_file_path', 'mesh_filename'])


def hyphen_unique_reference(df: DataFrame) -> DataFrame:
    """
    Replaces colons (:) in the UNIQUE_REFERENCE field with hyphens (-).

    Args:
        df: The DataFrame to replace values in

    Returns:
        The updated DataFrame.
    """
    return df.withColumn(RequestFields.UNIQUE_REFERENCE, regexp_replace(RequestFields.UNIQUE_REFERENCE, ':', '-'))


def colon_unique_reference(df: DataFrame) -> DataFrame:
    """
    Replaces hyphens (-) in the UNIQUE_REFERENCE field with colons (:).

    Args:
        df: The DataFrame to replace values in

    Returns:
        The updated DataFrame.
    """
    return df.withColumn(RequestFields.UNIQUE_REFERENCE, regexp_replace(RequestFields.UNIQUE_REFERENCE, '-', ':'))


@log_action(log_args=["raw_path", "submission_id", "chunk_size_rows"])
def generate_mps_files(
        spark: SparkSession,
        storage_path: str,
        submission_id: Union[int, str],
        df_unmatched: DataFrame,
        chunk_size_rows: int = 500000,
        local_id_prefix: Optional[str] = None,
        mailbox_from: Optional[str] = None,
):
    """Provided with a dataframe containing records that were unmatched by the local cross-check and a dataframe
    containing records that were matched by the local cross-check, take the appropriate action to determine how to
    proceed. If there are zero unmatched records, there is no need to send anything to MPS or persist anything to
    S3 - we have all the information we need in the matched dataframe - so we can proceed with that info.

    However, if we have some records in the unmatched dataframe, we need to fire off request(s) to MPS to let them
    run their algorithms against the data we send.

    Args:
        spark: SparkSession instance
        storage_path: storage path on S3, where the MPS files will be written
        submission_id: submission id as either int or str
        df_unmatched: unmatched dataframe from the cross check
        chunk_size_rows: optionally, a number of chunks to divide the request file into,
                         the default of 500,000 is the maximum size of chunks MESH can accept
        local_id_prefix: Optionally, the local ID prefix to use for the MPS request.
                         See shared.constants.MPS_LOCAL_ID_PREFIXES for the list of valid local ID prefixes.
        mailbox_from: Optionally, the mailbox to send the MPS request from.
    """
    if not df_unmatched.count():
        add_fields(message="No unmatched data")
        return

    request_ids, transfer_ids = generate_request_files(
        spark,
        hyphen_unique_reference(df_unmatched),
        storage_path,
        submission_id,
        chunk_size_rows,
        local_id_prefix,
        mailbox_from
    )

    create_mps_manifest(submission_id=submission_id, request_ids=request_ids)

    # Now that the manifest is created, we're okay for the uploads to be sent
    for transfer_id in transfer_ids:
        MeshUploadJobs.mark_ready(transfer_id)


@log_action(log_args=["submission_id", "request_ids", "response_id"])
def create_mps_manifest(submission_id: Union[int, str], request_ids: List[str], response_id: Optional[str] = None):
    response_ids = []
    if response_id:
        response_ids = [response_id]

    manifest_id = MpsManifests.create_mps_manifest(submission_id=submission_id,
                                                   request_chunks=request_ids,
                                                   response_chunks=response_ids)
    add_fields(manifest_id=manifest_id)


def find_chunks(path: str, suffix: Optional[str] = None) -> List[str]:
    if path.startswith('s3://'):
        _, bucket, key_prefix = s3_split_path(path)
        keys = s3_bucket(bucket).objects.filter(Prefix=f'{key_prefix}/')
        if suffix is not None:
            keys = filter(lambda k: k.key.endswith(suffix), keys)
        return [f's3://{bucket}/{k.key}' for k in keys]
    else:
        return glob(f'{path}/*{suffix}')


def strip_invalid_postcodes(df_unmatched: DataFrame) -> DataFrame:
    if "POSTCODE" in df_unmatched.columns:
        expression = r'^[a-zA-Z]{1,2}[0-9][a-zA-Z0-9]?\s*[0-9][a-zA-Z]{2}$'
        df_unmatched = df_unmatched.withColumn(
            "POSTCODE",
            when(
                col("POSTCODE").rlike(expression),
                col("POSTCODE")
            ).otherwise("")
        )
    return df_unmatched


@log_action(log_args=['storage_path', 'submission_id', 'chunk_size_rows', 'local_id_prefix', 'mailbox_from'])
def generate_request_files(
        spark: SparkSession,
        df_unmatched: DataFrame,
        storage_path: str,
        submission_id: str,
        chunk_size_rows: int,
        local_id_prefix: str = None,
        mailbox_from: Optional[str] = None
) -> Tuple[List[str], List[str]]:
    """
    Generate the required request files for an MPS request and store them in the specified directory

    Args:
        spark (SparkSession): The active Spark session
        df_unmatched (DataFrame): A data frame of unmatched person identifiers to be requested
        storage_path (str): Path to store the files to be sent via MESH
        submission_id (str): The ID of the submission which has triggered this request
        chunk_size_rows (int): The number of rows to include in each request chunk
        local_id_prefix (str): A prefix to apply - defaults to None - but allows for customisation of the behaviour for
                responses to requests by switching on the local id. See MPS_LOCAL_ID_PREFIXES for prefixes
        mailbox_from (str): Id of the mailbox to send MESH request from
    Returns:
        List[str]: A list of IDs of the generated chunks, and a list of the transfer ids of the uploads
    """
    if not mailbox_from:
        mailbox_from = ssm_parameter('/core/mps_core_sender_mailbox')

    request_ids = []
    transfer_ids = []

    df_unmatched = strip_invalid_postcodes(df_unmatched)

    # Rank all the rows so we can pick off batches by row number range
    df_unmatched = df_unmatched.withColumn(
        'rnk',
        row_number().over(
            Window.partitionBy().
                orderBy(
                df_unmatched.UNIQUE_REFERENCE.asc()  # sort for repeatability in case spark decides to re-evaluate
            )
        )
    )

    df_unmatched = df_unmatched.cache()

    try:
        unmatched_count = df_unmatched.count()

        ranges = []

        for start in range(1, unmatched_count + 1, chunk_size_rows):
            end = start + chunk_size_rows - 1
            ranges.append((start, end))

            df_chunk = df_unmatched.filter(
                df_unmatched.rnk.between(start, end)
            ).drop('rnk')

            request_config = GeneratorConfig(
                generate_mps_request_header_fields(), RequestHeaderFields.REQUEST_REFERENCE, 'request'
            )

            file_info = generate_files(spark, df_chunk, storage_path, request_config)

            Submissions.add_dataset_id_tag(file_info.header_file_path, submission_id, True)
            Submissions.add_dataset_id_tag(file_info.body_file_path, submission_id, True)

            header = find_chunks(file_info.header_file_path, FT.CSV)[0]
            chunks_body = find_chunks(file_info.body_file_path, FT.CSV)

            local_id = f'{submission_id}_{file_info.id}'

            if local_id_prefix:
                local_id = f'{local_id_prefix}_{local_id}'

            transfer_id = _create_mesh_upload_job(
                s3_locations=[header, *chunks_body],
                filename=file_info.mesh_filename,
                local_id=local_id,
                mailbox_from=mailbox_from,
            )

            request_ids.append(file_info.id)
            transfer_ids.append(transfer_id)
    finally:
        df_unmatched.unpersist()

    add_fields(ranges=ranges, request_ids=request_ids, transfer_ids=transfer_ids)

    return request_ids, transfer_ids


def _create_mesh_upload_job(
        s3_locations: List[str], filename: str, local_id: str, mailbox_from: Optional[str] = None) -> str:
    mesh_metadata = MeshMetadata(dict(
        filename=filename,
        local_id=local_id,
        message_type='DATA',
        encrypted=False,
        compressed=False
    ))

    # Set to preparing as we don't want it to be processed until we have got the manifest ready
    transfer_id = MeshUploadJobs.create_mesh_upload_job(
        workflow_id=MESH_WORKFLOW_ID.MPS_REQUEST,
        s3_locations=s3_locations,
        mesh_metadata=mesh_metadata,
        mailbox_to=MESH_MAILBOXES.MPS_SYSTEM,
        mailbox_from=mailbox_from,
        status=MeshTransferStatus.Preparing,
        priority=MeshTransferJobPriority.High,
    )

    return transfer_id


@log_action(log_args=['storage_path', 'config'])
def generate_files(spark: SparkSession, df: DataFrame, storage_path: str, config: GeneratorConfig) -> GeneratedFileInfo:
    fields = config.fields
    fields[RequestHeaderFields.NO_OF_DATA_RECORDS] = df.count()

    reference = fields[config.reference_key]
    uuid = str(uuid4())

    body_file_path = os.path.join(storage_path, config.file_path, f"{reference}_{uuid}")
    df = df.repartition(1)  # so that only one CSV file is created
    df.write.csv(body_file_path)

    header_file_path = os.path.join(storage_path, config.file_path, f"{reference}_{uuid}.header")
    df = spark.createDataFrame([[str(v) for v in fields.values()]])
    df = df.repartition(1)  # so that only one CSV file is created
    df.write.csv(header_file_path)

    generated_field_info = GeneratedFileInfo(
        id=uuid,
        header_file_path=header_file_path,
        body_file_path=body_file_path,
        mesh_filename=reference
    )

    add_fields(generated_field_info=generated_field_info)

    return generated_field_info
