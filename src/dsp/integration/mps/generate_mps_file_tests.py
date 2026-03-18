import copy
import csv
import json
import os
import re
from typing import Tuple, List
from uuid import uuid4

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import Row
from pytest import raises

from dsp.datasets.fields.mps.request import Order as MPSRequestOrder
from dsp.datasets.fields.mps.request_header import Fields as RequestFields
from dsp.integration.mps.cross_check import schema_fill_values
from dsp.integration.mps.generate_mps_file import generate_files, create_mps_manifest, generate_request_files, \
    strip_invalid_postcodes, generate_mps_files, GeneratorConfig
from dsp.integration.mps.mps_schema import mps_schema, generate_mps_request_header_fields
# noinspection PyUnresolvedReferences
from dsp.model.pds_record import PDSRecordPaths, PDSRecord
# noinspection PyUnresolvedReferences
from dsp.model.pds_record_tests import full_pds_record_data
from shared.aws import s3_ls
from shared.common.test_helpers import smart_open
from shared.constants import PATHS, MESH_WORKFLOW_ID, MPS_LOCAL_ID_PREFIXES
from shared.models import MeshTransferStatus
from shared.store.base import ModelNotFound
from shared.store.mesh_upload_jobs import MeshUploadJobs
from shared.store.mps_manifests import MpsManifests

UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


@pytest.fixture()
def pds_records(full_pds_record_data) -> List[PDSRecord]:
    record_1 = copy.deepcopy(full_pds_record_data)
    record_1[PDSRecordPaths.NHS_NUMBER] = '9876389375'
    record_1[PDSRecordPaths.SERIAL_CHANGE_NUMBER] = 2
    record_1[PDSRecordPaths.DOB] = 20081015
    record_1[PDSRecordPaths.EMAIL_ADDRESS] = 'person1@example.com'

    record_2 = copy.deepcopy(full_pds_record_data)
    record_2[PDSRecordPaths.NHS_NUMBER] = '9876389376'
    record_2[PDSRecordPaths.SERIAL_CHANGE_NUMBER] = 2
    record_2[PDSRecordPaths.DOB] = 20081015
    record_2[PDSRecordPaths.EMAIL_ADDRESS] = 'person2@example.com'

    return [PDSRecord.from_json(json.dumps(record_1)), PDSRecord.from_json(json.dumps(record_2))]


@pytest.fixture(scope='function')
def unmatched_cross_check_output(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([
        schema_fill_values(mps_schema, MATCH=False, UNIQUE_REFERENCE='UR1', NHS_NO='9876389373'),
        schema_fill_values(mps_schema, MATCH=False, UNIQUE_REFERENCE='UR2', NHS_NO='9876389374'),
        schema_fill_values(mps_schema, MATCH=False, UNIQUE_REFERENCE='UR5', NHS_NO='9876389377'),
        schema_fill_values(mps_schema, MATCH=False, UNIQUE_REFERENCE='UR26', NHS_NO='9876389378')
    ], mps_schema).select([col(source) for source in MPSRequestOrder])


@pytest.fixture(scope='function')
def create_path(temp_dir: str) -> Tuple[str, str, str, str]:
    submission_id = 'submission-{}'.format(uuid4())
    submission_path = f"s3://local-testing/submissions/{submission_id}"
    raw_path = f"{submission_path}/{PATHS.RAW_DATA}"
    storage_path = f"{submission_path}/mps/"

    return submission_path, raw_path, storage_path, submission_id


def test_null_invalid_postcodes(spark: SparkSession):
    # Arrange
    header = Row("POSTCODE")

    invalid_postcodes = [
        "L,52FT", "LS!2FT", "LS52-T", "LS52F#", "LS52]T", "LS[2FT", "LS5'FT", "~S52FT", "LS5>FT", "LS>%FT", "LS£@$T",
        "LS52FTT", "LS52FTUUUUU", "LS5|/T", "LS52FT%^", "L,5 2FT", "LS! 2FT", "LS5 2-T", "LS5 2F#", "LS52]T", "LS[2FT",
        "LS5 'FT", "~S5 2FT", "LS5 >FT", "LS> %FT", "LS£ @$T", "LS5 2FTT", "LS52FTUU UUU", "LS5|/T", "LS5 2FT%^",
        "LS52FT T"
    ]
    valid_postcodes = ["LS52fT", "LS5 2FT", "ls1 2Ft", "m146GB", "M146GB", "M14 6GB", "M14  6GB"]

    # Check invalid
    input_invalid_df = spark.createDataFrame([header(postcode) for postcode in invalid_postcodes])
    output_invalid_df = strip_invalid_postcodes(input_invalid_df)
    invalid_intersect_df = input_invalid_df.intersect(output_invalid_df)
    assert invalid_intersect_df.count() == 0, \
        f'unexpected output for invalid postcodes: {output_invalid_df.collect()}'

    # Check valid
    input_valid_df = spark.createDataFrame([header(postcode) for postcode in valid_postcodes])
    output_valid_df = strip_invalid_postcodes(input_valid_df)
    valid_intersect_df = input_valid_df.intersect(output_valid_df)
    assert valid_intersect_df.count() == input_valid_df.count(), \
        f'unexpected output for valid postcodes: {output_valid_df.collect()}'


def test_null_invalid_postcodes_if_postcode_column_missing(spark: SparkSession):
    # Arrange
    header = Row("NotAPostcode")
    test_postcodes = [
        header("Jerry"),
        header("John"),
        header("Jamie"),
        header("Jane"),
        header("Jeremy"),
        header("Jenny"),
    ]
    input_df = spark.createDataFrame(test_postcodes)

    # Act
    output_df = strip_invalid_postcodes(input_df)

    # Assert
    assert output_df == input_df


def test_generate_request_file(spark: SparkSession, unmatched_cross_check_output,
                               create_path: Tuple[str, str, str, str]):
    submission_path, raw_path, storage_path, submission_id = create_path

    unmatched = unmatched_cross_check_output

    request_config = GeneratorConfig(fields=generate_mps_request_header_fields(),
                                     reference_key=RequestFields.REQUEST_REFERENCE,
                                     file_path='request/')

    file_info = generate_files(spark, unmatched, storage_path, request_config)

    # Check the contents of response file

    body_file_path = f"{submission_path}/mps/request/{file_info.mesh_filename}_{UUID_PATTERN}"

    assert re.fullmatch(body_file_path, file_info.body_file_path)

    df_request_file = spark.read.csv(file_info.body_file_path)
    assert len(df_request_file.collect()) == 4
    assert df_request_file.filter("_c0 = 'UR1'").collect()[0]._c1 == '9876389373'
    assert df_request_file.filter("_c0 = 'UR2'").collect()[0]._c1 == '9876389374'

    # Check header response reference matches filename
    header_file_path = f"{submission_path}/mps/request/{file_info.mesh_filename}_{UUID_PATTERN}.header"
    assert re.fullmatch(header_file_path, file_info.header_file_path)
    df_request_header = spark.read.csv(os.path.join(file_info.header_file_path))
    assert "{}".format(df_request_header.collect()[0][0]) in file_info.header_file_path


def test_generate_request_files(
        spark: SparkSession,
        unmatched_cross_check_output,
        create_path: Tuple[str, str, str, str]
):
    chunk_size_rows = 3
    processing_count = 0

    unmatched = unmatched_cross_check_output
    _, _, storage_path, submission_id = create_path

    request_ids, transfer_ids = generate_request_files(
        spark, unmatched, storage_path, submission_id, chunk_size_rows,
    )

    for job in MeshUploadJobs.get_all_by_status(MeshTransferStatus.Preparing, MESH_WORKFLOW_ID.MPS_REQUEST):
        if job.mesh_metadata.local_id.split("_")[1] in request_ids:
            processing_count += 1

    assert processing_count == 2
    assert len(request_ids) == 2


def test_generate_request_files_adhoc(
        spark: SparkSession,
        unmatched_cross_check_output: DataFrame,
        create_path: Tuple[str, str, str, str]
):
    chunk_size_rows = 3
    processing_count = 0

    unmatched = unmatched_cross_check_output
    submission_path, raw_path, storage_path, submission_id = create_path

    request_ids, transfer_ids = generate_request_files(
        spark, unmatched, storage_path, submission_id, chunk_size_rows, local_id_prefix=MPS_LOCAL_ID_PREFIXES.ADHOC
    )

    for job in MeshUploadJobs.get_all_by_status(MeshTransferStatus.Preparing, MESH_WORKFLOW_ID.MPS_REQUEST):
        local_id_parts = job.mesh_metadata.local_id.split("_")

        if len(local_id_parts) == 3 and local_id_parts[2] in request_ids:
            assert local_id_parts[0] == 'ADHOC'
            assert local_id_parts[1] == submission_id
            processing_count = processing_count + 1

    assert processing_count == 2
    assert len(request_ids) == 2


def test_process_mps_files_with_requests():
    submission_id = 'submission-{}'.format(uuid4())
    request_id_1 = str(uuid4())
    request_id_2 = str(uuid4())
    response_id = str(uuid4())
    request_ids = [request_id_1, request_id_2]

    create_mps_manifest(submission_id, request_ids, response_id)

    mps_manifest = MpsManifests.get_by_submission_id(submission_id)
    assert mps_manifest.submission_id == submission_id
    assert mps_manifest.all_chunks == request_ids + [response_id]
    assert mps_manifest.remaining_chunks == request_ids


def test_process_mps_files_without_requests():
    submission_id = 'submission-{}'.format(uuid4())
    response_id = str(uuid4())
    request_ids = []

    with pytest.raises(Exception) as exc_inf:
        manifest_created = create_mps_manifest(submission_id, request_ids, response_id)
        assert not manifest_created

    assert "Request chunks should not be empty" in (exc_inf.value.args[0])


def test_generate_mps_files_with_unmatched_data(
        spark: SparkSession,
        create_path: Tuple[str, str, str, str]
):
    chunk_size_rows = 10
    unmatched_df = spark.createDataFrame([
        schema_fill_values(
            mps_schema,
            MATCH=False,
            UNIQUE_REFERENCE='U:R:1',
            NHS_NO='9876389373',
            DATE_OF_BIRTH=20030415,
            EMAIL_ADDRESS='bob@example.com'
        )
    ], mps_schema)
    unmatched_df = unmatched_df.select([col(source) for source in MPSRequestOrder])
    _, _, storage_path, submission_id = create_path
    generate_mps_files(spark, storage_path, submission_id, unmatched_df, chunk_size_rows)

    request_path = os.path.join(storage_path, 'request')

    header_files = list(
        s3_ls(request_path, predicate=lambda key: '.header/part-' in key and key.endswith('.csv'))
    )
    assert len(header_files) == 1

    with smart_open(f's3://{header_files[0].bucket_name}/{header_files[0].key}', mode='r') as csvfile:
        header_reader = csv.reader(csvfile)
        rows = list(row for row in header_reader)
        assert len(rows) == 1
        row = rows[0]
        assert row[0].startswith('MPTREQ_')
        assert row[0][7:].isdigit()  # datetime
        assert len(row[0]) == 21
        assert row[1] == 'SPINE_MPTPHASE4_TRACE'
        assert len(row[2]) == 14
        assert row[2].isdigit()
        assert row[3] == '1'

    body_files = list(
        s3_ls(request_path, predicate=lambda key: not '.header/' in key and key.endswith('.csv'))
    )
    assert len(body_files) == 1

    with smart_open(f's3://{body_files[0].bucket_name}/{body_files[0].key}', mode='r') as csvfile:
        body_reader = csv.reader(csvfile)
        rows = list(body_reader)
        assert len(rows) == 1
        row = rows[0]

        assert len(row) == 23
        assert row[0] == 'U-R-1'
        assert row[1] == '9876389373'
        assert row[6] == '20030415'
        assert row[22] == 'bob@example.com'

    assert MpsManifests.get_by_submission_id(submission_id) is not None


def test_multiple_chunk_for_performance(
        spark: SparkSession,
        create_path: Tuple[str, str, str, str]
):
    total_records = 67

    chunk_size_rows = 17
    unmatched_df = spark.createDataFrame([
        schema_fill_values(mps_schema, MATCH=False, UNIQUE_REFERENCE=f'UR{i}', NHS_NO='9876389373') for i in
        range(total_records)
    ], mps_schema).select([col(source) for source in MPSRequestOrder])

    unmatched_df = unmatched_df.select([col(source) for source in MPSRequestOrder])
    _, _, storage_path, submission_id = create_path
    generate_mps_files(spark, storage_path, submission_id, unmatched_df, chunk_size_rows)
    request_path = os.path.join(storage_path, 'request')

    header_files = list(
        s3_ls(request_path, predicate=lambda key: '.header/part-' in key and key.endswith('.csv'))
    )
    assert len(header_files) == 4  # 4 chunks

    body_successes = list(
        s3_ls(request_path, predicate=lambda key: '.header' not in key and key.endswith('_SUCCESS'))
    )
    assert len(body_successes) == 4

    refs = []

    body_files = list(
        s3_ls(request_path, predicate=lambda key: '.header' not in key and key.endswith('.csv'))
    )
    assert len(body_files) == 4

    for body_file in body_files:

        with smart_open(f's3://{body_file.bucket_name}/{body_file.key}', mode='r') as csvfile:
            body_reader = csv.reader(csvfile)

            file_refs = []
            for row in body_reader:
                assert len(row) == 23

                # Store the uniq reference
                file_refs.append(row[0])

            # Assert that there are no duplicates
            assert len(set(file_refs)) == len(file_refs)

            # Assert that the sorting is working for repeatable outputs
            assert sorted(file_refs) == file_refs

            refs.extend(file_refs)

    # Check the number of references = the number of records expected
    assert len(refs) == total_records
    # Check the distinct number of references = the number of records expected
    assert len(set(refs)) == total_records

    assert MpsManifests.get_by_submission_id(submission_id) is not None


def test_generate_mps_files_with_all_matched_data(
        spark: SparkSession,
        create_path: Tuple[str, str, str, str]
):
    chunk_size_rows = 3
    unmatched_df = spark.createDataFrame([], mps_schema)
    _, _, storage_path, submission_id = create_path

    request_path = os.path.join(storage_path, 'request')

    generate_mps_files(spark, storage_path, submission_id, unmatched_df, chunk_size_rows)

    request_files = list(
        s3_ls(request_path)
    )
    assert len(request_files) == 0

    with raises(ModelNotFound):
        MpsManifests.get_by_submission_id(submission_id)
