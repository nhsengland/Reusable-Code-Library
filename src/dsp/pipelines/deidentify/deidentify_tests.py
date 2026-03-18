import os
import random
import secrets
from datetime import datetime
from typing import Any, Iterable, List
from uuid import uuid4

import boto3
import pytest
from mock import MagicMock
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType

from dsp.pipeline.deidentify import Deidentify, handle_outstanding_deid_jobs
from dsp.pipeline.deidentify.base_tests import create_mock_request
from shared.constants import PATHS
from shared.models import DeIdJob, DeIdJobSourceFormat, DeIdJobStatus, PipelineStatus, Submission
from shared.models_tests import valid_submission_metadata
from shared.store.blocks import BlockStore
from shared.store.counters import Counters
from shared.store.deid_jobs import DeIdJobs
from shared.store.submissions import SubmissionsStore

SCHEMA = StructType(
    [
        StructField('id', StringType()),
        StructField('domain', StringType()),
        StructField('pseudo_type', StringType()),
        StructField('pseudo', StringType()),
        StructField('clear', StringType()),
        StructField('created', DateType()),
        StructField('job_id', StringType())
    ]
)


def _create_df(spark: SparkSession, db_name: str, rows: List[Iterable[Any]]) -> None:
    df = spark.createDataFrame(rows, SCHEMA)
    df.write.saveAsTable(f'{db_name}.pseudo')


@pytest.fixture(scope='function')
def pseudo_db(spark: SparkSession) -> str:
    db_name = 'pseudo' + uuid4().hex

    spark.sql(f'CREATE DATABASE IF NOT EXISTS {db_name}')

    return db_name


@pytest.fixture(scope='function')
def multiple_domains_pseudo_table(spark, pseudo_db) -> str:
    rows = [
        ('1', 'Domain-0', 'Person_ID', 'LD2ND9ZN3C2DY2Q', '10000', datetime.utcnow(), '1'),
        ('1', 'Domain-1', 'Person_ID', 'LD2ND9ZN3C2DY2Q', '10000', datetime.utcnow(), '1'),
        ('1', 'PDD-1', 'Person_ID', 'LD2ND9ZN3C2DY2Q', '10000', datetime.utcnow(), '1')
    ]
    _create_df(spark, pseudo_db, rows)
    return pseudo_db


@pytest.fixture(scope='function')
def simple_domains_pseudo_table(spark, pseudo_db) -> str:
    rows = [('1', 'Domain-0', 'Person_ID', 'LD2ND9ZN3C2DY2Q', '10000', datetime.utcnow(), '1')]
    _create_df(spark, pseudo_db, rows)
    return pseudo_db


@pytest.fixture(scope='function')
def pseudo_table(spark, pseudo_db) -> str:
    rows = [
        ('1', 'Domain-0', 'Person_ID', 'LD2ND9ZN3C2DY2Q', '10000', datetime.utcnow(), '1'),
        ('1', 'Domain-1', 'Person_ID', 'LD2ND9ZN3C2DY2Q', '10000', datetime.utcnow(), '1'),
        ('1', 'Domain-0', 'Person_ID', 'LD2ND9ZN3C2DY3Q', '10001', datetime.utcnow(), '1'),
    ]
    _create_df(spark, pseudo_db, rows)
    return pseudo_db


@pytest.fixture(scope='function')
def long_pseudo_table(spark, pseudo_db):
    rows = [
        (
            str(counter),
            'Domain-0',
            'Person_ID',
            secrets.token_hex(12).upper(),
            str(counter).rjust(12, "0"),
            datetime.utcnow(),
            "1",
        )
        for counter in range(50000)
    ]
    _create_df(spark, pseudo_db, rows)
    return pseudo_db


@pytest.fixture(scope='function')
def test_deid_job() -> Deidentify:
    test_deidjob = Deidentify('test.privacy.data.digital.nhs.uk')
    test_deidjob.p12_secret = 'secret'

    return test_deidjob


mocked_requests_get = create_mock_request(
    {
        "policies": [{"name": Deidentify.pod_policy_name, "id": "policy123"}],
        "dataflow": [{"name": Deidentify.data_flow_job_name, "id": "abc123"}],
    }
)


def test_create_pseudo_queue(spark: SparkSession, temp_dir, test_deid_job, simple_domains_pseudo_table):
    db_name = simple_domains_pseudo_table

    DeIdJobs.truncate_table()

    # Create simple deid cohort and save to temp file
    df_deid_cohort = spark.createDataFrame(
        [
            (1, "Person_ID", "10000"), (1, "Person_ID", "10001"),
            (1, "", "10003"), (1, "Person_ID", ""),
            (1, None, "10004"), (1, "Person_ID", None)
        ], ["correlation_id", "pseudo_type", "clear"]
    )

    create_test_deid_job(df_deid_cohort, temp_dir, "temp1")

    df_deid_cohort = spark.createDataFrame(
        [(1, "Person_ID", "10001"), (2, "Person_ID", "10002")], ["correlation_id", "pseudo_type", "clear"]
    )

    job = create_test_deid_job(df_deid_cohort, temp_dir, "temp2")

    jobs_status_view = uuid4().hex
    pseudo_batch_view = uuid4().hex

    # Call create_pseudo_queue
    processing_jobs = test_deid_job.create_pseudo_queue(
        spark,
        max_jobs_in_batch=100,
        db_name=db_name,
        run_id=42,
        jobs_status_view=jobs_status_view,
        pseudo_batch_view=pseudo_batch_view,
    )

    # Assert that returned queue is correct and processing jobs is correct (and that there are no other jobs in any
    # unexpected states

    assert DeIdJobs.get_by_id(job).runs[-1].run_id == 42
    assert processing_jobs

    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Ready))) == 0
    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Failed))) == 0
    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Success))) == 0

    df_queue = spark.table(pseudo_batch_view)

    queue_row = Row('clear', 'pseudo_type', 'domain', 'job_id')

    assert df_queue.select('clear', 'pseudo_type', 'domain', 'job_id').collect() == [
        queue_row('10001', 'Person_ID', 'Domain-0', str(processing_jobs[0]['job_id'])),
        queue_row('10002', 'Person_ID', 'Domain-0', str(processing_jobs[1]['job_id']))
    ]

    df_jobs = spark.table(jobs_status_view)

    job_row = Row("complete_after_retokenisation", "domain", "job_id", "resume_deid_submission", "source", "status",
                  "submission_id")

    assert df_jobs.collect() == [
        job_row(
            False,
            "Domain-0",
            processing_jobs[0]["job_id"],
            False,
            processing_jobs[0]["source"],
            "processing",
            processing_jobs[0]["submission_id"],
        ),
        job_row(
            False,
            "Domain-0",
            processing_jobs[1]["job_id"],
            False,
            processing_jobs[1]["source"],
            "processing",
            processing_jobs[1]["submission_id"],
        ),
    ]


def test_create_null_pseudo_queue(spark: SparkSession, temp_dir, test_deid_job, simple_domains_pseudo_table):
    db_name = simple_domains_pseudo_table

    DeIdJobs.truncate_table()

    # Create simple deid cohort and save to temp file
    df_deid_cohort = spark.createDataFrame([(1, "Person_ID", "10000")], ["correlation_id", "pseudo_type", "clear"])

    job_id = create_test_deid_job(df_deid_cohort, temp_dir, "temp1")

    job_status_view = uuid4().hex
    pseudo_batch_view = uuid4().hex

    # Call create_pseudo_queue
    processing_jobs = test_deid_job.create_pseudo_queue(
        spark, 100, run_id=1, db_name=db_name, jobs_status_view=job_status_view, pseudo_batch_view=pseudo_batch_view
    )

    # Assert that returned queue is correct and processing jobs is correct (and that there are no other jobs in any
    # unexpected states

    assert not processing_jobs

    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Processing))) == 0
    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Ready))) == 0
    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Failed))) == 0
    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Success))) == 1

    tables = spark.catalog.listTables()

    assert not any(pseudo_batch_view in name for name in tables)
    assert not any(job_status_view in name for name in tables)

    job = DeIdJobs.get_by_id(job_id)

    assert job.status == DeIdJobStatus.Success


def create_test_deid_job(df_deid_cohort, temp_dir, file):
    job_id = Counters.get_next_deid_job_id()

    raw_file_path = os.path.join(temp_dir, PATHS.DEID)
    parquet_full_path = os.path.join(raw_file_path, file)
    df_deid_cohort.write.parquet(parquet_full_path)

    # Create job for file
    deid_job = DeIdJob(
        dict(
            id=job_id,
            submission_id=0,
            status=DeIdJobStatus.Ready,
            created=datetime.utcnow(),
            domain="Domain-0",
            source=parquet_full_path,
            source_format=DeIdJobSourceFormat.Parquet,
        )
    )
    DeIdJobs.put(deid_job)

    return job_id


def test_get_dataflow_job(requests_pkcs12_get_mock, test_deid_job):
    requests_pkcs12_get_mock.side_effect = mocked_requests_get

    dataflow_job = test_deid_job.get_dataflow_job()

    assert dataflow_job == 'abc123'


def test_get_domain_zero_jobs(requests_pkcs12_get_mock, test_deid_job):
    requests_pkcs12_get_mock.side_effect = mocked_requests_get

    domain_zero_jobs = test_deid_job.get_config_jobs()

    assert domain_zero_jobs['transitJobId'] == 'abc123'
    assert domain_zero_jobs['tokenisationJobId'] == 'abc000'


def create_deid_job(domain, status, source):
    deid_job = DeIdJob(
        dict(
            id=Counters.get_next_deid_job_id(),
            submission_id=0,
            status=status,
            created=datetime.utcnow(),
            domain=domain,
            source=source,
            source_format=DeIdJobSourceFormat.Parquet,
        )
    )

    DeIdJobs.put(deid_job)


def create_deid_jobs():
    DeIdJobs.truncate_table()

    create_deid_job('Domain-0', DeIdJobStatus.Success, '/tmp/1')
    create_deid_job('Domain-0', DeIdJobStatus.Success, '/tmp/2')
    create_deid_job('Domain-0', DeIdJobStatus.Processing, '/tmp/3')
    create_deid_job('Domain-1', DeIdJobStatus.Ready, '/tmp/1')
    create_deid_job('Domain-1', DeIdJobStatus.Success, '/tmp/10')
    create_deid_job('Domain-1', DeIdJobStatus.Failed, '/tmp/11')
    create_deid_job('PDD-1', DeIdJobStatus.Success, '/tmp/1')


@pytest.mark.parametrize(
    ['process_new_domains', "expected_pseudo_domains_view"],
    [
        (False, [('ddd111', 'Domain-1')]),  # retokenise currently populated domains
        (True, [('ppp123', 'PDD-1')]),  # retokenise only new domains
    ],
)
def test_create_retokenisation_queue(
        requests_pkcs12_get_mock,
        spark: SparkSession,
        temp_dir,
        pseudo_table,
        test_deid_job: Deidentify,
        process_new_domains: bool,
        expected_pseudo_domains_view: List[Any],
        pseudo_db: str,
):
    requests_pkcs12_get_mock.side_effect = mocked_requests_get
    DeIdJobs.truncate_table()

    pseudo_domains_view = uuid4().hex

    # Call create_pseudo_queue
    is_domain_one_plus_jobs = test_deid_job.create_retokenisation_queue(
        spark, pseudo_table, pseudo_domains_view, process_new_domains=process_new_domains
    )

    # Assert that returned queue is correct and processing jobs is correct (and that there are no other jobs in any
    # unexpected states

    assert is_domain_one_plus_jobs

    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Ready))) == 0
    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Processing))) == 0
    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Failed))) == 0
    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Success))) == 0

    df_domains = spark.table(pseudo_domains_view)

    domain_row = Row('id', 'name')

    assert df_domains.count() == len(expected_pseudo_domains_view)
    assert not set(df_domains.collect()).symmetric_difference([domain_row(*r) for r in expected_pseudo_domains_view])


def test_no_op_retokenisation_queue(
        requests_pkcs12_get_mock, spark: SparkSession, temp_dir, test_deid_job, multiple_domains_pseudo_table
):
    requests_pkcs12_get_mock.side_effect = mocked_requests_get
    DeIdJobs.truncate_table()

    db_name = multiple_domains_pseudo_table

    pseudo_domains_view = uuid4().hex

    # Call create_pseudo_queue
    is_domain_one_plus_jobs = test_deid_job.create_retokenisation_queue(
        spark, db_name, pseudo_domains_view, process_new_domains=True
    )

    # Assert that returned queue is correct and processing jobs is correct (and that there are no other jobs in any
    # unexpected states

    assert not is_domain_one_plus_jobs

    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Ready))) == 0
    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Processing))) == 0
    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Failed))) == 0
    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Success))) == 0

    tables = spark.catalog.listTables()

    assert not any(pseudo_domains_view in name for name in tables)


@pytest.mark.skip(reason='only run for performance testing')
def test_create_large_pseudo_batch(spark: SparkSession, temp_dir, test_deid_job, long_pseudo_table):
    db_name = long_pseudo_table

    DeIdJobs.truncate_table()

    job_counter = 0

    while job_counter < 100:
        job_counter = job_counter + 1

        counter = 0
        rows = []

        while counter < 1000:
            counter = counter + 1
            rows.append((str(uuid4()), 'Person_ID', str(random.randint(1, 100000)).rjust(12, '0')))

        df_deid_cohort = spark.createDataFrame(rows, ['correlation_id', 'pseudo_type', 'clear'])

        create_test_deid_job(df_deid_cohort, temp_dir, 'temp{}'.format(job_counter))

    # Call create_pseudo_queue
    processing_jobs = test_deid_job.create_pseudo_queue(
        spark, max_jobs_in_batch=100, db_name=db_name, run_id=random.randint(1, 100000)
    )

    assert processing_jobs


@pytest.fixture(scope="function")
def temp_submissions(
        temp_submissions_table: boto3.session.Session.resource, temp_blocks: BlockStore
) -> Iterable[SubmissionsStore]:
    store = SubmissionsStore(temp_submissions_table, temp_blocks)
    yield store


def test_resume_deid_submission_with_valid_submission_id(test_deid_job, temp_submissions):
    submission_store = temp_submissions
    job_id = Counters.get_next_deid_job_id()
    submission_id = Counters.get_next_submission_id()

    submission = Submission(
        dict(
            id=submission_id,
            sender_id="0123456789",
            agent_id=1,
            dataset_id="deid",
            status=PipelineStatus.Suspended,
            received=datetime.utcnow(),
            submitted=datetime.utcnow(),
            metadata=valid_submission_metadata(),
        )
    )
    submission_store.put(submission)

    # Create job for file
    deid_job = DeIdJob(
        dict(
            id=job_id,
            submission_id=submission_id,
            status=DeIdJobStatus.Success,
            created=datetime.utcnow(),
            domain="Domain-0",
            source="s3://local-testing/test",
            source_format=DeIdJobSourceFormat.Parquet,
        )
    )
    DeIdJobs.put(deid_job)

    resumed_submission_id = test_deid_job.resume_deid_submission(deid_job=deid_job, submission_store=submission_store)

    assert resumed_submission_id == submission_id
    new_submission = submission_store.get_by_id(submission_id=submission_id)
    assert new_submission.status == PipelineStatus.Pending


def test_resume_deid_submission_with_invalid_submission_id(test_deid_job, temp_submissions):
    submission_store = temp_submissions
    job_id = Counters.get_next_deid_job_id()

    # Create job for file
    deid_job = DeIdJob(
        dict(
            id=job_id,
            submission_id=0,
            status=DeIdJobStatus.Success,
            created=datetime.utcnow(),
            domain="Domain-0",
            source="s3://local-testing/test",
            source_format=DeIdJobSourceFormat.Parquet,
        )
    )
    DeIdJobs.put(deid_job)

    resumed_submission_id = test_deid_job.resume_deid_submission(deid_job=deid_job, submission_store=submission_store)

    assert resumed_submission_id is None


def test_handle_outstanding_deid_jobs(mocker):
    job_to_mark_as_success_id = Counters.get_next_deid_job_id()
    job_already_marked_success_id = Counters.get_next_deid_job_id()
    processing_jobs = []

    deid_job_to_mark_as_success = DeIdJob(
        dict(
            id=job_to_mark_as_success_id,
            submission_id=0,
            status=DeIdJobStatus.Processing,
            created=datetime.utcnow(),
            domain="Domain-0",
            source="s3://local-testing/test",
            source_format=DeIdJobSourceFormat.Parquet,
            complete_after_retokenisation=True,
            resume_deid_submission=True
        ))
    deid_job_already_marked_success = DeIdJob(
        dict(
            id=job_already_marked_success_id,
            submission_id=1,
            status=DeIdJobStatus.Success,
            created=datetime.utcnow(),
            domain="Domain-0",
            source="s3://local-testing/test",
            source_format=DeIdJobSourceFormat.Parquet,
            complete_after_retokenisation=False,
            resume_deid_submission=False
        ))

    processing_job_to_mark_as_success = Row(
        job_id=job_to_mark_as_success_id,
        domain="Domain-0",
        status=DeIdJobStatus.Processing,
        submission_id=0,
        source="s3://local-testing/test",
        resume_deid_submission=True,
        complete_after_retokenisation=True
    )
    processing_job_already_marked_success = Row(
        job_id=job_already_marked_success_id,
        domain="Domain-0",
        status=DeIdJobStatus.Success,
        submission_id=1,
        source="s3://local-testing/test",
        resume_deid_submission=False,
        complete_after_retokenisation=False
    )
    processing_jobs.append(processing_job_to_mark_as_success)
    processing_jobs.append(processing_job_already_marked_success)

    DeIdJobs.put(deid_job_to_mark_as_success)
    DeIdJobs.put(deid_job_already_marked_success)

    mocker.patch('src.dsp.pipeline.deidentify.deidentify.SubmissionsStore')

    mock_deidentify = MagicMock()
    handle_outstanding_deid_jobs(processing_jobs, mock_deidentify)

    mock_deidentify.resume_deid_submission.assert_called_once()
    assert DeIdJobs.get_by_id(job_to_mark_as_success_id).status == DeIdJobStatus.Success
    assert DeIdJobs.get_by_id(job_already_marked_success_id).status == DeIdJobStatus.Success


def test_handle_outstanding_deid_jobs_raises():
    processing_jobs = []

    invalid_status_job_id = Counters.get_next_deid_job_id()

    processing_job_invalid_status = Row(
        job_id=invalid_status_job_id,
        domain="NIC-1234",
        status=DeIdJobStatus.Pending,
        submission_id=0,
        source="s3://local-testing/test",
        resume_deid_submission=True,
        complete_after_retokenisation=True
    )

    deid_job_invalid_status = DeIdJob(
        dict(
            id=invalid_status_job_id,
            submission_id=0,
            status=DeIdJobStatus.Pending,
            created=datetime.utcnow(),
            domain="NIC-1234",
            source="s3://local-testing/test",
            source_format=DeIdJobSourceFormat.Parquet,
            complete_after_retokenisation=True,
            resume_deid_submission=True
        ))

    DeIdJobs.put(deid_job_invalid_status)
    processing_jobs.append(processing_job_invalid_status)

    mock_deidentify = MagicMock()

    with pytest.raises(Exception):
        handle_outstanding_deid_jobs(processing_jobs, mock_deidentify)

    assert len(list(DeIdJobs.get_all_by_status(DeIdJobStatus.Failed))) == 1
