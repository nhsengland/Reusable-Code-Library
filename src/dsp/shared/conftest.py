# pylint: disable=redefined-outer-name

import logging
import os
import tempfile
from logging.handlers import RotatingFileHandler
from typing import Generator, Iterable, List, Tuple
from uuid import uuid4

import boto3
import petname
import pytest
from boto3.dynamodb.table import TableResource
from boto3.session import Session

from dsp.shared.aws import dynamodb, ddb_table, LOCAL_MODE, sqs, s3_bucket
from dsp.shared.common.test_helpers import LOCAL_TESTING_BUCKET
from dsp.shared.logger import app_logger, LogLevel, logging_context
from dsp.shared.logger.formatters import JSONFormatter
from dsp.shared.logger.handlers import capturing_log_handlers
from dsp.shared.models import GenericPipelineConfig
from dsp.shared.store import TasksStore, AgentsStore, ExtractRequestsStore, SubmissionsStore, SubmissionHashesStore, \
    MergeQueueStore, LastSubmissionsStore, LastSequenceNumbersStore
from dsp.shared.store.blocks import BlockStore
from dsp.shared.store.counters import CountersStore
from dsp.shared.databricks_users import DatabricksUsersStore
from dsp.shared.store.deid_jobs import DeIdJobStore
from dsp.shared.store.delta_merge import DeltaMergeStore
from dsp.shared.store.generic_pipelines_config import GenericPipelinesConfigStore
from dsp.shared.gp_data_submissions import GPDataSubmissionsStore
from dsp.shared.store.mesh_download_queues import MeshDownloadQueuesStore
from dsp.shared.store.mesh_upload_jobs import MeshUploadJobsStore
from dsp.shared.sftp_upload_jobs import SftpUploadJobsStore

__all__ = [
    'log_capture',
    'log_capture_global',
    's3_temp',
    'temp_dir',
    'temp_agents_table',
    'temp_agents',
    'temp_blocks_table',
    'temp_blocks',
    'temp_counters_table',
    'temp_counters',
    'temp_dae_requests_queue_name',
    'temp_dae_responses_queue_name',
    'temp_email_send_queue_name',
    'temp_databricks_users_store',
    'temp_databricks_users_table',
    'temp_deid_jobs_table',
    'temp_deid_jobs',
    'temp_delta_merge_table',
    'temp_delta_merges',
    'temp_dynamodb_table',
    'temp_extract_requests_table',
    'temp_extract_requests',
    'temp_gp_data_submissions',
    'temp_gp_data_submissions_table',
    'temp_last_submissions_table',
    'temp_last_submissions',
    'temp_last_sequence_numbers_table',
    'temp_last_sequence_numbers',
    'temp_merge_queues_table',
    'temp_merge_queues',
    'temp_mesh_download_queues_table',
    'temp_mesh_download_queues',
    'temp_mesh_upload_jobs_table',
    'temp_mesh_upload_jobs',
    'temp_mps_manifests_table',
    'temp_publish_queues_table',
    'temp_submissions_table',
    'temp_submissions',
    'temp_submission_hashes_table',
    'temp_submission_hashes',
    'temp_sftp_upload_jobs_table',
    'temp_sftp_upload_jobs',
    'temp_snapshots_table',
    'temp_sqs_queue',
    'temp_tasks_table',
    'temp_tasks',
    'temp_s3_submissions',
    'temp_s3_submissions_table',
    'temp_generic_pipelines_config_table',
    'temp_generic_pipelines_config',
    'temp_generic_pipeline_config',
    'temp_generic_pipeline_regex_mailbox_config',
]

TEST_LOG_FOLDER = os.path.expanduser('~/.dsp_dev_logs')


@pytest.fixture()
def temp_dir() -> Generator[str, None, None]:
    """ create a temporary folder and cleanup afterwards
    """
    with tempfile.TemporaryDirectory('_dsp', prefix='pytest_') as temp_dir_name:
        yield temp_dir_name


@pytest.fixture(scope='session', autouse=True)
def global_setup():
    os.environ.setdefault(LOCAL_MODE, 'True')
    os.environ.setdefault('env', 'local')
    initialise_logging()


@pytest.fixture(scope='function', autouse=True)
def reset_logging_storage():
    logging_context.thread_local_context_storage()


_file_handler = None


def initialise_logging():
    os.makedirs(TEST_LOG_FOLDER, exist_ok=True)

    _file_handler = RotatingFileHandler(
        filename=os.path.join(TEST_LOG_FOLDER, 'pytest.log'),
        maxBytes=30 * 1024 * 1024,
        backupCount=3,
        encoding='UTF-8'
    )
    _file_handler.setFormatter(JSONFormatter())

    app_logger.setup('pytest', handlers=[_file_handler], append=True)


def clone_schema(table):
    key_schema = table.key_schema

    attributes = table.attribute_definitions

    indexes = table.global_secondary_indexes

    if indexes:
        for index in indexes:
            del index['IndexStatus']
            del index['IndexSizeBytes']
            del index['ItemCount']
            del index['IndexArn']

    clone = {
        'KeySchema': key_schema,
        'AttributeDefinitions': attributes,
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 1000,
            'WriteCapacityUnits': 1000
        }
    }

    if indexes:

        for index in indexes:
            index['ProvisionedThroughput']['ReadCapacityUnits'] = 1000
            index['ProvisionedThroughput']['WriteCapacityUnits'] = 1000
        clone['GlobalSecondaryIndexes'] = indexes

    return clone


@pytest.fixture(scope='session')
def log_capture_global() -> Iterable[Tuple[List[dict], List[dict]]]:
    std_out = []
    std_err = []

    capturing_handlers = capturing_log_handlers(std_out, std_err)

    app_logger.setup('tests')

    for handler in capturing_handlers:
        logging.root.addHandler(handler)

    yield std_out, std_err

    for handler in capturing_handlers:
        logging.root.removeHandler(handler)


@pytest.fixture(scope='function')
def log_capture(log_capture_global) -> Iterable[Tuple[List[dict], List[dict]]]:
    std_out, std_err = log_capture_global

    std_out.clear()
    std_err.clear()

    log_at_level = app_logger.log_at_level

    app_logger.log_at_level = LogLevel.DEBUG

    yield std_out, std_err

    app_logger.log_at_level = log_at_level


@pytest.fixture()
def s3_temp() -> Generator[Tuple[object, str], None, None]:
    bucket = s3_bucket(LOCAL_TESTING_BUCKET)

    bucket.create()

    bucket.Acl().put(ACL='public-read')

    temp_folder = uuid4().hex

    yield bucket, 's3://{}/{}'.format(bucket.name, temp_folder)

    for obj in bucket.objects.filter(Prefix=temp_folder):
        obj.delete()
    for version in bucket.object_versions.filter(Prefix=temp_folder):
        version.delete()


def temp_dynamodb_table(source_table_name: str) -> Generator[TableResource, None, None]:
    """
    Create a table that copies the schema of <source_table> but uses a random name, can be used throughout
    a test and is deleted at the end.
    """
    ddb = dynamodb()

    table_name = petname.Generate(words=4, separator='_')

    source_table = ddb_table(source_table_name)

    cloned = clone_schema(source_table)

    table = ddb.create_table(TableName=table_name, **cloned)

    yield table

    table.delete()


@pytest.fixture(scope='function')
def temp_agents_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('agents')


@pytest.fixture(scope='function')
def temp_agents(temp_agents_table: Session.resource) \
        -> Generator[AgentsStore, None, None]:
    yield AgentsStore(temp_agents_table)


@pytest.fixture(scope='function')
def temp_blocks_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('blocks')


@pytest.fixture(scope='function')
def temp_blocks(temp_blocks_table: boto3.session.Session.resource) -> \
        Generator[BlockStore, None, None]:
    store = BlockStore(temp_blocks_table)
    yield store


@pytest.fixture(scope='function')
def temp_counters_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('counters')


@pytest.fixture(scope='function')
def temp_counters(temp_counters_table: Session.resource) \
        -> Generator[CountersStore, None, None]:
    yield CountersStore(temp_counters_table)


@pytest.fixture(scope='function')
def temp_delta_merge_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('delta_merge')


@pytest.fixture(scope='function')
def temp_delta_merges(temp_delta_merge_table: Session.resource) \
        -> Generator[DeltaMergeStore, None, None]:
    yield DeltaMergeStore(temp_delta_merge_table)


@pytest.fixture(scope='function')
def temp_deid_jobs_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('deid_jobs')


@pytest.fixture(scope='function')
def temp_deid_jobs(temp_deid_jobs_table: Session.resource,
                   temp_blocks: Session.resource) \
        -> Generator[DeIdJobStore, None, None]:
    yield DeIdJobStore(temp_deid_jobs_table, temp_blocks)


@pytest.fixture(scope='function')
def temp_extract_requests_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('extract_requests')


@pytest.fixture(scope='function')
def temp_extract_requests(temp_extract_requests_table: Session.resource) \
        -> Generator[ExtractRequestsStore, None, None]:
    yield ExtractRequestsStore(temp_extract_requests_table)


@pytest.fixture(scope='function')
def temp_gp_data_submissions_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('gp_data_submissions')


@pytest.fixture(scope='function')
def temp_gp_data_submissions(temp_gp_data_submissions_table: Session.resource) \
        -> Generator[GPDataSubmissionsStore, None, None]:
    yield GPDataSubmissionsStore(temp_gp_data_submissions_table)


@pytest.fixture(scope='function')
def temp_last_submissions_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('last_submissions')


@pytest.fixture(scope='function')
def temp_last_submissions(temp_last_submissions_table: Session.resource) \
        -> Generator[LastSubmissionsStore, None, None]:
    yield LastSubmissionsStore(temp_last_submissions_table)


@pytest.fixture(scope='function')
def temp_last_sequence_numbers_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('last_sequence_numbers')


@pytest.fixture(scope='function')
def temp_last_sequence_numbers(temp_last_sequence_numbers_table: Session.resource) \
        -> Generator[LastSequenceNumbersStore, None, None]:
    yield LastSequenceNumbersStore(temp_last_sequence_numbers_table)


@pytest.fixture(scope='function')
def temp_merge_queues_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('merge_queues')


@pytest.fixture(scope="function")
def temp_merge_queues(temp_merge_queues_table: Session.resource) \
        -> Generator[MergeQueueStore, None, None]:
    yield MergeQueueStore(temp_merge_queues_table)


@pytest.fixture(scope='function')
def temp_mesh_download_queues_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('mesh_download_queues')


@pytest.fixture(scope="function")
def temp_mesh_download_queues(temp_mesh_download_queues_table: Session.resource) \
        -> Generator[MeshDownloadQueuesStore, None, None]:
    yield MeshDownloadQueuesStore(temp_mesh_download_queues_table)


@pytest.fixture(scope='function')
def temp_mesh_upload_jobs_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('mesh_upload_jobs')


@pytest.fixture(scope='function')
def temp_mesh_upload_jobs(temp_mesh_upload_jobs_table: Session.resource) \
        -> Generator[MeshUploadJobsStore, None, None]:
    yield MeshUploadJobsStore(temp_mesh_upload_jobs_table)


@pytest.fixture(scope='function')
def temp_mps_manifests_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('mps_manifests')


@pytest.fixture(scope='function')
def temp_unsuspend_jobs_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('unsuspend_jobs')


@pytest.fixture(scope='function')
def temp_publish_queues_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('publish_queues')


@pytest.fixture(scope='function')
def temp_sftp_upload_jobs_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('sftp_upload_jobs')


@pytest.fixture(scope='function')
def temp_sftp_upload_jobs(temp_sftp_upload_jobs_table: Session.resource) \
        -> Generator[SftpUploadJobsStore, None, None]:
    yield SftpUploadJobsStore(temp_sftp_upload_jobs_table)


@pytest.fixture(scope='function')
def temp_snapshots_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('snapshots')


@pytest.fixture(scope='function')
def temp_submissions_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('submissions')


@pytest.fixture(scope='function')
def temp_submissions(temp_submissions_table: Session.resource) \
        -> Generator[SubmissionsStore, None, None]:
    yield SubmissionsStore(temp_submissions_table)


@pytest.fixture(scope='function')
def temp_submission_hashes_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('submission_hashes')


@pytest.fixture(scope='function')
def temp_submission_hashes(temp_submission_hashes_table: Session.resource) \
        -> Generator[SubmissionHashesStore, None, None]:
    yield SubmissionHashesStore(temp_submission_hashes_table)


@pytest.fixture(scope='function')
def temp_tasks_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('tasks')


@pytest.fixture(scope='function')
def temp_tasks(temp_tasks_table: boto3.session.Session.resource) -> Iterable[TasksStore]:
    store = TasksStore(temp_tasks_table)
    yield store


@pytest.fixture(scope='function')
def temp_tasks_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('tasks')


def temp_sqs_queue(source_queue_name: str) -> Generator[str, None, None]:
    """
    Create a queue that copies the schema of <source_queue> but uses a random name, can be used throughout
    a test and is deleted at the end.
    """
    sqs_client = sqs()

    queue_name = petname.Generate(separator='_')

    source_queue_url = sqs_client.get_queue_url(
        QueueName=source_queue_name
    )['QueueUrl']

    source_queue_attr = sqs_client.get_queue_attributes(
        QueueUrl=source_queue_url,
        AttributeNames=['VisibilityTimeout', 'MaximumMessageSize', 'MessageRetentionPeriod', 'DelaySeconds',
                        'ReceiveMessageWaitTimeSeconds', 'RedrivePolicy', 'FifoQueue', 'ContentBasedDeduplication',
                        'KmsMasterKeyId', 'KmsDataKeyReusePeriodSeconds']
    )['Attributes']

    queue_response = sqs_client.create_queue(
        QueueName=queue_name,
        Attributes=source_queue_attr
    )

    assert queue_response['ResponseMetadata']['HTTPStatusCode'] == 200

    yield queue_name

    temp_queue_url = sqs_client.get_queue_url(QueueName=queue_name)
    delete_response = sqs_client.delete_queue(
        QueueUrl=temp_queue_url.get('QueueUrl')
    )
    assert delete_response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.fixture(scope='function')
def temp_dae_requests_queue_name():
    yield from temp_sqs_queue('dae-requests')


@pytest.fixture(scope='function')
def temp_dae_responses_queue_name():
    yield from temp_sqs_queue('dae-responses')


@pytest.fixture(scope='function')
def temp_databricks_users_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('databricks_users')


@pytest.fixture(scope='function')
def temp_databricks_users_store(temp_databricks_users_table: Session.resource) \
        -> Generator[DatabricksUsersStore, None, None]:
    yield DatabricksUsersStore(temp_databricks_users_table)


@pytest.fixture(scope='function')
def temp_s3_submissions_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('s3_submissions')


@pytest.fixture(scope='function')
def temp_s3_submissions(temp_submissions_table: Session.resource) \
        -> Generator[SubmissionsStore, None, None]:
    yield SubmissionsStore(temp_s3_submissions_table)


@pytest.fixture(scope='function')
def temp_generic_pipelines_config_table() -> Generator[TableResource, None, None]:
    yield from temp_dynamodb_table('generic_pipelines_config')


@pytest.fixture(scope='function')
def temp_generic_pipelines_config(temp_generic_pipelines_config_table: boto3.session.Session.resource) -> \
        Generator[GenericPipelinesConfigStore, None, None]:
    store = GenericPipelinesConfigStore(temp_generic_pipelines_config_table)
    yield store


@pytest.fixture(scope='function')
def temp_generic_pipeline_config() -> GenericPipelineConfig:
    primitive = dict(
        pipeline_name='tp',
        workflow='tw',
        database='tdb',
        table='tt',
        file_type_config={'file_type': 'csv'},
        allowed_mailboxes=['tm1']
    )
    return GenericPipelineConfig(primitive)


@pytest.fixture(scope='function')
def temp_generic_pipeline_regex_mailbox_config() -> GenericPipelineConfig:
    primitive = dict(
        pipeline_name='tp',
        workflow='tw',
        database='tdb',
        table='tt',
        file_type_config={'file_type': 'csv'},
        allowed_mailboxes=['a.*', 'b.*', '.*cde.*', '.*x$']
    )
    return GenericPipelineConfig(primitive)


@pytest.fixture(scope='function')
def temp_email_send_queue_name():
    yield from temp_sqs_queue('email-send')
