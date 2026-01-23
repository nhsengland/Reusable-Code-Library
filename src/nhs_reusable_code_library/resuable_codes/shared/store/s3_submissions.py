import datetime
from datetime import timedelta
from typing import AbstractSet, Generator, List, Mapping, Union

from boto3.dynamodb.conditions import Attr, Key

from src.nhs_reusable_code_library.resuable_codes.shared.aws import dynamodb_retry_backoff, ddb_query_paginated_count
from src.nhs_reusable_code_library.resuable_codes.shared.common import run_in_executor
from src.nhs_reusable_code_library.resuable_codes.shared.constants import JOBS
from src.nhs_reusable_code_library.resuable_codes.shared.logger import log_action
from src.nhs_reusable_code_library.resuable_codes.shared.models import PipelineResult, PipelineStatus, S3Submission
from src.nhs_reusable_code_library.resuable_codes.shared.store.base import BaseStore
from src.nhs_reusable_code_library.resuable_codes.shared.store.blocks import Blocks, BlockStore
from src.nhs_reusable_code_library.resuable_codes.shared.store.counters import Counters


class Indexes:
    Status = "ix_s3_submissions_by_status"
    DatasetId = "ix_s3_submissions_by_dataset_id"
    DatasetIdStatus = "ix_s3_submissions_by_dataset_id_status"


class S3SubmissionsStore(BaseStore):
    _table_name = 's3_submissions'
    _blocks_store = None
    _keys = ['id']

    def __init__(self, table=None, blocks_store: BlockStore = None):
        super().__init__(S3Submission, table)
        self._blocks_store = blocks_store or Blocks

    def get_by_key(self, s3_submission_id: int, consistent_read: bool = False) -> S3Submission:
        return self.get(key=dict(id=s3_submission_id), consistent_read=consistent_read)

    def get_by_id(self, s3_submission_id: int, consistent_read: bool = False):
        return self.get_by_key(s3_submission_id, consistent_read=consistent_read)

    async def async_get_by_id(self, s3_submission_id: int, consistent_read: bool = False):
        return await run_in_executor(self.get_by_key, s3_submission_id, consistent_read=consistent_read)

    class SchemaMigrations:

        def from_0_to_1(self, item):
            pass

    @log_action()
    @dynamodb_retry_backoff()
    def get_by_status(self, status: str, *other_statuses: str) -> Generator[S3Submission, None, None]:
        def query_status(status_to_query: str) -> Generator[S3Submission, None, None]:
            response = self.table.query(
                IndexName=Indexes.Status,
                KeyConditionExpression=Key('status').eq(status_to_query),
            )

            assert 200 == response['ResponseMetadata']['HTTPStatusCode']

            items = response.get('Items', [])
            items.sort(key=lambda s: s.get('id'))

            for item in items:
                item = self.get_by_id(item['id'], consistent_read=True)
                if item.status != status_to_query:
                    continue
                yield item

        yield from query_status(status)
        for other_status in other_statuses:
            yield from query_status(other_status)

    @log_action()
    @dynamodb_retry_backoff()
    def get_by_dataset_status(self, dataset_id: str, status: str, *other_statuses: str) -> Generator[S3Submission, None, None]:
        def query_status(status_to_query: str) -> Generator[S3Submission, None, None]:
            response = self.table.query(
                IndexName=Indexes.DatasetIdStatus,
                KeyConditionExpression=Key('dataset_id').eq(dataset_id) & Key('status').eq(status_to_query),
            )

            assert 200 == response['ResponseMetadata']['HTTPStatusCode']

            items = response.get('Items', [])
            items.sort(key=lambda s: s.get('id'))

            for item in items:
                item = self.get_by_id(item['id'], consistent_read=True)
                if item.status != status_to_query:
                    continue
                yield item

        yield from query_status(status)
        for other_status in other_statuses:
            yield from query_status(other_status)

    @log_action()
    @dynamodb_retry_backoff()
    def get_by_dataset_id(self, dataset_id: str):
        response = self.table.query(
            IndexName=Indexes.DatasetId,
            KeyConditionExpression=Key('dataset_id').eq(dataset_id)
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        items = response.get('Items', [])
        items.sort(key=lambda s: s.get('id'))

        for item in items:
            item = self.get_by_id(item['id'], consistent_read=True)
            yield item

    @log_action()
    @dynamodb_retry_backoff()
    def get_status_count(self, status: str) -> int:
        return ddb_query_paginated_count(
            TableName=self.table.name,
            IndexName=Indexes.Status,
            KeyConditionExpression=Key('status').eq(status)
        )

    @log_action()
    @dynamodb_retry_backoff()
    def get_dataset_status_count(self, dataset_id: str, status: str) -> int:
        return ddb_query_paginated_count(
            TableName=self.table.name,
            IndexName=Indexes.DatasetIdStatus,
            KeyConditionExpression=Key('dataset_id').eq(dataset_id) & Key('status').eq(status)
        )

    @log_action()
    def get_non_blocked_pending(self) -> Generator[S3Submission, None, None]:

        blocks = self._blocks_store.get_blocks(JOBS.S3_INGESTION)

        for pending in self.get_pending():

            if blocks.is_blocked(pending.get_block_group(), pending.dataset_id, pending.received):
                continue

            yield pending

    @log_action()
    def get_pending(self) -> Generator[S3Submission, None, None]:
        yield from self.get_by_status(PipelineStatus.Pending)

    @log_action()
    def get_pending_by_dataset(self, dataset_id: str) -> Generator[S3Submission, None, None]:
        yield from self.get_by_dataset_status(dataset_id, PipelineStatus.Pending)

    @log_action()
    def get_non_blocked_pending_by_dataset(self, dataset_id: str) -> Generator[S3Submission, None, None]:

        blocks = self._blocks_store.get_blocks(JOBS.S3_INGESTION)

        for pending in self.get_pending_by_dataset(dataset_id):

            if blocks.is_blocked(pending.get_block_group(), pending.dataset_id, pending.received):
                continue

            yield pending

    @log_action()
    def get_running(self) -> Generator[S3Submission, None, None]:
        yield from self.get_by_status(PipelineStatus.Running)

    @log_action()
    def get_active(self) -> Generator[S3Submission, None, None]:
        """
        Get all submissions that are in a state where we expect the pipeline to continue execution.

        This is distinct from get_running, as that method will _only_ retrieve submissions in state 'running'.

        Yields:
            All submissions that are in either a 'running' or 'processed' state
        """
        yield from self.get_by_status(PipelineStatus.Processed, PipelineStatus.Running)

    @log_action(action='update_status', log_args=['submission_id', 'status'])
    @dynamodb_retry_backoff()
    def mark_running(self, s3_submission_id: int) -> bool:
        return self.update_status(
            s3_submission_id, PipelineStatus.Running, PipelineStatus.Pending, raise_if_condition_fails=True
        )

    @log_action()
    def mark_scheduling(self, s3_submission_id: int) -> bool:
        return self.update_status(
            s3_submission_id, PipelineStatus.Scheduling, PipelineStatus.Pending, raise_if_condition_fails=False
        )

    @log_action()
    def mark_archived(self, s3_submission_id: int) -> bool:
        return self.update_status(
            s3_submission_id, PipelineStatus.Archived, {PipelineStatus.Rejected, PipelineStatus.Success,
                                                        PipelineStatus.Failed},
            raise_if_condition_fails=True
        )

    @log_action()
    def mark_failed(self, s3_submission_id: int) -> bool:
        return self.update_status(s3_submission_id, PipelineStatus.Failed)

    @log_action()
    def mark_rejected(self, s3_submission_id: int) -> bool:
        return self.update_status(s3_submission_id, PipelineStatus.Rejected)

    @log_action()
    def mark_success(self, s3_submission_id: int) -> bool:
        return self.update_status(
            s3_submission_id, PipelineStatus.Success, PipelineStatus.Running, raise_if_condition_fails=True
        )

    @log_action(log_args=['s3_submission_id', 'status', 'check_existing_status_is'])
    @dynamodb_retry_backoff()
    def update_status(
            self, s3_submission_id: int, status: str,
            check_existing_status_is: Union[str, AbstractSet[str]] = None,
            raise_if_condition_fails=True
    ) -> bool:
        args = dict(
            Key=dict(id=s3_submission_id),
            UpdateExpression="SET #status = :val1",
            ExpressionAttributeValues={
                ':val1': status,
            },
            ExpressionAttributeNames={
                # reserved names
                '#status': 'status',
            }
        )

        if check_existing_status_is:
            if isinstance(check_existing_status_is, str):
                check_existing_status_is = {check_existing_status_is}

            value_names = []  # type: List[str]
            for index, expected_status in enumerate(check_existing_status_is):
                value_name = ':val{}'.format(index + 2)
                value_names.append(value_name)
                args['ExpressionAttributeValues'][value_name] = expected_status

            args['ConditionExpression'] = '#status IN ({})'.format(','.join(value_names))

        try:
            response = self.table.update_item(**args)

        except Exception as e:
            if hasattr(e, 'response') and getattr(e, 'response')['Error']['Code'] == 'ConditionalCheckFailedException':
                if raise_if_condition_fails:
                    raise
                return False
            raise

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    @log_action(log_args=['s3_submission_id'])
    @dynamodb_retry_backoff()
    def update_submission_id(self, s3_submission_id: int, submission_id: int):
        response = self.table.update_item(
            Key=dict(id=s3_submission_id),
            UpdateExpression="SET submission_id = :val1",
            ExpressionAttributeValues={
                ':val1': submission_id,
            },
        )

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    @log_action(log_args=['s3_submission_id', 'submission_id'])
    @dynamodb_retry_backoff()
    def mark_running_and_set_submission_id(self, s3_submission_id: int, submission_id: int):
        args = dict(
            Key=dict(id=s3_submission_id),
            UpdateExpression="SET #status = :val1, submission_id = :val2",
            ConditionExpression='#status = :status',
            ExpressionAttributeValues={
                ':val1': PipelineStatus.Running,
                ':val2': submission_id,
                ':status': PipelineStatus.Pending
            },
            ExpressionAttributeNames={
                # reserved names
                '#status': 'status',
            }
        )
        try:
            response = self.table.update_item(**args)

        except Exception as e:
            raise

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    def to_pipeline_result(self, result_json: Mapping):
        return PipelineResult(result_json)

    @log_action(log_args=['s3_submission_id'])
    @dynamodb_retry_backoff()
    def update_finished(self, s3_submission_id: int, finished: datetime.datetime):
        response = self.table.update_item(
            Key=dict(id=s3_submission_id),
            AttributeUpdates={
                'finished': {
                    'Value': S3Submission.finished.to_primitive(finished),
                    'Action': 'PUT'
                }
            }
        )

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    @log_action(log_args=['status', 'from_time'])
    @dynamodb_retry_backoff()
    def get_completed_job_in_time(self, status, from_time: datetime.datetime) -> Generator[S3Submission, None, None]:
        response = self.table.query(
            IndexName=Indexes.Status,
            ProjectionExpression='id',
            KeyConditionExpression=Key('status').eq(status) & Key('received').gte(
                (datetime.datetime.now() - timedelta(days=1)).isoformat()
            ),
            FilterExpression=Attr('finished').exists() & Attr('finished').gte(from_time.isoformat())
        )
        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        items = response.get('Items', [])
        for item in items:
            item = self.get_by_id(item['id'])
            yield item

    @log_action(log_args=['dataset_id', 'filename'])
    @dynamodb_retry_backoff()
    def create_s3_submissions_job(self,
                                  dataset_id: str,
                                  filename: str,
                                  event_data,
                                  received: datetime = None) -> int:
        """Create a new s3 submissions job in the dynamodb table and return the unique ID given"""

        job_id = Counters.get_next_s3_submission_id()
        s3_submission_job = S3Submission(dict(
            id=job_id,
            status=PipelineStatus.Pending,
            received=received or datetime.datetime.now(),
            dataset_id=dataset_id,
            filename=filename,
            event_data=event_data
        ))
        self.put(s3_submission_job)
        return job_id


S3Submissions = S3SubmissionsStore()
