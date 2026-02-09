import datetime
from datetime import timedelta
from typing import AbstractSet, Generator, List, Mapping, Union

from boto3.dynamodb.conditions import Attr, Key, ComparisonCondition

from nhs_reusable_code_library.resuable_codes.shared import constants
from nhs_reusable_code_library.resuable_codes.shared.aws import dynamodb_retry_backoff, s3_object, s3_split_path, s3_ls, tag_s3_object, ddb_query_paginated_count
from nhs_reusable_code_library.resuable_codes.shared.common import parse_timestamp, run_in_executor, enumlike_values, retry
from nhs_reusable_code_library.resuable_codes.shared.constants import JOBS, DigiTrialsPipelines
from nhs_reusable_code_library.resuable_codes.shared.logger import add_fields, log_action
from nhs_reusable_code_library.resuable_codes.shared.models import PipelineResult, PipelineStatus, Run, Submission, SubmissionStage
from nhs_reusable_code_library.resuable_codes.shared.store.base import BaseStore, ModelNotFound
from nhs_reusable_code_library.resuable_codes.shared.store.blocks import Blocks, BlockStore


class Indexes:
    AgentId = "ix_submissions_by_agent_id"
    RecordVersion = "ix_submissions_by_version"
    Status = "ix_submissions_by_status"
    DatasetId = "ix_submissions_by_dataset_id"
    DatasetIdStatus = "ix_submissions_by_dataset_id_status"
    WorkspaceDatasetIDStatus = "ix_submissions_by_workspace_dataset_status"


class SubmissionsStore(BaseStore):
    _table_name = 'submissions'
    _blocks_store = None
    _keys = ['id']

    def __init__(self, table=None, blocks_store: BlockStore = None):
        super().__init__(Submission, table)
        self._blocks_store = blocks_store or Blocks

    def get_by_key(self, submission_id: int, consistent_read: bool = False) -> Submission:
        return self.get(key=dict(id=submission_id), consistent_read=consistent_read)

    def get_by_id(self, submission_id: int, consistent_read: bool = False):
        return self.get_by_key(submission_id, consistent_read=consistent_read)

    async def async_get_by_id(self, submission_id: int, consistent_read: bool = False):
        return await run_in_executor(self.get_by_key, submission_id, consistent_read=consistent_read)

    class SchemaMigrations:

        def from_0_to_1(self, item):
            del item['metadata']['status']
            return True

        def from_1_to_2(self, item):
            pass

        def from_2_to_3(self, item):
            try:
                s3_url = '{}/{}'.format(item['metadata']['working_folder'], constants.PATHS.RAW_DATA)
                item['metadata']['filesize'] = s3_object(s3_url).content_length
                return True
            except:
                pass

        def from_3_to_4(self, item):
            item['created'] = item['received']
            del item['received']
            item['metadata']['submission_created_timestamp'] = item['metadata']['received_timestamp']
            del item['metadata']['received_timestamp']
            item['metadata']['event_received_timestamp'] = item['metadata']['submitted_timestamp']
            del item['metadata']['submitted_timestamp']
            item['metadata']['submitted']['event_received_timestamp'] = item['metadata']['submitted'][
                'submission_datetime']
            del item['metadata']['submitted']['submission_datetime']
            return True

        def from_4_to_5(self, item):
            item['metadata']['submitted']['expected_failure'] = False
            return True

        def from_5_to_6(self, item):
            request = item['metadata'].pop('submitted')
            request['submitted_timestamp'] = request.pop('event_received_timestamp')
            item['metadata']['request'] = request

            submitted = item['metadata'].pop('event_received_timestamp')
            item['metadata']['received_timestamp'] = item['metadata'].pop('submission_created_timestamp')
            item['metadata']['submitted_timestamp'] = submitted

            item['submitted'] = parse_timestamp(submitted)
            item['received'] = item.pop('created')

            return True

        def from_6_to_7(self, item):
            item['metadata']['request']['expected_file'] = item['metadata']['request']['expected_csv']
            del item['metadata']['request']['expected_csv']
            return True

        def from_7_to_8(self, item):
            if 'extra' in item['metadata']['request'] and 'sender_id' in item['metadata']['request']['extra']:
                sender_id = item['metadata']['request']['extra']['sender_id']
                item['sender_id'] = sender_id
                item['metadata']['sender_id'] = sender_id
                del item['metadata']['request']['extra']['sender_id']

            return True

        def from_8_to_9(self, item):
            item['metadata']['test_submission'] = False
            return True

        def from_9_to_10(self, item):
            return False

    @log_action()
    @dynamodb_retry_backoff()
    def get_by_status(self, status: str, *other_statuses: str) -> Generator[Submission, None, None]:
        def query_status(status_to_query: str) -> Generator[Submission, None, None]:
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
    def get_by_dataset_status(self, dataset_id: str, status: str,
                              *other_statuses: str) -> Generator[Submission, None, None]:
        yield from self._query_field_with_status('dataset_id', dataset_id, status, Indexes.DatasetIdStatus)
        for other_status in other_statuses:
            yield from self._query_field_with_status('dataset_id', dataset_id, other_status, Indexes.DatasetIdStatus)

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
    def get_non_blocked_pending(self) -> Generator[Submission, None, None]:

        blocks = self._blocks_store.get_blocks(JOBS.INGESTION)

        for pending in self.get_pending():

            if blocks.is_blocked(pending.get_block_group(), pending.dataset_id, pending.submitted):
                continue

            yield pending

    def get_pending(self) -> Generator[Submission, None, None]:
        yield from self.get_by_status(PipelineStatus.Pending)

    def get_running(self) -> Generator[Submission, None, None]:
        yield from self.get_by_status(PipelineStatus.Running)

    def get_active(self) -> Generator[Submission, None, None]:
        """
        Get all submissions that are in a state where we expect the pipeline to continue execution.

        This is distinct from get_running, as that method will _only_ retrieve submissions in state 'running'.

        Yields:
            All submissions that are in either a 'running' or 'processed' state
        """
        yield from self.get_by_status(PipelineStatus.Processed, PipelineStatus.Running)

    @log_action(action='update_status', log_args=['submission_id', 'status'])
    @dynamodb_retry_backoff()
    def mark_running(self, submission_id: int, run: Run) -> bool:
        target_status = PipelineStatus.Running
        add_fields(**run.to_primitive())
        add_fields(status=target_status)

        response = self.table.update_item(
            Key=dict(id=submission_id),
            UpdateExpression="SET #status = :val1, runs = list_append(runs, :val2)",
            ExpressionAttributeValues={
                ':val1': target_status,
                ':val2': [run.to_primitive()],
                ':val3': PipelineStatus.Scheduling
            },
            ExpressionAttributeNames={
                # reserved names
                '#status': 'status',
            },
            ConditionExpression='#status = :val3'
        )

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    def mark_scheduling(self, submission_id: int) -> bool:
        return self.update_status(
            submission_id, PipelineStatus.Scheduling, PipelineStatus.Pending, raise_if_condition_fails=False
        )

    def mark_archived(self, submission_id: int) -> bool:
        return self.update_status(
            submission_id, PipelineStatus.Archived, {PipelineStatus.Rejected, PipelineStatus.Success,
                                                     PipelineStatus.Failed, PipelineStatus.Cancelled,
                                                     PipelineStatus.Investigating, PipelineStatus.Unknown},
            raise_if_condition_fails=True
        )

    def mark_failed(self, submission_id: int) -> bool:
        return self.update_status(submission_id, PipelineStatus.Failed)

    @log_action(log_args=['submission_id', 'status', 'check_existing_status_is'])
    @dynamodb_retry_backoff()
    def update_status(
            self, submission_id: int, status: str,
            check_existing_status_is: Union[str, AbstractSet[str]] = None,
            raise_if_condition_fails=True
    ) -> bool:
        args = dict(
            Key=dict(id=submission_id),
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

    @log_action(log_args=['submission_id'])
    @dynamodb_retry_backoff()
    def update_pipeline_result(self, submission_id: int, pipeline_result: PipelineResult):
        response = self.table.update_item(
            Key=dict(id=submission_id),
            UpdateExpression="SET pipeline_result = :val1",
            ExpressionAttributeValues={
                ':val1': pipeline_result.to_primitive(),
            },
        )

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    def to_pipeline_result(self, result_json: Mapping):
        return PipelineResult(result_json)

    @log_action(log_args=['submission_id'])
    @dynamodb_retry_backoff()
    def update_finished(self, submission_id: int, finished: datetime.datetime):
        response = self.table.update_item(
            Key=dict(id=submission_id),
            AttributeUpdates={
                'finished': {
                    'Value': Submission.finished.to_primitive(finished),
                    'Action': 'PUT'
                }
            }
        )

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    @log_action(log_args=['status', 'from_time'])
    @dynamodb_retry_backoff()
    def get_completed_job_in_time(self, status, from_time: datetime.datetime) -> Generator[Submission, None, None]:
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

    def get_previous_submission_id(self, received_date: str, dataset_id: str) -> int:
        response = self.table.query(
            IndexName="ix_submissions_by_dataset_id",
            KeyConditionExpression=Key('dataset_id').eq(dataset_id) & Key('received').lt(received_date),
            ScanIndexForward=False,
            Limit=1
        )
        prev_submission = response.get('Items', [])

        if prev_submission:
            return prev_submission[0].get('id')

    @log_action()
    @dynamodb_retry_backoff()
    def get_by_workspace_dataset_status(self, workspace: str, dataset_id: str,
                                        status_list: List[str]) -> Generator[Submission, None, None]:
        for status in status_list:
            yield from self._query_field_with_status('workspace', workspace, status, Indexes.WorkspaceDatasetIDStatus,
                                                     Attr('dataset_id').eq(dataset_id))

    def _query_field_with_status(self, key: str, value: str, status_to_query: str, index: str,
                                 attr_filter: ComparisonCondition = None) -> Generator[Submission, None, None]:

        kwargs = {}
        if attr_filter:
            kwargs = dict(FilterExpression=attr_filter)

        response = self.table.query(
            IndexName=index,
            KeyConditionExpression=Key(key).eq(value) & Key('status').eq(status_to_query),
            **kwargs
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        items = response.get('Items', [])
        items.sort(key=lambda s: s.get('id'))

        for item in items:
            item = self.get_by_id(item['id'], consistent_read=True)
            if item.status != status_to_query:
                continue
            yield item

    @log_action()
    @dynamodb_retry_backoff()
    def append_submission_stage(self, submission_id: int, submission_stage: SubmissionStage):
        result = self.table.update_item(
            Key=dict(id=submission_id),
            UpdateExpression="SET stages = list_append(stages, :i)",
            ExpressionAttributeValues={
                ':i': [submission_stage.to_primitive()],
            },
            ReturnValues="UPDATED_NEW"
        )

        assert result['ResponseMetadata']['HTTPStatusCode'] == 200 
        assert 'Attributes' in result
        assert dict(submission_stage) in result['Attributes']['stages']

    @log_action(log_args=['filepath', 'submission_id', 'dataset_id'])
    def add_dataset_id_tag(self, filepath: str, submission_id, check_folder: bool = False, dataset_id: str = None):
        """
            The check_folder parameter was added for scenarios where parts of the object are stored in a folder. An
            example could be partitions generated by Apache Spark.
        """
        _, bucket, key = s3_split_path(filepath)

        if not dataset_id:
            try:
                submission_id = int(submission_id)
            except ValueError:
                return

            try:
                dataset_id = Submissions.get_by_id(submission_id).dataset_id
            except ModelNotFound:
                # if we can't find a submission, then we don't care about tagging
                return

        if dataset_id not in enumlike_values(DigiTrialsPipelines):
            return

        objects_to_be_tagged = [obj.key for obj in s3_ls(filepath)] if check_folder else [key]

        @retry(max_retries=5, exponential_backoff=True)
        @log_action()
        def _put_object_tag(obj_key):
            tag_s3_object(bucket, obj_key, 'dataset_id', dataset_id)

        for key in objects_to_be_tagged:
            _put_object_tag(key)


Submissions = SubmissionsStore()
