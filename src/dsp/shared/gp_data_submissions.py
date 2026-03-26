import os
import time
from operator import itemgetter
from typing import AbstractSet, Generator, List, Mapping, Union, MutableMapping

from boto3.dynamodb.conditions import Key
from dsp.shared.aws import dynamodb_retry_backoff, s3_build_uri, ddb_query_paginated_count
from dsp.shared.constants import JOBS
from dsp.shared.logger import add_fields, log_action
from dsp.shared.models import Run, GPDataSubmission, GPDataJobStatus
from dsp.shared.store.base import BaseStore
from dsp.shared.store.blocks import Blocks, BlockStore


class Indexes:
    Status = "ix_submissions_by_status"
    DatasetIdStatus = "ix_submissions_by_dataset_id_status"
    FileNameWorkingFolder = "ix_submissions_by_filename_working_folder"


class GPDataSubmissionsStore(BaseStore):
    _table_name = 'gp_data_submissions'
    _blocks_store = None
    _keys = ['id']

    def __init__(self, table=None, blocks_store: BlockStore = None):
        super().__init__(GPDataSubmission, table)
        self._blocks_store = blocks_store or Blocks

    def get_by_id(self, submission_id: int, consistent_read: bool = False) -> GPDataSubmission:
        return self.get(key={"id": submission_id}, consistent_read=consistent_read)

    @log_action(action='get_by_status', log_args=['status'])
    @dynamodb_retry_backoff()
    def get_by_status(self, status: str) -> Generator[GPDataSubmission, None, None]:
        response = self.table.query(
            IndexName=Indexes.Status,
            KeyConditionExpression=Key('status').eq(status),
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        items = response.get('Items', [])  # List[Dict]

        for item in sorted(items, key=itemgetter('id')):
            gp_data_submission = self.get_by_id(item['id'])
            if gp_data_submission.status != status:
                continue
            yield gp_data_submission

    @log_action(action='get_by_dataset_status_count', log_args=['dataset_id', 'status'])
    @dynamodb_retry_backoff()
    def get_by_dataset_status_count(self, dataset_id: str, status: str) -> int:
        count = ddb_query_paginated_count(
            TableName=self.table.name,
            IndexName=Indexes.DatasetIdStatus,
            KeyConditionExpression=Key('dataset_id').eq(dataset_id) & Key('status').eq(status)
        )

        add_fields(count=count)
        return count

    @log_action(action='get_by_dataset_status', log_args=['dataset_id', 'status'])
    @dynamodb_retry_backoff()
    def get_by_dataset_status(self, dataset_id: str, status: str) -> Generator[GPDataSubmission, None, None]:
        response = self.table.query(
            IndexName=Indexes.DatasetIdStatus,
            KeyConditionExpression=Key('dataset_id').eq(dataset_id) & Key('status').eq(status)
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        items = response.get('Items', [])  # List[Dict]

        for item in sorted(items, key=itemgetter('id')):
            item = self.get_by_id(item['id'], consistent_read=True)
            if item.status != status:
                continue
            yield item

    @log_action(action='get_by_daterange_status', log_args=['dataset_id', 'status'])
    @dynamodb_retry_backoff()
    def get_by_daterange_status(self, received_from: str, received_to: str, status: str) \
            -> Generator[GPDataSubmission, None, None]:
        if received_from and received_to:
            condition = Key('received').between(received_from, received_to)
        elif received_from:
            condition = Key('received').gte(received_from)
        elif received_to:
            condition = Key('received').lte(received_to)
        else:
            raise ValueError("condition_from or condition_to required")

        response = self.table.query(
            IndexName=Indexes.Status,
            KeyConditionExpression=condition & Key('status').eq(status),
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        items = response.get('Items', [])  # List[Dict]

        for item in sorted(items, key=itemgetter('id')):
            item = self.get_by_id(item['id'], consistent_read=True)
            if item.status != status:
                continue
            yield item

    @log_action(action='get_ids_by_key', log_args=['key'])
    @dynamodb_retry_backoff()
    def get_ids_by_key(self, key: str, bucket: str = None) -> List[int]:
        working_folder = s3_build_uri(bucket, "/".join(key.split("/")[:-1])) if bucket else "/".join(key.split("/")[:-1])
        response = self.table.query(
            IndexName=Indexes.FileNameWorkingFolder,
            ProjectionExpression="id",
            KeyConditionExpression=Key('filename').eq(os.path.basename(key)) & Key('working_folder').eq(working_folder)
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        items = response.get('Items', [])  # List[Dict]

        return [item['id'] for item in sorted(items, key=itemgetter('id'))]

    def get_running(self) -> Generator[GPDataSubmission, None, None]:
        yield from self.get_by_status(GPDataJobStatus.Running)

    def get_pending_by_dataset_id(self, dataset_id: str) -> Generator[GPDataSubmission, None, None]:
        yield from self.get_by_dataset_status(dataset_id, GPDataJobStatus.Pending)

    def get_non_blocked_pending_by_dataset_id(self, dataset_id: str) -> Generator[GPDataSubmission, None, None]:
        # check if the block has been applied on the whole job type (this happens as part of DPS deployment)
        blocks = self._blocks_store.get_blocks(JOBS.GP_DATA_INGESTION)
        if blocks.is_blocked(dataset_id=dataset_id):
            return

        # get all blocked channels first
        blocked_channels = {submission.channel_id() for status in GPDataJobStatus.blocking_statuses()
                            for submission in self.get_by_dataset_status(dataset_id, status)}

        for pending in self.get_pending_by_dataset_id(dataset_id):
            channel_id = pending.channel_id()
            if channel_id not in blocked_channels:
                # the earliest pending submission of a particular channel blocks all subsequent pending submissions
                # from the same channel, so we need to register the new block
                blocked_channels.add(channel_id)
                yield pending

    @log_action()
    def get_non_blocked_pending_count_by_dataset_id_with_limit(self, dataset_id: str, count_limit: int) -> int:
        count = 0
        for _ in self.get_non_blocked_pending_by_dataset_id(dataset_id):
            count += 1
            if count == count_limit:
                break
        return count

    @log_action()
    def get_by_dataset_id_without_channel_blocking(self, dataset_id: str) -> Generator[GPDataSubmission, None, None]:
        # check if the block has been applied on the whole job type (this happens as part of DPS deployment)
        blocks = self._blocks_store.get_blocks(JOBS.GP_DATA_INGESTION)
        if blocks.is_blocked(dataset_id=dataset_id):
            return

        for pending in self.get_pending_by_dataset_id(dataset_id):
            yield pending

    @log_action(action='mark_running', log_args=['submission_id'])
    @dynamodb_retry_backoff()
    def mark_running(self, submission_id: int, run: Run) -> bool:
        target_status = GPDataJobStatus.Running
        add_fields(**run.to_primitive())
        add_fields(status=target_status, submission_running_timestamp=time.time())

        response = self.table.update_item(
            Key=dict(id=submission_id),
            UpdateExpression="SET #status = :val1, runs = list_append(runs, :val2)",
            ExpressionAttributeValues={
                ':val1': target_status,
                ':val2': [run.to_primitive()]
            },
            ExpressionAttributeNames={
                # reserved names
                '#status': 'status',
            }
        )

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    @log_action(log_args=['submission_id', 'status', 'check_existing_status_is', 'raise_condition_if_fails'])
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
        )  # type: MutableMapping[str, Union[str, Mapping[str, str]]]

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

    def mark_failed(self, submission_id: int) -> bool:
        return self.update_status(submission_id, GPDataJobStatus.Failed)

    def mark_rejected(self, submission_id: int) -> bool:
        return self.update_status(submission_id, GPDataJobStatus.Rejected)

    def mark_success(self, submission_id: int) -> bool:
        return self.update_status(submission_id, GPDataJobStatus.Success)

    def mark_pending(self, submission_id: int) -> bool:
        return self.update_status(submission_id, GPDataJobStatus.Pending)

    def mark_archived(self, submission_id: int) -> bool:
        return self.update_status(submission_id, GPDataJobStatus.Archived)


GPDataSubmissions = GPDataSubmissionsStore()
