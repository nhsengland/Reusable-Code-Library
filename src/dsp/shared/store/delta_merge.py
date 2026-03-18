from datetime import datetime
from typing import Generator, List, Union, Optional

from boto3.dynamodb.conditions import Key, Attr

from dsp.shared.aws import dynamodb_retry_backoff, ddb_query_paginated_count
from dsp.shared.constants import JOBS
from dsp.shared.logger import app_logger, log_action, LogLevel, add_fields
from dsp.shared.models import MergeStatus, DeltaMerge, Submission, MergeType
from dsp.shared.store.base import BaseStore, ModelNotFound
from dsp.shared.store.blocks import Blocks, BlockStore
from dsp.shared.store.common import replace_decimals
from dsp.shared.store.merge_queues import MergeQueues, MergeQueueStore


class Indexes:
    RecordVersion = "ix_delta_merge_by_version"
    Status = "ix_delta_merge_by_status"
    DatasetId = "ix_delta_merge_by_dataset_id"
    BlockGroup = "ix_delta_merge_by_block_group"


class DeltaMergeStore(BaseStore[DeltaMerge]):
    _table_name = 'delta_merge'
    _keys = ['key']

    _merge_queues_store = None

    def __init__(self, table=None, merge_queues_store: MergeQueueStore = None, blocks_store: BlockStore = None):
        super().__init__(DeltaMerge, table)
        self._merge_queues_store = merge_queues_store or MergeQueues
        self._blocks_store = blocks_store or Blocks

    def get_by_key(self, submission_id: int, merge_type: str, consistent_read: bool = False) -> DeltaMerge:

        return self.get(key=dict(key=self.key(submission_id, merge_type)), consistent_read=consistent_read)

    @staticmethod
    def key(submission_id: int, merge_type: str) -> str:
        return '{}:{}'.format(submission_id, merge_type)

    @log_action(log_level=LogLevel.INFO, log_args=['submission', 'received', 'dataset_id', 'uri', 'block_group'])
    def create_from_submission(self, submission: Submission, merge_type: str, merge_status: str) -> str:

        uri = submission.metadata.working_folder

        return self.create_delta_merge(
            submission_id=submission.id,
            submitted=submission.submitted,
            dataset_id=submission.dataset_id,
            merge_type=merge_type,
            uri=uri,
            block_group=submission.get_block_group(),
            merge_status=merge_status,
            test_scope=submission.test_scope
        )

    @log_action(log_level=LogLevel.INFO,
                log_args=['submission', 'submitted', 'dataset_id', 'merge_type', 'uri', 'block_group'])
    @dynamodb_retry_backoff()
    def create_delta_merge(
            self, submission_id: int, submitted: datetime, dataset_id: str, merge_type: str,
            uri: str, block_group: str, merge_status: str, test_scope: str = None
    ) -> str:

        merge = DeltaMerge(dict(
            key=self.key(submission_id, merge_type),
            submitted=submitted,
            dataset_id=dataset_id,
            submission_id=submission_id,
            type=merge_type,
            uri=uri,
            block_group=block_group,
            status=merge_status,
            test_scope=test_scope
        ))

        response = self.table.put_item(
            Item=merge.to_primitive(),
            ConditionExpression="attribute_not_exists(#key)",
            ExpressionAttributeNames={
                # reserved names
                '#key': 'key',
            }
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        return merge.key

    @log_action(log_args=['status'])
    @dynamodb_retry_backoff()
    def get_by_status_paginate(self, status: str, dataset_id: str = None, exclusive_start_key: Optional[str] = None) -> \
            Generator[dict, None, None]:

        extra_kwargs = {}
        if dataset_id:
            extra_kwargs['FilterExpression'] = Attr('dataset_id').eq(dataset_id)
        if exclusive_start_key:
            extra_kwargs['ExclusiveStartKey'] = exclusive_start_key
        response = self.table.query(
            IndexName=Indexes.Status,
            KeyConditionExpression=Key('status').eq(status),
            **extra_kwargs
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        items = response.get('Items', [])
        for item in items:
            yield item

        last_evaluated_key = response.get('LastEvaluatedKey')
        if last_evaluated_key:
            for item in self.get_by_status_paginate(status, dataset_id=dataset_id,
                                                    exclusive_start_key=last_evaluated_key):
                yield item

    @log_action(log_args=['status'])
    def get_by_status(self, status: str, dataset_id: str = None) -> List[dict]:
        items = list(self.get_by_status_paginate(status, dataset_id))
        items.sort(key=lambda i: i['created'])
        results = replace_decimals(items)
        add_fields(results=[i['key'] for i in results[:100]])

        return results

    @log_action(log_args=['block_group'])
    @dynamodb_retry_backoff()
    def archive_by_block_group(self, block_group: str) -> List[str]:
        keys_updated = []

        for item in self.table.query(
                IndexName=Indexes.BlockGroup,
                KeyConditionExpression=Key('block_group').eq(block_group),
                FilterExpression=Attr('status').ne(MergeStatus.Archived),
        ).get('Items', []):
            self.mark_archived(item['key'])
            keys_updated.append(item['key'])

        return keys_updated

    @log_action(log_args=['test_scope'])
    @dynamodb_retry_backoff()
    def archive_by_test_scope(self, test_scope: str) -> List[str]:
        keys_updated = []

        for item in self.table.scan(
                AttributesToGet=['key', 'test_scope'],
                ConsistentRead=True
        )['Items']:

            if item.get('test_scope') != test_scope:
                continue

            self.mark_archived(item['key'])
            keys_updated.append(item['key'])

        return keys_updated

    @log_action(log_args=['status'])
    @dynamodb_retry_backoff()
    def get_count_by_status_type(self, status: str, type: str) -> int:
        app_logger.debug(lambda: dict(message='get_count_by_status', status=status))

        return ddb_query_paginated_count(
            TableName=self.table.name,
            IndexName=Indexes.Status,
            KeyConditionExpression=Key('status').eq(status),
            FilterExpression=Attr('key').contains(':' + type)
        )

    @log_action(log_args=['key', 'projection_expr', 'attribute_names', 'consistent_read', 'raise_if_not_found'])
    @dynamodb_retry_backoff()
    def get_item(self, key: str, projection_expr: str = None, attribute_names: dict = None,
                 consistent_read: bool = False, raise_if_not_found: bool = False) -> Union[DeltaMerge, None]:

        args = dict(
            Key=dict(key=key),
            ConsistentRead=consistent_read
        )

        if projection_expr:
            args['ProjectionExpression'] = projection_expr

        if attribute_names:
            args['ExpressionAttributeNames'] = attribute_names

        response = self.table.get_item(**args)

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            return None

        item = response.get('Item')
        if not item:
            if raise_if_not_found:
                raise ModelNotFound

            return None

        item['key'] = key
        return self.item_to_model(item)

    @log_action()
    def get_non_blocked_dq_merge(self) -> Generator[DeltaMerge, None, None]:

        blocks = self._blocks_store.get_blocks(JOBS.DQ_MERGE)

        for pending in self.get_pending():

            if pending.type != MergeType.DQFail:
                continue

            if blocks.is_blocked(pending.block_group, pending.dataset_id, pending.submitted):
                continue

            yield pending

    @log_action()
    def get_non_blocked(self, dataset_id: str = None) -> Generator[DeltaMerge, None, None]:
        blocks = self._blocks_store.get_blocks(JOBS.DELTA_MERGE)

        for delta_merge in self.get_pending(dataset_id):

            if delta_merge.type != MergeType.DQPass:
                continue

            if blocks.is_blocked(delta_merge.block_group, delta_merge.dataset_id, delta_merge.submitted):
                continue

            merge_queue = self._merge_queues_store.get_by_dataset_and_block_group(
                delta_merge.dataset_id,
                delta_merge.block_group
            )

            if not merge_queue:
                continue

            first_id_in_queue = merge_queue.submission_ids[0] if merge_queue.submission_ids else None

            if first_id_in_queue is None:
                app_logger.error(args=lambda: dict(message='nothing in queue', **delta_merge.to_primitive()))
                continue

            if first_id_in_queue != delta_merge.submission_id:
                app_logger.info(args=lambda: dict(
                    message='not first in queue', first=first_id_in_queue,
                    queue=merge_queue.submission_ids, **delta_merge.to_primitive(),

                ))
                continue

            yield delta_merge

    @log_action()
    @dynamodb_retry_backoff()
    def get_pending(self, dataset_id: str = None, include_dq: bool = True) -> Generator[DeltaMerge, None, None]:

        for item in self.get_by_status(MergeStatus.Pending, dataset_id=dataset_id):

            projected = self.get_item(
                item['key'],
                '#status, created, submitted, dataset_id, submission_id, #type, uri, block_group',
                {
                    '#status': 'status',
                    '#type': 'type'
                },
                consistent_read=True
            )

            if not include_dq:
                if item['key'].endswith('dq'):
                    continue

            if projected.status != MergeStatus.Pending:
                continue

            yield projected

    def mark_merging(self, key: str) -> bool:

        return self.update_status(key, MergeStatus.Merging, MergeStatus.Pending, raise_if_condition_fails=False)

    def mark_failed(self, key: str) -> bool:

        return self.update_status(key, MergeStatus.Failed)

    def mark_merged(self, key: str) -> bool:

        return self.update_status(key, MergeStatus.Merged)

    def mark_archived(self, key: str) -> bool:

        return self.update_status(key, MergeStatus.Archived)

    @log_action(log_level=LogLevel.INFO, log_args=['key', 'status', 'check_existing_status_is'])
    @dynamodb_retry_backoff()
    def update_status(
            self, key: str, status: str,
            check_existing_status_is: Union[str, None] = None,
            raise_if_condition_fails=True
    ) -> bool:

        args = dict(
            Key=dict(key=key),
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
            args['ExpressionAttributeValues'][':val2'] = check_existing_status_is
            args['ConditionExpression'] = '#status = :val2'

        try:
            response = self.table.update_item(**args)

        except Exception as e:
            if hasattr(e, 'response') and getattr(e, 'response')['Error']['Code'] == 'ConditionalCheckFailedException':
                if raise_if_condition_fails:
                    raise
                return False
            raise

        return response['ResponseMetadata']['HTTPStatusCode'] == 200


DeltaMerges = DeltaMergeStore()
