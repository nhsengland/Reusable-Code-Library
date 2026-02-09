from time import sleep
from typing import Dict, Union

from boto3.dynamodb import conditions
from botocore.exceptions import ClientError

from nhs_reusable_code_library.resuable_codes.shared import models
from nhs_reusable_code_library.resuable_codes.shared.aws import dynamodb_retry_backoff
from nhs_reusable_code_library.resuable_codes.shared.logger import log_action, add_fields
from nhs_reusable_code_library.resuable_codes.shared.models import MergeQueue
from nhs_reusable_code_library.resuable_codes.shared.store.base import ModelNotFound, BaseStore


class NotFirstInQueue(ValueError):
    pass


class MaxRetriesRemovingFromQueue(ValueError):
    pass


class MergeQueueStore(BaseStore[MergeQueue]):
    _table_name = 'merge_queues'
    _keys = ['dataset_id', 'block_group']

    def __init__(self, table=None):
        super().__init__(MergeQueue, table)

    def get_by_key(self, block_group: str, dataset_id: str, consistent_read: bool = False) -> MergeQueue:

        return self.get(key=dict(block_group=block_group, dataset_id=dataset_id), consistent_read=consistent_read)

    @staticmethod
    def key(dataset_id: str, block_group: str) -> Dict:
        return {
            'dataset_id': dataset_id,
            'block_group': block_group,
        }

    @log_action(log_args=['dataset_id', 'block_group', 'submission_id'])
    @dynamodb_retry_backoff()
    def add_to_merge_queue(self, dataset_id: str, block_group: str, submission_id: int):
        key = self.key(dataset_id, block_group)
        self.table.update_item(
            Key=key,
            UpdateExpression='SET submission_ids = '
                             'list_append(if_not_exists(submission_ids, :empty_list), :submission_id)',
            ExpressionAttributeValues={
                ':empty_list': [],
                ':submission_id': [submission_id]
            }
        )

    @log_action(log_args=['dataset_id', 'block_group', 'submission_id'])
    @dynamodb_retry_backoff()
    def remove_from_merge_queue_if_first(self, dataset_id: str, block_group: str, submission_id: int):
        key = self.key(dataset_id, block_group)

        try:
            self.table.update_item(
                Key=key,
                UpdateExpression='REMOVE submission_ids[0]',
                ConditionExpression=conditions.Attr('submission_ids[0]').eq(submission_id)
            )

        except ClientError as e:

            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise NotFirstInQueue

            raise

        return True

    @log_action(log_args=['dataset_id', 'block_group', 'submission_id'])
    @dynamodb_retry_backoff()
    def remove_from_merge_queue(self, dataset_id: str, block_group: str, submission_id: int, retries=50):
        key = self.key(dataset_id, block_group)

        retry_count = 0
        while True:
            add_fields(retry_count=retry_count)

            # Evaluate the current index for this submission in the merge queue
            current = self.get(self.key(dataset_id, block_group), consistent_read=True)
            try:
                index = current.submission_ids.index(submission_id)
            except ValueError as e:
                print('Error removing from submission ids: {}'.format(e.args[0]))
                return False

            try:
                self.table.update_item(
                    Key=key,
                    UpdateExpression='REMOVE submission_ids[{}]'.format(index),
                    ConditionExpression=conditions.Attr('submission_ids[{}]'.format(index)).eq(submission_id)
                )
                return True

            except ClientError as e:

                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    raise

                retry_count += 1

                if retry_count > retries:
                    raise

                sleep(0.2)

    @log_action()
    def create_from_submission(self, submission: models.Submission):

        self.add_to_merge_queue(
            submission.dataset_id,
            submission.get_block_group(),
            submission.id
        )

    @log_action(log_args=['dataset_id', 'block_group'])
    def get_by_dataset_and_block_group(self, dataset_id: str, block_group: str) -> Union[models.MergeQueue, None]:

        try:
            result = self.get(self.key(dataset_id, block_group), consistent_read=True)
        except ModelNotFound:
            return None

        if not result:
            return None

        add_fields(result=result)
        return result

    @log_action(log_args=['dataset_id', 'block_group'])
    def remove_by_dataset_and_block_group(self, dataset_id: str, block_group: str):
        return self.table.delete_item(Key=self.key(dataset_id, block_group))

    @log_action(log_args=['test_scope'])
    def remove_test_scope(self, test_scope: str):
        if not test_scope:
            return

        for item in self.table.scan(AttributesToGet=self._keys, ConsistentRead=True)['Items']:
            if not item.get('block_group').startswith('{}:'.format(test_scope)):
                continue

            self.table.delete_item(
                Key={k: item[k] for k in self._keys}
            )


MergeQueues = MergeQueueStore()
