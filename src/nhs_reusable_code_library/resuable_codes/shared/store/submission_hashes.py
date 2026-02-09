from typing import Union

from nhs_reusable_code_library.resuable_codes.shared.aws import dynamodb_retry_backoff
from nhs_reusable_code_library.resuable_codes.shared.logger import log_action, add_fields
from nhs_reusable_code_library.resuable_codes.shared.models import SubmissionHash
from nhs_reusable_code_library.resuable_codes.shared.store.base import BaseStore


class Indexes:
    SubmissionId = 'ix_submission_hashes_by_submission_id'
    Created = 'ix_submission_hashes_by_created'


class SubmissionHashesStore(BaseStore[SubmissionHash]):

    _table_name = 'submission_hashes'
    _keys = ['hash']

    def __init__(self, table=None):
        super().__init__(SubmissionHash, table)

    def get_by_key(self, hash: str, consistent_read: bool = False) -> SubmissionHash:

        return self.get(key=dict(hash=hash), consistent_read=consistent_read)

    @log_action(log_args=['submission_hash'])
    @dynamodb_retry_backoff()
    def create_submission_hash(self, submission_hash: SubmissionHash) -> int:
        add_fields(**submission_hash.to_primitive())

        response = self.table.put_item(
            Item=submission_hash.to_primitive(),
            ConditionExpression="attribute_not_exists(#hash)",
            ExpressionAttributeNames={
                # reserved names
                '#hash': 'hash',
            },
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        return submission_hash['hash']

    def _get_by_hash(self, hash: str) -> Union[dict, list]:

        response = self.table.get_item(
            Key={'hash': hash}
        )

        status_code = response['ResponseMetadata']['HTTPStatusCode']
        add_fields(status_code=status_code)
        assert 200 == status_code, status_code

        return response.get('Item', [])

    @log_action(log_args=['hash'])
    @dynamodb_retry_backoff()
    def get_by_hash(self, hash: str) -> SubmissionHash:

        item = self._get_by_hash(hash)

        return self.item_to_model(item)

    @log_action(log_args=['hash'])
    @dynamodb_retry_backoff()
    def exists(self, hash: str) -> bool:

        item = self._get_by_hash(hash)

        return True if item else False


SubmissionHashes = SubmissionHashesStore()
