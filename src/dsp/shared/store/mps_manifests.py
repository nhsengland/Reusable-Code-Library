from typing import Generator, Optional, List, Union, Iterable

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from dsp.shared.aws import dynamodb_retry_backoff
from dsp.shared.logger import log_action, LogLevel
from dsp.shared.models import MpsManifest, MpsManifestStatus
from dsp.shared.store.base import BaseStore, ModelNotFound


class Indexes:
    Status = "ix_status"


class MpsManifestsStore(BaseStore[MpsManifest]):

    _table_name = "mps_manifests"
    _keys = ['submission_id']

    def __init__(self, table=None):
        super().__init__(MpsManifest, table)

    def get_by_key(self, submission_id: str, consistent_read: bool = True) -> MpsManifest:
        return self.get(key=dict(submission_id=submission_id), consistent_read=consistent_read)

    @log_action(
        log_level=LogLevel.INFO,
        log_args=[
            'submission_id',
            'request_chunks',
            'response_chunks',
            'metadata'
        ])
    @dynamodb_retry_backoff()
    def create_mps_manifest(
            self,
            submission_id: Union[int, str],
            request_chunks: List[str],
            response_chunks: List[str],
            metadata: Optional[str] = None,
    ) -> str:
        """ Create a new MPS manifest and return the submission ID given """
        if not request_chunks:
            raise ValueError('Request chunks should not be empty')

        all_chunks = request_chunks
        if response_chunks:
            all_chunks = request_chunks + response_chunks

        mps_manifest = MpsManifest(dict(
            submission_id=str(submission_id),
            all_chunks=all_chunks,
            remaining_chunks=request_chunks,
            status=MpsManifestStatus.Pending,
            metadata=metadata
        ))

        response = self.table.put_item(
            Item=mps_manifest.to_primitive(),
            ConditionExpression='attribute_not_exists(submission_id)'
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        return mps_manifest.submission_id

    @log_action(log_level=LogLevel.INFO, log_args=['submission_id'])
    @dynamodb_retry_backoff()
    def get_by_submission_id(self, submission_id: Union[str, int]) -> MpsManifest:
        return self.get_by_key(str(submission_id))

    @log_action(log_level=LogLevel.INFO, log_args=['status'])
    @dynamodb_retry_backoff()
    def get_all_by_status(self, status: str) -> Optional[Generator[MpsManifest, None, None]]:
        response = self.table.query(
            IndexName=Indexes.Status,
            KeyConditionExpression=Key('status').eq(status)
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        items = response.get('Items', [])

        for item in items:
            item = self.get_by_submission_id(item['submission_id'])
            mps_manifest = MpsManifest(item)
            if mps_manifest.status != status:
                continue
            yield mps_manifest

    @log_action(log_level=LogLevel.INFO)
    @dynamodb_retry_backoff()
    def get_all_pending(self) -> Optional[List[MpsManifest]]:
        return self.get_all_by_status(MpsManifestStatus.Pending)

    @log_action(log_level=LogLevel.INFO)
    @dynamodb_retry_backoff()
    def get_all_success(self) -> Optional[List[MpsManifest]]:
        return self.get_all_by_status(MpsManifestStatus.Success)

    @log_action(log_level=LogLevel.INFO, log_args=['submission_id'])
    @dynamodb_retry_backoff()
    def mark_success(self, submission_id: str) -> bool:
        try:
            response = self.table.update_item(
                Key=dict(submission_id=submission_id),
                UpdateExpression="SET #status = :val2",
                ExpressionAttributeValues={
                    ':val1': MpsManifestStatus.Pending,
                    ':val2': MpsManifestStatus.Success
                },
                ExpressionAttributeNames={
                    # reserved names
                    '#status': 'status'
                },
                ConditionExpression='#status = :val1'
            )
            assert response['ResponseMetadata']['HTTPStatusCode'] == 200
            return True
        except ClientError as e:
            print("Error occurred marking as success: {}".format(e.response['Error']['Code']))
            return False

    @log_action(log_level=LogLevel.INFO, log_args=['submission_id', 'chunk_to_remove'])
    @dynamodb_retry_backoff()
    def remove_from_remaining_chunks(self, submission_id: str, chunk_to_remove: str) -> bool:
        key = {'submission_id': submission_id}
        current = self.get(key, consistent_read=True)
        try:
            index = current.remaining_chunks.index(chunk_to_remove)
        except ValueError as e:
            print('Error removing chunk: {}'.format(e.args[0]))
            return False

        try:
            self.table.update_item(
                Key=key,
                UpdateExpression='REMOVE remaining_chunks[{}]'.format(index),
                ConditionExpression=Attr('remaining_chunks[{}]'.format(index)).eq(chunk_to_remove)
            )
            return True

        except ClientError as e:
            print("Error removing remaining chunk: {}".format(e.response['Error']['Code']))
            return False


MpsManifests = MpsManifestsStore()
