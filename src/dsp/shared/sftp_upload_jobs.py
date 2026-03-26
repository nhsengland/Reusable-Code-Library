import datetime
import sys
from typing import Generator, Optional, Union, AbstractSet, Dict, Any
from uuid import uuid4

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from dsp.shared.aws import dynamodb_retry_backoff, s3_uri_get_size, ddb_query_paginated_count
from dsp.shared.logger import log_action, LogLevel, debug_fields, app_logger, add_fields
from dsp.shared.models import SftpTransferJob, SftpTransferStatus
from dsp.shared.store.base import BaseStore


class Indexes:
    Status_Created = "ix_status_created"


class SftpUploadJobsStore(BaseStore[SftpTransferJob]):
    _table_name = 'sftp_upload_jobs'

    def __init__(self, table=None):
        super().__init__(SftpTransferJob, table)

    class SchemaMigrations:
        def from_0_to_1(self, job: Dict[str, Any]):
            return True

        def from_1_to_2(self, job: Dict[str, Any]):
            job['is_seft'] = True
            job['directory'] = job['seft_dir']
            job['filename'] = job['seft_filename']
            del job['seft_dir']
            del job['seft_filename']
            return True

        def from_2_to_3(self, job: Dict[str, Any]):
            job['content_length'] = 0
            return True

    @log_action(log_level=LogLevel.INFO,
                log_args=['s3_location', 'directory', 'filename', 'correlation_id',
                          'hostname', 'username', 'is_seft'])
    @dynamodb_retry_backoff()
    def create_sftp_upload_job(
            self,
            s3_location: str,
            directory: Optional[str],
            filename: Optional[str],
            correlation_id: str,
            status: str = SftpTransferStatus.Ready,
            hostname: Optional[str] = None,
            username: Optional[str] = None,
            is_seft: bool = False
    ) -> str:
        """ Create a new SFTP upload job in the DynamoDB table and return the unique key given """
        content_length = s3_uri_get_size(s3_location)

        add_fields(content_length=content_length)

        seft_transfer_job = SftpTransferJob(dict(
            transfer_id=str(uuid4()),
            status=status,
            s3_location=s3_location,
            directory=directory,
            filename=filename,
            correlation_id=correlation_id,
            hostname=hostname,
            username=username,
            is_seft=is_seft,
            content_length=content_length
        ))

        response = self.table.put_item(
            Item=seft_transfer_job.to_primitive(),
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        return seft_transfer_job.transfer_id

    def create_seft_upload_job(
            self,
            s3_location: str,
            seft_dir: str,
            seft_filename: Optional[str],
            correlation_id: str,
            status: str = SftpTransferStatus.Ready
    ) -> str:
        """
        Creates a new SFTP upload job with the is_seft flag set to True.
        """
        return self.create_sftp_upload_job(s3_location=s3_location, directory=seft_dir, filename=seft_filename,
                                           correlation_id=correlation_id, status=status, hostname=None,
                                           username=None, is_seft=True)

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def get_by_transfer_id(self, transfer_id: str, consistent_read: bool = True) -> Optional[SftpTransferJob]:
        model = self.get({'transfer_id': transfer_id}, consistent_read=consistent_read)

        debug_fields(lambda: model.to_primitive())

        return model

    @log_action(log_level=LogLevel.INFO, log_args=['status'])
    @dynamodb_retry_backoff()
    def get_all_by_status(self, status: str) -> Generator[SftpTransferJob, None, None]:

        response = self.table.query(
            IndexName=Indexes.Status_Created,
            KeyConditionExpression=Key('status').eq(status),
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        items = response.get('Items', [])

        for item in items:
            item = self.get_by_transfer_id(item['transfer_id'])
            if item.status != status:
                continue
            yield SftpTransferJob(item)

    @log_action()
    @dynamodb_retry_backoff()
    def get_status_count(self, status: str) -> int:
        return ddb_query_paginated_count(
            TableName=self.table.name,
            IndexName=Indexes.Status_Created,
            KeyConditionExpression=Key('status').eq(status),
        )

    def get_all_ready(self) -> Generator[SftpTransferJob, None, None]:
        yield from self.get_all_by_status(SftpTransferStatus.Ready)

    def get_all_processing(self) -> Generator[SftpTransferJob, None, None]:
        yield from self.get_all_by_status(SftpTransferStatus.Processing)

    def get_all_failed(self) -> Generator[SftpTransferJob, None, None]:
        yield from self.get_all_by_status(SftpTransferStatus.Failed)

    @log_action(log_args=['min_content_length', 'max_content_length'])
    @dynamodb_retry_backoff()
    def get_next_ready(self, min_content_length: int = 0, max_content_length: int = sys.maxsize) \
            -> Optional[SftpTransferJob]:
        ready = self.table.query(
            IndexName=Indexes.Status_Created,
            KeyConditionExpression=Key('status').eq(SftpTransferStatus.Ready),
            # Inclusive filter
            FilterExpression=Attr('content_length').between(min_content_length, max_content_length),
        )
        queue = ready['Items']

        queue.sort(key=lambda i: i['created'])
        if queue:
            job = self.get_by_transfer_id(queue[0]['transfer_id'])
            if job.status != SftpTransferStatus.Ready:
                return None
            return job

        return None

    @log_action(log_level=LogLevel.INFO, log_args=['message_id', 'mailbox_from'])
    @dynamodb_retry_backoff()
    def mark_processing(self, transfer_id: str) -> bool:
        # If there's no job with that transfer_id, we can't do anything with it
        job = self.get_by_transfer_id(transfer_id)
        if not job:
            return False

        mark_processing = self._transition_status(transfer_id, SftpTransferStatus.Ready, SftpTransferStatus.Processing)
        return mark_processing

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def rollback_to_ready(self, transfer_id: str) -> bool:
        return self._transition_status(transfer_id, SftpTransferStatus.Processing, SftpTransferStatus.Ready)

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def mark_failed(self, transfer_id: str) -> bool:
        return self._transition_status(transfer_id, SftpTransferStatus.Processing, SftpTransferStatus.Failed)

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def mark_archived(self, transfer_id: str) -> bool:
        return self._transition_status(transfer_id, frozenset(), SftpTransferStatus.Archived)

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def mark_success(self, transfer_id: str) -> bool:
        job = self.get_by_transfer_id(transfer_id)
        if not job.status == SftpTransferStatus.Processing:
            return False
        mark_success = self._transition_status(transfer_id, SftpTransferStatus.Processing, SftpTransferStatus.Success)

        return mark_success

    def _transition_status(self, transfer_id: str, mandated_from_status: Union[str, AbstractSet[str]],
                           to_status: str) -> bool:
        """Transition status to a new status. The `mandated_from_status` is the status in which the job must be in
        in order to transition.

        Args:
            transfer_id: the transfer_idid of the job
            mandated_from_status: the status(es) the job must currently have in order to transition
            to_status: the status that we want to transition to

        Returns:
            bool: True if successfully transitioned; False if not
        """
        args = dict(
            Key=dict(transfer_id=transfer_id),
            UpdateExpression="SET #status = :val1, completed = :val2",
            ExpressionAttributeValues={
                ':val1': to_status,
                ':val2': datetime.datetime.utcnow().isoformat(),
            },
            ExpressionAttributeNames={
                # reserved names
                '#status': 'status',
            }
        )

        if mandated_from_status:
            if isinstance(mandated_from_status, str):
                mandated_from_status = {mandated_from_status}

            value_names = []  # type: List[str]
            for index, expected_status in enumerate(mandated_from_status):
                value_name = ':val{}'.format(index + 3)
                value_names.append(value_name)
                args['ExpressionAttributeValues'][value_name] = expected_status

            args['ConditionExpression'] = '#status IN ({})'.format(','.join(value_names))

        try:
            response = self.table.update_item(**args)
        except ClientError as e:
            app_logger.warn(lambda: dict(
                message='Error occurred transitioning to {}: {}'.format(to_status, e.response['Error']['Code'])))
            return False

        return response['ResponseMetadata']['HTTPStatusCode'] == 200


SftpUploadJobs = SftpUploadJobsStore()
