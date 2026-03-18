import sys
from typing import Optional, List
from uuid import uuid4
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from dsp.shared.aws import dynamodb_retry_backoff, s3_uri_get_size
from dsp.shared.constants import Blockables, Senders
from dsp.shared.logger import log_action, LogLevel, app_logger, add_fields
from dsp.shared.models import MeshTransferJob, MeshTransferStatus, MeshMetadata, Block
from dsp.shared.store.mesh_transfer_jobs import MeshTransferJobsStore, MeshTransferJobPriority, Indexes as BaseIndexes


class Indexes(BaseIndexes):
    pass


class MeshUploadJobsStore(MeshTransferJobsStore):
    _table_name = 'mesh_upload_jobs'

    @log_action(log_level=LogLevel.INFO,
                log_args=['workflow_id', 's3_locations', 'mesh_metadata', 'mailbox_to', 'mailbox_from'])
    @dynamodb_retry_backoff()
    def create_mesh_upload_job(
            self,
            workflow_id: str,
            s3_locations: List[str],
            mesh_metadata: MeshMetadata,
            mailbox_to: str,
            mailbox_from: Optional[str] = None,
            status: str = MeshTransferStatus.Pending,
            priority: int = MeshTransferJobPriority.Normal
    ) -> str:
        content_length = sum([s3_uri_get_size(s3_loc) for s3_loc in s3_locations])
        """ Create a new MESH upload job in the DynamoDB table and return the unique key given """
        mesh_upload_job = MeshTransferJob(dict(
            transfer_id=str(uuid4()),
            status=status,
            workflow_id=workflow_id,
            s3_locations=s3_locations,
            mesh_metadata=mesh_metadata,
            mailbox_to=mailbox_to,
            mailbox_from=mailbox_from,
            priority=priority,
            content_length=content_length,
        ))

        add_fields(job=mesh_upload_job)

        response = self.table.put_item(
            Item=mesh_upload_job.to_primitive(),
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        add_fields(transfer_id=mesh_upload_job.transfer_id)

        return mesh_upload_job.transfer_id

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def mark_success(self, transfer_id: str, message_id: str) -> bool:
        # Do not mark as success before the message id is set...
        job = self.get_by_transfer_id(transfer_id)
        if not job.status == MeshTransferStatus.Processing:
            return False
        self._set_message_id(transfer_id, message_id)
        mark_success = self._transition_status(transfer_id, MeshTransferStatus.Processing, MeshTransferStatus.Success)

        return mark_success

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id', 'message_id'])
    @dynamodb_retry_backoff()
    def _set_message_id(self, transfer_id: str, message_id: str) -> bool:
        try:
            job = self.get_by_transfer_id(transfer_id)
            response = self.table.update_item(
                Key=dict(transfer_id=transfer_id),
                UpdateExpression="SET message_id = :val1",
                ExpressionAttributeValues={
                    ':val1': message_id,
                },
                ConditionExpression=Attr('message_id').eq(None)
            )
            return response['ResponseMetadata']['HTTPStatusCode'] == 200
        except ClientError as e:
            app_logger.warn(lambda: dict(
                message='Error setting message_id {} on job with transfer_id {}: {}'.format(
                    message_id, transfer_id, e.response['Error']['Code'])))
            return False

    @log_action(log_args=['min_content_length', 'max_content_length'])
    @dynamodb_retry_backoff()
    def get_next_ready(self, min_content_length: int = 0, max_content_length: int = sys.maxsize) -> Optional[
        MeshTransferJob]:
        sender_blocks = self._blocks_store.get_blocks(Blockables.SENDERS)

        if sender_blocks.is_blocked(Senders.MESH_ALL):
            return None

        response = self.table.query(
            IndexName=Indexes.Status_Priority_Created,
            KeyConditionExpression=Key('status').eq(MeshTransferStatus.Ready),
            ScanIndexForward=True,  # ascending
            Limit=128,
            # Inclusive filter
            FilterExpression=Attr('content_length').between(min_content_length, max_content_length),
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        queue = response.get('Items', [])
        for item in queue:
            job = self.get_by_transfer_id(item['transfer_id'])
            if job.status != MeshTransferStatus.Ready:
                continue

            if sender_blocks.is_blocked(Block.create_composite_block_group(Senders.MESH_WORKFLOW,
                                                                           job.workflow_id)):
                continue

            if sender_blocks.is_blocked(Block.create_composite_block_group(Senders.MESH_MAILBOX_TO,
                                                                           job.mailbox_to)):
                continue

            if job.mailbox_from:
                if sender_blocks.is_blocked(Block.create_composite_block_group(Senders.MESH_MAILBOX_FROM,
                                                                               job.mailbox_from)):
                    continue

            return job

        return None

    @log_action(log_level=LogLevel.INFO, log_args=['message_id', 'mailbox_from'])
    @dynamodb_retry_backoff()
    def mark_processing(self, transfer_id: str, mailbox_from: Optional[str] = None) -> bool:
        # If there's no job with that transfer_id, we can't do anything with it
        job = self.get_by_transfer_id(transfer_id)
        if not job:
            return False

        mark_processing = self._transition_status(transfer_id, MeshTransferStatus.Ready, MeshTransferStatus.Processing)
        if mark_processing and mailbox_from:
            self._set_mailbox_from(transfer_id, mailbox_from)

        return mark_processing

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id', 'mailbox_from'])
    @dynamodb_retry_backoff()
    def _set_mailbox_from(self, transfer_id: str, mailbox_from: str):
        try:
            response = self.table.update_item(
                Key=dict(transfer_id=transfer_id),
                UpdateExpression="SET mailbox_from = :val1",
                ExpressionAttributeValues={
                    ':val1': mailbox_from,
                }
            )
            return response['ResponseMetadata']['HTTPStatusCode'] == 200
        except ClientError as e:
            app_logger.warn(lambda: dict(
                message='Error setting mailbox_from {} on job with transfer_id {}: {}'.format(
                    mailbox_from, transfer_id, e.response['Error']['Code'])))
            return False


MeshUploadJobs = MeshUploadJobsStore()
