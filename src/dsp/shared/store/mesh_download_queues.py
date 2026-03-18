import datetime
from hashlib import sha1
from time import sleep
from typing import Optional, Dict, Callable

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from dsp.shared.aws import dynamodb_retry_backoff, ddb_query_paginated_count
from dsp.shared.constants import Blockables
from dsp.shared.logger import log_action, LogLevel, app_logger, action_logging, add_temporary_global_fields, add_fields
from dsp.shared.models import MeshTransferJob, MeshTransferStatus, MeshMetadata
from dsp.shared.store.blocks import BlockStore, Blocks
from dsp.shared.store.mesh_transfer_jobs import MeshTransferJobsStore, Indexes as BaseIndexes


class Indexes(BaseIndexes):
    MessageId = "ix_message_id"


class JobAlreadyExistsException(Exception):
    def __init__(self, message_id):
        super(JobAlreadyExistsException, self).__init__()
        self.message_id = message_id

    def __str__(self):
        return 'JobAlreadyExistsException[{}]'.format(self.message_id)


class MeshDownloadQueuesStore(MeshTransferJobsStore):
    _table_name = 'mesh_download_queues'

    @log_action(log_level=LogLevel.INFO, log_args=['workflow_id', 'message_id', 's3_location', 'mesh_metadata'])
    @dynamodb_retry_backoff()
    def create_mesh_download_job(self, workflow_id: str, message_id: str, s3_location: str,
                                 mesh_metadata: MeshMetadata, mailbox_to: str,
                                 mailbox_from: Optional[str] = None) -> str:
        """ Create a new mesh download job in the dynamodb table and return the unique ID given """

        if not message_id:
            raise ValueError("message_id cannot be empty")

        transfer_id = sha1(message_id.encode('utf-8')).hexdigest()
        mesh_transfer_job = MeshTransferJob(dict(
            transfer_id=transfer_id,
            message_id=message_id,
            created=datetime.datetime.utcnow(),
            status=MeshTransferStatus.Pending,
            workflow_id=workflow_id,
            s3_locations=[s3_location],
            mesh_metadata=mesh_metadata,
            mailbox_from=mailbox_from,
            mailbox_to=mailbox_to
        ))

        add_fields(job=mesh_transfer_job.to_primitive())

        try:
            response = self.table.put_item(
                Item=mesh_transfer_job.to_primitive(),
                ConditionExpression="attribute_not_exists(transfer_id)",
            )

            assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        except ClientError as error:
            # getting a duplicate transfer_id could occur in one of two ways:
            # 1. attempting to create a download job with the same message_id twice
            # 2. hashing the message_id to get the transfer_id, and creating a duplicate transfer_id
            if error.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise JobAlreadyExistsException(mesh_transfer_job.transfer_id)
            raise error

        return mesh_transfer_job.transfer_id

    @log_action(log_args=['workflow_id'])
    @dynamodb_retry_backoff()
    def get_next_ready_for_workflow(self, workflow_id: str) -> Optional[MeshTransferJob]:
        """Get the ready job at the top of the queue. We perform some checks in here to ensure that there aren't any
        existing processing/failed jobs.  It also checks the pending/preparing/ready jobs to ensure that, when sorted,
        the one at the top of the queue is ready - otherwise we'd return the jobs out of order.

        Args:
            workflow_id (str): the workflow id for the jobs we want to process
            block_on_failed (bool): whether to stop processing when there's a failed MeshDownloadJob

        Returns:
            Optional[MeshTransferJob]: the next job, if it's ready
        """
        receiver_blocks = self._blocks_store.get_blocks(Blockables.RECEIVERS)
        if receiver_blocks.is_blocked(workflow_id):
            return None

        processing_items = self.table.query(
            IndexName=Indexes.Status_WorkflowId,
            KeyConditionExpression=Key('workflow_id').eq(workflow_id) & Key('status').eq(MeshTransferStatus.Processing),
        )
        if processing_items['Items']:
            add_fields(
                message='a job is already processing. not starting another',
                transfer_id=processing_items['Items'][0]['transfer_id']
            )
            return None

        failed_items = self.table.query(
            IndexName=Indexes.Status_WorkflowId,
            KeyConditionExpression=Key('workflow_id').eq(workflow_id) & Key('status').eq(MeshTransferStatus.Failed),
        )
        if failed_items['Items']:
            add_fields(
                message='a job has failed. not starting another',
                transfer_id=failed_items['Items'][0]['transfer_id']
            )
            return None

        pending = self.table.query(
            IndexName=Indexes.Status_WorkflowId,
            KeyConditionExpression=Key('workflow_id').eq(workflow_id) & Key('status').eq(MeshTransferStatus.Pending),
        )
        preparing = self.table.query(
            IndexName=Indexes.Status_WorkflowId,
            KeyConditionExpression=Key('workflow_id').eq(workflow_id) & Key('status').eq(
                MeshTransferStatus.Preparing),
        )
        ready = self.table.query(
            IndexName=Indexes.Status_WorkflowId,
            KeyConditionExpression=Key('workflow_id').eq(workflow_id) & Key('status').eq(MeshTransferStatus.Ready),
        )
        queue = pending['Items'] + preparing['Items'] + ready['Items']
        # Sort by the sortable MESH message id. If the first one is ready, process it - otherwise we can't process
        # anything as we'll end up processing out of order
        add_fields(
            pending_count=len(pending['Items']),
            preparing_count=len(preparing['Items']),
            ready_count=len(ready['Items'])
        )
        queue.sort(key=lambda i: i['message_id'])
        if queue and queue[0]['status'] == MeshTransferStatus.Ready:
            job = self.get_by_transfer_id(queue[0]['transfer_id'])
            if job.status != MeshTransferStatus.Ready:
                add_fields(message='transfer at top of queue {} is no longer ready'.format(job.transfer_id))
                return None
            add_fields(transfer_id=job.transfer_id)
            return job

        return None

    @log_action()
    @dynamodb_retry_backoff()
    def get_workflow_id_count(self, workflow_id: str) -> int:
        return ddb_query_paginated_count(
            TableName=self.table.name,
            IndexName=Indexes.WorkflowId,
            KeyConditionExpression=Key('workflow_id').eq(workflow_id)
        )

    @log_action(log_level=LogLevel.INFO, log_args=['message_id'])
    @dynamodb_retry_backoff()
    def get_transfer_id_by_message_id(self, message_id: str) -> Optional[str]:
        response = self.table.query(
            IndexName=Indexes.MessageId,
            KeyConditionExpression=Key('message_id').eq(message_id),
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        items = response.get('Items', [])

        if len(items) == 1:
            return items[0]['transfer_id']

        if not items:
            return None

        raise Exception('Invalid number of records with message ID {}'.format(message_id))

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def mark_success(self, transfer_id: str) -> bool:
        return self._transition_status(transfer_id, MeshTransferStatus.Processing, MeshTransferStatus.Success)

    @log_action(log_level=LogLevel.INFO, log_args=['message_id'])
    @dynamodb_retry_backoff()
    def mark_processing(self, transfer_id: str) -> bool:
        """Mark the job as processing. This function performs some checks to ensure that the job the caller is trying to
        run is actually at the head of the queue and is available to be processed.

        Args:
            transfer_id (str): the transfer id of the job

        Returns:
            bool: True if successful; False if not
        """
        # If there's no job with that transfer_id id, we can't do anything with it
        job = self.get_by_transfer_id(transfer_id)
        if not job:
            add_fields(mark_processing='rejected', failed_reason="cannot mark processing job not found")
            return False

        # If this transfer id isn't the next one to be processed, we can't offer it up to be processed otherwise we
        # will be returning the jobs out of order.
        job = self.get_next_ready_for_workflow(job.workflow_id)
        if not job or job.transfer_id != transfer_id:
            add_fields(
                mark_processing='rejected',
                failed_reason=(
                    f"cannot mark processing another job {job.transfer_id} is ahead of this job {transfer_id}"
                    if job
                    else f"cannot mark processing {transfer_id} queue may be blocked or another job is processing"
                )
            )
            return False

        # We have the next ready job... try to transition. If it fails, another process has just pipped us to it.
        # That's fine. The caller will just have to try again later.
        return self._transition_status(transfer_id, MeshTransferStatus.Ready, MeshTransferStatus.Processing)

    @log_action(log_level=LogLevel.INFO, log_args=['message_id'])
    @dynamodb_retry_backoff()
    def mark_processing_unordered(self, transfer_id: str) -> bool:
        """Mark the job as processing. This function does not check its the next in the queue
        This should only be used for mesh_to_pos

        Args:
            transfer_id (str): the transfer id of the job

        Returns:
            bool: True if successful; False if not
        """
        # If there's no job with that transfer_id id, we can't do anything with it
        job = self.get_by_transfer_id(transfer_id)
        if not job:
            add_fields(mark_processing='rejected', failed_reason="cannot mark processing job not found")
            return False

        # We have the next ready job... try to transition. If it fails, another process has just pipped us to it.
        # That's fine. The caller will just have to try again later.
        return self._transition_status(transfer_id, MeshTransferStatus.Ready, MeshTransferStatus.Processing)

    @log_action(log_level=LogLevel.INFO, log_args=['workflow_id'])
    def monitor(self, workflow_id: str, poll_frequency: int, fn: Callable, *args, **kwargs) -> None:
        """Monitor the mesh download queues for a given workflow id and callback when one is available.
        """
        while True:
            with action_logging(message='loop'):  # To generate a new task_uuid for each loop to aid log tracing
                try:
                    job = self.get_next_ready_for_workflow(workflow_id)

                    if job:
                        fn(self, *args, **kwargs)
                except (MemoryError, SyntaxError, KeyboardInterrupt, NameError, AttributeError, SystemExit):
                    app_logger.exception()
                    raise

                except:  # pylint:disable=bare-except
                    app_logger.exception()

                finally:
                    sleep(poll_frequency)

    @log_action(log_level=LogLevel.INFO, log_args=['workflows'])
    def process_jobs(self, workflows: Dict[str, bool], poll_frequency: int, fn: Callable, *args, **kwargs):
        app_logger.info({
            'message': 'Starting MeshDownloadQueues.process_jobs loop',
            'workflows': workflows,
            'poll_frequency': poll_frequency,
        })

        while True:
            workflow_processed_job = {}
            with action_logging(message='loop'):  # To generate a new task_uuid for each loop to aid log tracing
                try:
                    for workflow_id, blocking in workflows.items():
                        workflow_processed_job[workflow_id] = self.process_job(workflow_id, blocking, fn, *args,
                                                                               **kwargs)

                except (MemoryError, SyntaxError, KeyboardInterrupt, NameError, AttributeError, SystemExit):
                    app_logger.exception()
                    raise

                except:  # pylint:disable=bare-except
                    app_logger.exception()

                finally:
                    if not any(workflow_processed_job.values()):
                        sleep(poll_frequency)

    @log_action(log_args=['workflow_id', 'blocking'])
    def process_job(self, workflow_id: str, blocking: bool, fn: Callable, *args, **kwargs) -> bool:
        job = self.get_next_ready_for_workflow(workflow_id)
        return self._process_job(job, blocking, fn, *args, **kwargs)

    def persist_local_id(self, transfer_id: str, query_id: str) -> Optional[MeshTransferJob]:
        # If there's no job with that transfer_id id, we can't do anything with it
        job = self.get_by_transfer_id(transfer_id)
        if not job:
            return

        job.mesh_metadata.local_id = query_id

        self.put(job)
        return job

    @log_action()
    def process_ready_jobs(self, poll_frequency: int, is_workflow_allowed: Callable[[str, str], bool], fn: Callable,
                           *args, **kwargs):
        app_logger.info({
            'message': 'Starting MeshDownloadQueues.process_ready_jobs loop',
            'poll_frequency': poll_frequency,
        })

        while True:
            with action_logging(message='loop'):  # To generate a new task_uuid for each loop to aid log tracing
                job: MeshTransferJob
                for job in self.get_all_ready(None, is_workflow_allowed):
                    with action_logging(message='loop'):  # To generate a new task_uuid for each loop to aid log tracing
                        try:
                            self._process_job(job, False, fn, *args, **kwargs)
                        except (MemoryError, SyntaxError, KeyboardInterrupt, NameError, AttributeError, SystemExit):
                            add_fields(loop_error='UnhandledException')
                            app_logger.exception()
                            raise
                        except:  # pylint:disable=bare-except
                            add_fields(loop_error='HandledException')
                            app_logger.exception()

                        add_fields(loop_status='Finished')
                sleep(poll_frequency)

    @log_action(log_args=['job', 'blocking'])
    def _process_job(self, job: MeshTransferJob, blocking: bool, fn: Callable, *args, **kwargs) -> bool:

        if not job:
            # Nothing to do
            return False

        with add_temporary_global_fields(job=job):

            if not self.mark_processing(job.transfer_id):
                # Another process has beaten us to it
                app_logger.warn(lambda: dict(transfer_id=job.transfer_id, message='unable to mark processing'))
                return False

            try:
                job = self.get_by_key(job.transfer_id)
                return_value = fn(job, *args, **kwargs)
                if return_value:
                    self.mark_success(job.transfer_id)
                    app_logger.info({
                        'message': 'Marked success',
                        'transfer_id': job.transfer_id,
                    })
                else:
                    app_logger.info({
                        'message': 'Mark ready for another attempt',
                        'transfer_id': job.transfer_id,
                    })
                    self.mark_ready(job.transfer_id)

                return return_value

            except:  # pylint: disable=bare-except

                app_logger.exception(lambda: dict(transfer_id=job.transfer_id))
                if blocking:
                    self.mark_failed(job.transfer_id)
                    app_logger.info({
                        'message': 'Marked failed due to exceptions',
                        'transfer_id': job.transfer_id,
                    })
                else:
                    self.mark_unprocessable(job.transfer_id)
                    app_logger.info({
                        'message': 'Marked unprocessable due to exceptions',
                        'transfer_id': job.transfer_id,
                    })

                raise


MeshDownloadQueues = MeshDownloadQueuesStore()
