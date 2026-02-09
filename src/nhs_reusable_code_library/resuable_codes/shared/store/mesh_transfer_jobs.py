import datetime
from typing import Generator, Optional, Union, AbstractSet, List, Callable
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from nhs_reusable_code_library.resuable_codes.shared.aws import dynamodb_retry_backoff, ddb_query_batch_get_items, ddb_query_paginate, ddb_query_paginated_count
from nhs_reusable_code_library.resuable_codes.shared.logger import log_action, LogLevel, debug_fields, app_logger
from nhs_reusable_code_library.resuable_codes.shared.models import MeshTransferJob, MeshTransferStatus, MeshTransferJobPriority
from nhs_reusable_code_library.resuable_codes.shared.store import BlockStore, Blocks
from nhs_reusable_code_library.resuable_codes.shared.store.base import BaseStore, ModelNotFound


class Indexes:
    Status_WorkflowId = "ix_status_workflow_id"
    Status_Created = "ix_status_created"
    Status_Priority_Created = "ix_status_priority_created"
    WorkflowId = "ix_workflow_id"


class MeshTransferJobsStore(BaseStore[MeshTransferJob]):
    _keys = ['transfer_id']
    _blocks_store = None

    class SchemaMigrations:
        def from_0_to_1(self, item):
            item['mailbox_to'] = ''
            return True

        def from_1_to_2(self, item):
            item['priority'] = MeshTransferJobPriority.Normal
            return True

    def __init__(self, table=None, blocks_store: BlockStore = None):
        super().__init__(MeshTransferJob, table)
        self._blocks_store = blocks_store or Blocks

    def get_by_key(self, transfer_id: str) -> MeshTransferJob:
        return self.get(key=dict(transfer_id=transfer_id), consistent_read=True)

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def get_by_transfer_id(self, transfer_id: str) -> Optional[MeshTransferJob]:
        model = self.get({'transfer_id': transfer_id}, consistent_read=True)

        debug_fields(lambda: model.to_primitive())

        return model

    @log_action(log_level=LogLevel.INFO, log_args=['workflow_id', 'status', 'workflows_not_included'])
    @dynamodb_retry_backoff()
    def get_all_by_status(self, status: str, workflow_id: str = None,
                          is_workflow_allowed: Callable[[str, str], bool] = None) \
            -> Generator[MeshTransferJob, None, None]:
        if workflow_id:
            items = ddb_query_paginate(
                TableName=self.table.name,
                IndexName=Indexes.Status_WorkflowId,
                KeyConditionExpression=Key('workflow_id').eq(workflow_id) & Key('status').eq(status),
            )
        else:
            items = ddb_query_paginate(
                TableName=self.table.name,
                IndexName=Indexes.Status_Created,
                KeyConditionExpression=Key('status').eq(status),
            )

        for item in items:
            if is_workflow_allowed and "workflow_id" in item and "mailbox_to" in item and \
                    not is_workflow_allowed(item["workflow_id"], item["mailbox_to"]):
                continue

            item = self.get_by_transfer_id(item['transfer_id'])

            if item.status != status or \
                    (is_workflow_allowed and not is_workflow_allowed(item.workflow_id, item.mailbox_to)):
                continue
            yield MeshTransferJob(item)

    @log_action(log_level=LogLevel.INFO, log_args=['workflow_id'])
    @dynamodb_retry_backoff()
    def get_all_by_workflow_id(self, workflow_id: str) \
            -> Generator[MeshTransferJob, None, None]:
        kwargs = dict(
            TableName=self.table.name,
            IndexName=Indexes.WorkflowId,
            KeyConditionExpression=Key('workflow_id').eq(workflow_id)
        )

        items = ddb_query_paginate(**kwargs)

        for item in items:
            item = self.get_by_transfer_id(item['transfer_id'])
            yield MeshTransferJob(item)

    @log_action(log_level=LogLevel.INFO, log_args=['workflow_id'])
    @dynamodb_retry_backoff()
    def get_all_by_workflow_id_status(
            self, workflow_id: str, statuses: Union[List[str], str] = None
    ) -> Generator[MeshTransferJob, None, None]:

        if not statuses:
            statuses = MeshTransferStatus.except_archived()

        if type(statuses) == str:
            statuses = [statuses]

        for status in statuses:

            for item in ddb_query_batch_get_items(
                    key_field='transfer_id', TableName=self.table.name, IndexName=Indexes.Status_WorkflowId,
                    ExpressionAttributeNames={"#status": "status"},
                    KeyConditionExpression="#status = :ss and workflow_id = :wf",
                    ExpressionAttributeValues={":ss": {"S": status}, ":wf": {"S": workflow_id}}
            ):
                yield self.uplift_item(item)

    @log_action()
    @dynamodb_retry_backoff()
    def get_status_count(self, status: str, workflow_id: str = None) -> int:
        if workflow_id:
            count = ddb_query_paginated_count(
                TableName=self.table.name,
                IndexName=Indexes.Status_WorkflowId,
                KeyConditionExpression=Key('workflow_id').eq(workflow_id) & Key('status').eq(status),
            )
        else:
            count = ddb_query_paginated_count(
                TableName=self.table.name,
                IndexName=Indexes.Status_Created,
                KeyConditionExpression=Key('status').eq(status),
            )

        return count

    def get_all_pending(self, workflow_id: str = None) -> Generator[MeshTransferJob, None, None]:
        yield from self.get_all_by_status(MeshTransferStatus.Pending, workflow_id)

    def get_all_ready(self, workflow_id: str = None, is_workflow_allowed: Callable[[str, str], bool] = None) \
            -> Generator[MeshTransferJob, None, None]:
        yield from self.get_all_by_status(MeshTransferStatus.Ready, workflow_id, is_workflow_allowed)

    def get_all_processing(self, workflow_id: str = None) -> Generator[MeshTransferJob, None, None]:
        yield from self.get_all_by_status(MeshTransferStatus.Processing, workflow_id)

    def get_all_failed(self, workflow_id: str = None) -> Generator[MeshTransferJob, None, None]:
        yield from self.get_all_by_status(MeshTransferStatus.Failed, workflow_id)

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def mark_pending(self, transfer_id: str) -> bool:
        """This is a recovery transition - not part of the normal status workflow. It should be used when a job is
        stuck in a preparing state, e.g. the download has failed, or the container was killed before the download
        could be confirmed.

        Args:
            transfer_id (str): the transfer id of the job

        Returns:
            bool: True if successful; False if not
        """
        return self._transition_status(transfer_id, MeshTransferStatus.Preparing, MeshTransferStatus.Pending)

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def mark_preparing(self, transfer_id: str) -> bool:
        return self._transition_status(
            transfer_id,
            {
                MeshTransferStatus.Pending,  # new transfer
                MeshTransferStatus.Preparing,  # repeating a previously failed transfer
            },
            MeshTransferStatus.Preparing
        )

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def mark_ready(self, transfer_id: str) -> bool:
        return self._transition_status(transfer_id,
                                       {MeshTransferStatus.Preparing, MeshTransferStatus.Processing},
                                       MeshTransferStatus.Ready)

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def rollback_to_ready(self, transfer_id: str) -> bool:
        return self._transition_status(transfer_id, MeshTransferStatus.Processing, MeshTransferStatus.Ready)

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def mark_failed(self, transfer_id: str) -> bool:
        return self._transition_status(transfer_id, MeshTransferStatus.Processing, MeshTransferStatus.Failed)

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def mark_unprocessable(self, transfer_id: str) -> bool:
        return self._transition_status(transfer_id, MeshTransferStatus.Processing, MeshTransferStatus.Unprocessable)

    @log_action(log_level=LogLevel.INFO, log_args=['transfer_id'])
    @dynamodb_retry_backoff()
    def mark_archived(self, transfer_id: str) -> bool:
        return self._transition_status(transfer_id, frozenset(), MeshTransferStatus.Archived)

    @log_action(log_level=LogLevel.INFO, log_args=['workflow_id'])
    @dynamodb_retry_backoff()
    def delete_by_workflow_id_for_tests(self, workflow_id: str):
        try:
            to_delete = self.get_all_by_workflow_id(workflow_id)

            for job in to_delete:
                self.table.delete_item(
                    Key={
                        'transfer_id': job.transfer_id
                    }
                )
        except ModelNotFound:
            pass

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
