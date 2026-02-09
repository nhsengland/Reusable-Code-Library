import datetime
from typing import AbstractSet, Generator, Union

from boto3.dynamodb.conditions import Key

from nhs_reusable_code_library.resuable_codes.shared.aws import (
    ddb_query_paginate,
    ddb_query_paginated_count,
    dynamodb_retry_backoff,
)
from nhs_reusable_code_library.resuable_codes.shared.constants import JOBS, DigiTrialsPipelines
from nhs_reusable_code_library.resuable_codes.shared.logger import add_fields, log_action
from nhs_reusable_code_library.resuable_codes.shared.models import Block, DarsMessageJob, DarsMessageStatus, DarsSender
from nhs_reusable_code_library.resuable_codes.shared.store.base import BaseStore
from nhs_reusable_code_library.resuable_codes.shared.store.blocks import Blocks, BlockStore
from nhs_reusable_code_library.resuable_codes.shared.store.counters import Counters


class Indexes:
    StatusMessageType = "ix_status_message_type"
    StatusCreated = "ix_status_created"


class DarsMessageJobsStore(BaseStore):
    _table_name = 'dars_message_jobs'
    _blocks_store = None
    _keys = ['id']

    def __init__(self, table=None, blocks_store: BlockStore = None):
        super().__init__(DarsMessageJob, table, )
        self._blocks_store = blocks_store or Blocks

    @dynamodb_retry_backoff()
    def get_by_id(self, message_id: int) -> DarsMessageJob:
        return self.get(dict(id=message_id), consistent_read=True)

    @log_action()
    @dynamodb_retry_backoff()
    def get_by_status(self, status: str, message_type: str = None) -> Generator[DarsMessageJob, None, None]:
        if message_type:
            items = ddb_query_paginate(
                TableName=self.table.name,
                IndexName=Indexes.StatusMessageType,
                KeyConditionExpression=Key('message_type').eq(message_type) & Key('status').eq(status)
            )
        else:
            items = ddb_query_paginate(
                TableName=self.table.name,
                IndexName=Indexes.StatusCreated,
                KeyConditionExpression=Key('status').eq(status)
            )

        for item in items:
            item = self.get_by_id(item['id'])
            if item.status != status:
                continue
            yield DarsMessageJob(item)

    @log_action()
    @dynamodb_retry_backoff()
    def get_status_message_type_count(self, status: str, message_type: str) -> int:
        return ddb_query_paginated_count(
            TableName=self.table.name,
            IndexName=Indexes.StatusMessageType,
            KeyConditionExpression=Key('message_type').eq(message_type) & Key('status').eq(status)
        )

    def get_all_preparing(self, message_type: str = None) -> Generator[DarsMessageJob, None, None]:
        yield from self.get_by_status(DarsMessageStatus.Preparing, message_type)

    def get_all_ready(self, message_type: str = None) -> Generator[DarsMessageJob, None, None]:
        blocks = self._blocks_store.get_blocks(JOBS.DARS_MESSAGE)
        
        for ready in self.get_by_status(DarsMessageStatus.Ready, message_type):
            dataset_id = DigiTrialsPipelines.DIGITRIALS_OUT if ready.sender_id == DarsSender.DigiTrials else Block.ALL
            
            if not blocks.is_blocked(block_group = ready.message_type, dataset_id = dataset_id):
                yield ready

    @log_action(log_args=['message_id'])
    @dynamodb_retry_backoff()
    def mark_ready(self, message_id: int) -> bool:
        return self._transition_status(message_id, {DarsMessageStatus.Failed, DarsMessageStatus.Preparing}, DarsMessageStatus.Ready)

    @log_action(log_args=['message_id'])
    @dynamodb_retry_backoff()
    def mark_processing(self, message_id: int) -> bool:
        return self._transition_status(message_id, DarsMessageStatus.Ready, DarsMessageStatus.Processing)

    @log_action(log_args=['message_id'])
    @dynamodb_retry_backoff()
    def mark_failed(self, message_id: int) -> bool:
        return self._transition_status(message_id, DarsMessageStatus.Processing, DarsMessageStatus.Failed)

    @log_action(log_args=['message_id'])
    @dynamodb_retry_backoff()
    def mark_archived(self, message_id: int) -> bool:
        return self._transition_status(message_id, {DarsMessageStatus.Failed, DarsMessageStatus.Preparing}, DarsMessageStatus.Archived)

    def _transition_status(self, message_id: int, mandated_from_status: Union[str, AbstractSet[str]], to_status: str) -> bool:    
        args = dict(
            Key=dict(id=message_id),
            UpdateExpression="SET #status = :val1",
            ExpressionAttributeValues={
                ':val1': to_status,
            },
            ExpressionAttributeNames={
                '#status': 'status',
            }
        )

        if mandated_from_status:
            if isinstance(mandated_from_status, str):
                mandated_from_status = {mandated_from_status}

            value_names = []  # type: List[str]
            for index, expected_status in enumerate(mandated_from_status):
                value_name = ':val{}'.format(index + 2)
                value_names.append(value_name)
                args['ExpressionAttributeValues'][value_name] = expected_status

            args['ConditionExpression'] = '#status IN ({})'.format(','.join(value_names))

        response = self.table.update_item(**args)

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    @log_action(log_args=['message_id'])
    @dynamodb_retry_backoff()
    def mark_success(self, message_id: int) -> bool:
        response = self.table.update_item(
            Key=dict(id=message_id),
            UpdateExpression="SET #status = :val1, completed = :val2",
            ExpressionAttributeValues={
                ':val1': DarsMessageStatus.Success,
                ':val2': datetime.datetime.utcnow().isoformat(),
                ':val3': DarsMessageStatus.Processing
            },
            ExpressionAttributeNames={
                '#status': 'status',
            },
            ConditionExpression="#status = :val3"
        )

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    @log_action(log_args=['message_type', 'status', 'sender_id'])
    @dynamodb_retry_backoff()
    def create_dars_message_job(
            self, message_type: str, message_metadata, status: str = DarsMessageStatus.Ready, sender_id: str = None
    ) -> int:
        dars_message_job = DarsMessageJob(dict(
            id=Counters.get_next_dars_message_id(),
            status=status,
            sender_id=sender_id,
            message_type=message_type,
            message_metadata=message_metadata
        ))
        add_fields(job=dars_message_job, message_id=dars_message_job.id)

        response = self.table.put_item(Item=dars_message_job.to_primitive())
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        return dars_message_job.id


DarsMessageJobs = DarsMessageJobsStore()
