import datetime
from typing import Generator, Union, Mapping, List, Optional

from boto3.dynamodb.conditions import Key

from src.nhs_reusable_code_library.resuable_codes.shared.aws import dynamodb_retry_backoff, ddb_query_paginated_count
from src.nhs_reusable_code_library.resuable_codes.shared.common import run_in_executor
from src.nhs_reusable_code_library.resuable_codes.shared.constants import JOBS
from src.nhs_reusable_code_library.resuable_codes.shared.logger import LogLevel, log_action, add_fields, debug_fields
from src.nhs_reusable_code_library.resuable_codes.shared.models import ExtractRequest, PipelineStatus, ExtractResult, Run
from src.nhs_reusable_code_library.resuable_codes.shared.store.base import BaseStore
from src.nhs_reusable_code_library.resuable_codes.shared.store.counters import Counters
from src.nhs_reusable_code_library.resuable_codes.shared.store.blocks import Blocks, BlockStore


class Indexes:
    Status = "ix_extracts_status_received"
    TypeStatus = "ix_extracts_type_status"


class ExtractRequestsStore(BaseStore[ExtractRequest]):
    _table_name = 'extract_requests'
    _blocks_store = None
    _keys = ['extract_id']

    def __init__(self, table=None, blocks_store: BlockStore = None):
        super().__init__(ExtractRequest, table)
        self._blocks_store = blocks_store or Blocks

    def get_by_key(self, extract_id: int, consistent_read: bool = False) -> ExtractRequest:

        return self.get(key=dict(extract_id=extract_id), consistent_read=consistent_read)

    async def async_get_by_id(self, extract_id: int, consistent_read: bool = False):
        return await run_in_executor(self.get_by_key, extract_id, consistent_read=consistent_read)

    @log_action(log_level=LogLevel.INFO)
    @dynamodb_retry_backoff()
    def store_extract_request(self, extract_request: ExtractRequest) -> ExtractRequest:
        """
        Store the requested

        Args:
            extract_request: the request to store
        """
        extract_request.extract_id = Counters.get_next_extract_id()
        add_fields(extract_id=extract_request.extract_id)
        add_fields(extract_request=extract_request)

        response = self.table.put_item(
            Item=extract_request.to_primitive(),
            ConditionExpression="attribute_not_exists(extract_id)"
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        return extract_request

    @log_action()
    @dynamodb_retry_backoff()
    def get_by_status(self, status: str, *, exclusive_start_key: Optional[str] = None) \
            -> Generator[ExtractRequest, None, None]:
        response = self.table.query(
            IndexName=Indexes.Status,
            KeyConditionExpression=Key('status').eq(status),
            ExclusiveStartKey=exclusive_start_key
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        items = response.get('Items', [])

        for item in items:
            item = self.get_by_id(item['extract_id'])
            yield item

        last_evaluated_key = response.get('LastEvaluatedKey')
        if last_evaluated_key:
            for item in self.get_by_status(status, exclusive_start_key=last_evaluated_key):
                yield item

    @log_action()
    @dynamodb_retry_backoff()
    def get_by_status(self, status: str, *other_statuses: str) -> Generator[ExtractRequest, None, None]:
        def query_status(status_to_query: str) -> Generator[ExtractRequest, None, None]:
            response = self.table.query(
                IndexName=Indexes.Status,
                KeyConditionExpression=Key('status').eq(status_to_query),
            )

            assert 200 == response['ResponseMetadata']['HTTPStatusCode']

            items = response.get('Items', [])
            items.sort(key=lambda s: s.get('extract_id'))

            for item in items:
                item = self.get_by_id(item['extract_id'], consistent_read=True)
                if item.status != status_to_query:
                    continue
                yield item

        yield from query_status(status)
        for other_status in other_statuses:
            yield from query_status(other_status)

    @log_action()
    @dynamodb_retry_backoff()
    def get_by_type(self, extract_type: str, exclusive_start_key: str = None) -> Generator[ExtractRequest, None, None]:

        kwargs = dict(
            IndexName=Indexes.TypeStatus,
            KeyConditionExpression=Key('type').eq(extract_type),
        )

        if exclusive_start_key:
            kwargs['ExclusiveStartKey'] = exclusive_start_key

        response = self.table.query(**kwargs)

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        items = response.get('Items', [])

        for item in items:
            item = self.get_by_id(item['extract_id'])
            yield item

        last_evaluated_key = response.get('LastEvaluatedKey')
        if last_evaluated_key:
            for item in self.get_by_type(extract_type, exclusive_start_key=last_evaluated_key):
                yield item

    @log_action()
    @dynamodb_retry_backoff()
    def get_type_status_count(self, extract_type: str, status: str) -> int:
        """ Get paginated extract request count based on type and status

        Args:
            extract_type (str): extract type to count
            status (str): status to count

        Returns:
            int: count
        """
        return ddb_query_paginated_count(
            TableName=self.table.name,
            IndexName=Indexes.TypeStatus,
            KeyConditionExpression=Key('type').eq(extract_type) & Key('status').eq(status)
        )

    ## @log_action()
    ## @dynamodb_retry_backoff()
    ## async def async_get_by_status(self, status: str, *other_statuses: str) -> Generator[ExtractRequest, None, None]:
    ##     async def query_status(status_to_query: str) -> Generator[ExtractRequest, None, None]:
    ##         response = await run_in_executor(
    ##             self.table.query,
    ##             IndexName=Indexes.Status,
    ##             KeyConditionExpression=Key('status').eq(status_to_query)
    ##         )

    ##         assert 200 == response['ResponseMetadata']['HTTPStatusCode']

    ##         items = response.get('Items', [])
    ##         items.sort(key=lambda s: s.get('extract_id'))

    ##         for item in items:
    ##             item = await self.async_get_by_id(item['extract_id'], consistent_read=True)
    ##             if item.status not in {status, *other_statuses}:
    ##                 continue
    ##             yield item

    ##     async for job in query_status(status):
    ##         yield job

    ##     for other_status in other_statuses:
    ##         async for job in query_status(other_status):
    ##             yield job

    @log_action(log_level=LogLevel.INFO, log_args=['extract_id'])
    @dynamodb_retry_backoff()
    def update_pipeline_result(self, extract_id: int, pipeline_result: ExtractResult):
        add_fields(result=pipeline_result)
        response = self.table.update_item(
            Key=dict(extract_id=extract_id),
            UpdateExpression="SET #result = :val1",
            ExpressionAttributeValues={
                ':val1': pipeline_result.to_primitive(),
            },
            ExpressionAttributeNames={
                # reserved names
                '#result': 'result',
            },
        )

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    def to_pipeline_result(self, result_json: Mapping):
        return ExtractResult(result_json)

    @log_action(log_level=LogLevel.INFO, log_args=['extract_id'])
    @dynamodb_retry_backoff()
    def update_finished(self, extract_id: int, finished: datetime.datetime):
        response = self.table.update_item(
            Key=dict(extract_id=extract_id),
            AttributeUpdates={
                'finished': {
                    'Value': ExtractRequest.finished.to_primitive(finished),
                    'Action': 'PUT'
                }
            }
        )

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    @log_action(log_level=LogLevel.INFO, log_args=['extract_id'])
    def get_by_id(self, extract_id: int, consistent_read: bool = False) -> ExtractRequest:
        model = self.get({'extract_id': extract_id}, consistent_read=consistent_read)

        debug_fields(lambda: model.to_primitive())

        return model

    @log_action()
    def get_non_blocked_pending(self) -> Generator[ExtractRequest, None, None]:

        blocks = self._blocks_store.get_blocks(JOBS.EXTRACTING)

        for pending in self.get_pending():

            if blocks.is_blocked(pending.get_block_group(), pending.extract_id, pending.received):
                continue

            yield pending

    def get_pending(self) -> Generator[ExtractRequest, None, None]:
        yield from self.get_by_status(PipelineStatus.Pending)

    def get_running(self) -> Generator[ExtractRequest, None, None]:
        yield from self.get_by_status(PipelineStatus.Running)

    def get_active(self) -> Generator[ExtractRequest, None, None]:
        """
        Get all extracts that are in a state where we expect the pipeline to continue execution.

        This is distinct from get_running, as that method will _only_ retrieve submissions in state 'running'.

        Yields:
            All extracts that are in either a 'running' or 'processed' state
        """
        yield from self.get_by_status(PipelineStatus.Processed, PipelineStatus.Running)

    ## async def async_get_active(self) -> Generator[ExtractRequest, None, None]:
    ##     """
    ##     Get all submissions that are in a state where we expect the pipeline to continue execution.

    ##     This is distinct from get_running, as that method will _only_ retrieve submissions in state 'running'.

    ##     Yields:
    ##         All submissions that are in either a 'running' or 'processed' state
    ##     """
    ##     async for extract in self.async_get_by_status(PipelineStatus.Processed, PipelineStatus.Running):
    ##         yield extract

    @log_action()
    @dynamodb_retry_backoff()
    def get_status_count(self, status: str) -> int:
        return ddb_query_paginated_count(
            TableName=self.table.name,
            IndexName=Indexes.Status,
            KeyConditionExpression=Key('status').eq(status)
        )

    @log_action(action='update_status', log_level=LogLevel.INFO, log_args=['extract_id'])
    @dynamodb_retry_backoff()
    def mark_scheduling(self, extract_id: int, raise_if_condition_fails: bool = True) -> bool:
        target_status = PipelineStatus.Scheduling
        add_fields(status=target_status)

        return self.update_status(
            extract_id=extract_id,
            status=target_status,
            check_existing_status_is=PipelineStatus.Pending,
            raise_if_condition_fails=raise_if_condition_fails,
        )

    @log_action(action='update_status', log_level=LogLevel.INFO, log_args=['extract_id'])
    @dynamodb_retry_backoff()
    def mark_running(self, extract_id: int, run: Run, raise_if_condition_fails: bool = True) -> bool:
        target_status = PipelineStatus.Running

        add_fields(**run.to_primitive())
        add_fields(status=target_status)

        try:
            response = self.table.update_item(
                Key=dict(extract_id=extract_id),
                UpdateExpression="SET #status = :val1, runs = list_append(runs, :val2)",
                ExpressionAttributeValues={
                    ':val1': target_status,
                    ':val2': [run.to_primitive()],
                    ':val3': PipelineStatus.Scheduling
                },
                ExpressionAttributeNames={
                    # reserved names
                    '#status': 'status',
                },
                ConditionExpression='#status = :val3'
            )

        except Exception as e:
            if hasattr(e, 'response') and getattr(e, 'response')['Error']['Code'] == 'ConditionalCheckFailedException':
                if raise_if_condition_fails:
                    raise
                return False
            raise

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    @log_action(action='update_status', log_level=LogLevel.INFO, log_args=['extract_id'])
    @dynamodb_retry_backoff()
    def mark_processed(self, extract_id: int, result: ExtractResult = None, raise_if_condition_fails: bool = True) \
            -> bool:
        target_status = PipelineStatus.Processed
        result_val = result.to_primitive() if result else None
        add_fields(result=result_val)
        add_fields(status=target_status)

        try:
            response = self.table.update_item(
                Key=dict(extract_id=extract_id),
                UpdateExpression="SET #status = :val1, #result = :val2",
                ExpressionAttributeValues={
                    ':val1': target_status,
                    ':val2': result_val,
                    ':val3': PipelineStatus.Running
                },
                ExpressionAttributeNames={
                    # reserved names
                    '#status': 'status',
                    '#result': 'result',
                },
                ConditionExpression='#status = :val3'
            )

        except Exception as e:
            if hasattr(e, 'response') and getattr(e, 'response')['Error']['Code'] == 'ConditionalCheckFailedException':
                if raise_if_condition_fails:
                    raise
                return False
            raise

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    @log_action(action='update_status', log_level=LogLevel.INFO, log_args=['extract_id', 'failure_reason'])
    @dynamodb_retry_backoff()
    def mark_failed(self, extract_id: int, failure_reason: str = None, raise_if_condition_fails: bool = True) -> bool:
        target_status = PipelineStatus.Failed
        add_fields(status=target_status)

        try:
            response = self.table.update_item(
                Key=dict(extract_id=extract_id),
                UpdateExpression="SET #status = :val1, failure_reason = :val2",
                ExpressionAttributeValues={
                    ':val1': target_status,
                    ':val2': failure_reason or 'not specified',
                    ':val3': PipelineStatus.Running,
                    ':val4': PipelineStatus.Pending,
                    ':val5': PipelineStatus.Scheduling
                },
                ExpressionAttributeNames={
                    # reserved names
                    '#status': 'status',
                },
                ConditionExpression='(#status = :val3) OR (#status = :val4) OR (#status = :val5)'
            )

        except Exception as e:
            if hasattr(e, 'response') and getattr(e, 'response')['Error']['Code'] == 'ConditionalCheckFailedException':
                if raise_if_condition_fails:
                    raise
                return False
            raise

        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    @log_action(action='update_status', log_level=LogLevel.INFO, log_args=['extract_id'])
    @dynamodb_retry_backoff()
    def mark_archived(self, extract_id: int) -> bool:
        target_status = PipelineStatus.Archived
        add_fields(status=target_status)
        return self.update_status(
            extract_id=extract_id,
            status=target_status,
        )

    @log_action(log_level=LogLevel.INFO, log_args=['extract_id', 'status', 'check_existing_status_is'])
    @dynamodb_retry_backoff()
    def update_status(
            self, extract_id: int, status: str,
            check_existing_status_is: Union[str, None] = None,
            raise_if_condition_fails=True
    ) -> bool:

        args = dict(
            Key=dict(extract_id=extract_id),
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
            if isinstance(check_existing_status_is, str):
                check_existing_status_is = {check_existing_status_is}

            value_names = []  # type: List[str]
            for index, expected_status in enumerate(check_existing_status_is):
                value_name = ':val{}'.format(index + 4)
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


ExtractRequests = ExtractRequestsStore()


def create_extract_request(
        sender: str, consumer: str, extract_type: str, request: dict, priority: int = 10, test_scope: str = None,
        status: PipelineStatus = PipelineStatus.Pending
) -> ExtractRequest:
    return ExtractRequest(dict(
        received=datetime.datetime.utcnow(),
        extract_id=-1,
        priority=priority,
        status=status,
        sender=sender,
        consumer=consumer,
        request=request,
        type=extract_type,
        test_scope=test_scope
    ))
