from typing import (
    Generator,
    Optional,
    List,
    Tuple
)

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from nhs_reusable_code_library.resuable_codes.shared.aws import dynamodb_retry_backoff, ddb_query_paginated_count
from nhs_reusable_code_library.resuable_codes.shared.logger import log_action, LogLevel, app_logger, add_fields
from nhs_reusable_code_library.resuable_codes.shared.models import UnsuspendJob, UnsuspendJobStatus
from nhs_reusable_code_library.resuable_codes.shared.store.base import BaseStore


class Indexes:
    Status = "ix_status"


class UnsuspendJobStore(BaseStore[UnsuspendJob]):
    _table_name = "unsuspend_jobs"
    _keys = ['source_pipeline_id', 'source_pipeline_table']

    def __init__(self, table=None):
        super().__init__(UnsuspendJob, table)

    def get_by_key(self, source_pipeline_id: int, source_pipeline_table: str,
                   consistent_read: bool = True) -> UnsuspendJob:
        return self.get(key=dict(source_pipeline_id=source_pipeline_id, source_pipeline_table=source_pipeline_table),
                        consistent_read=consistent_read)

    @log_action(
        log_level=LogLevel.INFO,
        log_args=[
            'source_pipeline_id',
            'source_pipeline_table',
            'target_watch_list',
        ])
    @dynamodb_retry_backoff()
    def create_unsuspend_jobs(
            self,
            source_pipeline_id: int,
            source_pipeline_table: str,
            target_watch_list: dict,
    ) -> Tuple[str, str]:
        """ Create a new Unsuspend job and return the source_pipeline_id, source_pipeline_table """
        if not source_pipeline_id:
            raise ValueError('source_pipeline_id should not be empty or zero')
        if not source_pipeline_table:
            raise ValueError('source_pipeline_table should not be empty')
        if not target_watch_list:
            raise ValueError('target_watch_list should not be empty')
        unsuspend = UnsuspendJob(dict(
            source_pipeline_id=source_pipeline_id,
            source_pipeline_table=source_pipeline_table,
            target_watch_list=target_watch_list,
            status=UnsuspendJobStatus.Processing,
        ))
        response = self.table.put_item(Item=unsuspend.to_primitive())

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        return unsuspend.source_pipeline_id, unsuspend.source_pipeline_table

    @log_action(log_level=LogLevel.INFO, log_args=['source_pipeline_id', 'source_pipeline_table'])
    @dynamodb_retry_backoff()
    def get_by_source_pipeline_id_and_source_pipeline_table(self, source_pipeline_id: int,
                                                            source_pipeline_table: str) -> UnsuspendJob:
        return self.get_by_key(source_pipeline_id, source_pipeline_table)

    @log_action(log_level=LogLevel.INFO, log_args=['source_pipeline_id', 'source_pipeline_table', 'status'])
    @dynamodb_retry_backoff()
    def get_all_by_status(self, status: str) -> Optional[Generator[UnsuspendJob, None, None]]:
        response = self.table.query(
            IndexName=Indexes.Status,
            KeyConditionExpression=Key('status').eq(status)
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        items = response.get('Items', [])

        for item in items:
            item = self.get_by_source_pipeline_id_and_source_pipeline_table(item['source_pipeline_id'],
                                                                            item['source_pipeline_table'])
            unsuspend = UnsuspendJob(item)
            if unsuspend.status != status:
                continue
            yield unsuspend

    @log_action(log_level=LogLevel.INFO, log_args=['source_pipeline_id', 'source_pipeline_table'])
    @dynamodb_retry_backoff()
    def get_all_processing(self) -> Optional[List[UnsuspendJob]]:
        return self.get_all_by_status(UnsuspendJobStatus.Processing)

    @log_action(log_level=LogLevel.INFO, log_args=['source_pipeline_id', 'source_pipeline_table'])
    @dynamodb_retry_backoff()
    def get_all_success(self) -> Optional[List[UnsuspendJob]]:
        return self.get_all_by_status(UnsuspendJobStatus.Success)

    @log_action(log_level=LogLevel.INFO, log_args=['source_pipeline_id', 'source_pipeline_table'])
    @dynamodb_retry_backoff()
    def get_all_failed(self) -> Optional[List[UnsuspendJob]]:
        return self.get_all_by_status(UnsuspendJobStatus.Failed)

    @log_action(log_level=LogLevel.INFO, log_args=['source_pipeline_id', 'source_pipeline_table'])
    @dynamodb_retry_backoff()
    def mark_success(self, source_pipeline_id: int, source_pipeline_table: str) -> bool:
        try:

            response = self.table.update_item(
                Key=dict(source_pipeline_id=source_pipeline_id, source_pipeline_table=source_pipeline_table),
                UpdateExpression="SET #status = :val2",
                ExpressionAttributeValues={
                    ':val1': UnsuspendJobStatus.Processing,
                    ':val2': UnsuspendJobStatus.Success
                },
                ExpressionAttributeNames={
                    '#status': 'status'
                },
                ConditionExpression='#status = :val1'
            )
            assert response['ResponseMetadata']['HTTPStatusCode'] == 200
            return True
        except ClientError as e:
            app_logger.exception("Error occurred marking as success: {}".format(e.response['Error']['Code']))
            return False

    @log_action(log_level=LogLevel.INFO, log_args=['source_pipeline_id', 'source_pipeline_table'])
    @dynamodb_retry_backoff()
    def mark_failed(self, source_pipeline_id: int, source_pipeline_table: str) -> bool:
        try:
            response = self.table.update_item(
                Key=dict(source_pipeline_id=source_pipeline_id, source_pipeline_table=source_pipeline_table),
                UpdateExpression="SET #status = :val2",
                ExpressionAttributeValues={
                    ':val1': UnsuspendJobStatus.Processing,
                    ':val2': UnsuspendJobStatus.Failed
                },
                ExpressionAttributeNames={
                    '#status': 'status'
                },
                ConditionExpression='#status = :val1'
            )
            assert response['ResponseMetadata']['HTTPStatusCode'] == 200
            return True
        except ClientError as e:
            app_logger.exception("Error occurred marking as success: {}".format(e.response['Error']['Code']))
            return False

    @log_action(log_level=LogLevel.INFO, log_args=['source_pipeline_id', 'source_pipeline_table'])
    @dynamodb_retry_backoff()
    def update_target_watch_list_with_ids(
            self, source_pipeline_id: int, source_pipeline_table: str, new_target_watch_list: dict):
        unsuspend_job = self.get_by_source_pipeline_id_and_source_pipeline_table(
            source_pipeline_id, source_pipeline_table)

        for table, watch_list_data in new_target_watch_list.items():
            if isinstance(watch_list_data, list):
                new_target_watch_list[table] = set(watch_list_data + unsuspend_job.target_watch_list.get(table, []))

            elif isinstance(watch_list_data, dict):
                combined_watch_list_data = dict()
                original_watch_list_data = unsuspend_job.target_watch_list.get(table, {})
                for key in set(watch_list_data) | set(original_watch_list_data):
                    combined_watch_list_data[key] = watch_list_data.get(key, []) + original_watch_list_data.get(key, [])
                new_target_watch_list[table] = combined_watch_list_data

        for table, watch_list_data in unsuspend_job.target_watch_list.items():
            if table not in new_target_watch_list:
                new_target_watch_list[table] = watch_list_data
        add_fields(new_target_watch_list=new_target_watch_list)

        try:
            response = self.table.update_item(
                Key=dict(source_pipeline_id=source_pipeline_id, source_pipeline_table=source_pipeline_table),
                UpdateExpression="SET #target_watch_list = :val1, #status = :val2",
                ExpressionAttributeValues={
                    ':val1': new_target_watch_list,
                    ':val2': UnsuspendJobStatus.Processing
                },
                ExpressionAttributeNames={
                    '#target_watch_list': 'target_watch_list',
                    '#status': 'status'
                }
            )
            assert response['ResponseMetadata']['HTTPStatusCode'] == 200
            return True
        except ClientError as e:
            app_logger.exception("Error occurred updating target watch list: {}".format(e.response['Error']['Code']))
            return False

    def get_status_count(self, status):
        return ddb_query_paginated_count(
            TableName=self.table.name,
            IndexName=Indexes.Status,
            KeyConditionExpression=Key('status').eq(status)
        )


UnsuspendJobs = UnsuspendJobStore()
