import traceback
from datetime import datetime
from typing import Generator, Union, AbstractSet, List, Optional, Dict, Callable, Any, MutableMapping

from boto3.dynamodb.conditions import Key

from src.nhs_reusable_code_library.resuable_codes.shared.aws import dynamodb_retry_backoff, ddb_query_paginated_count
from src.nhs_reusable_code_library.resuable_codes.shared.logger import log_action
from src.nhs_reusable_code_library.resuable_codes.shared.models import Task, TaskStatus
from src.nhs_reusable_code_library.resuable_codes.shared.store.base import BaseStore


class Indexes:
    PARENT_ID_ID = 'ix_parent_id_id'
    STATUS_ID = 'ix_status_id'
    STATUS_CREATED = 'ix_status_created'
    STATUS_UPDATED = 'ix_status_updated'
    NAME_ID = 'ix_name_id'


class TasksStore(BaseStore):
    _table_name = 'tasks'
    _keys = ['task_id']

    def __init__(self, table=None):
        super().__init__(Task, table)

    @staticmethod
    def get_dataset_workflow_name(dataset_id: str, workflow: str, dataset_version: str = None) -> str:

        if not (dataset_id or '').strip():
            raise ValueError('dataset_id')

        if not (workflow or '').strip():
            raise ValueError('workflow')

        parts = [dataset_id]
        if dataset_version:
            parts.append(dataset_version)
        parts.append(workflow)

        return ':'.join(parts)

    @staticmethod
    def create_task_id(*parts: str) -> str:
        assert parts
        parts = [p.strip() for p in parts if p.strip()]
        assert len(parts) > 1, parts
        return ':'.join(parts)

    def create_task(
            self, task_name: str, parent_id: str, status: str = TaskStatus.Pending,
            complete: bool = False, result: Dict[str, Union[str, List[str]]] = None
    ) -> Task:
        """

        Args:
            task_name (str): name of the current task
            parent_id (str): parent task id
            status (str): initial TaskStatus pending/running
            complete (bool): if the task is complete (when it's created) ... will mark as finished and successful
            result (dict): result bag
        Returns:
            Task: the stored task object
        """
        return self._create_task(
            task_id=self.create_task_id(parent_id, task_name),
            task_name=task_name,
            parent_id=parent_id,
            status=status,
            complete=complete,
            result=result,
        )

    def create_workflow_run(
            self, workflow_name: str, run_param: str,
            status: str = TaskStatus.Pending, complete: bool = False,
            parent_param: str = None,
            result: Dict[str, Union[str, List[str]]] = None
    ) -> Task:
        """

        Args:
            workflow_name (str): name of the workflow .. eg  mhsds_v4_eom
            run_param (str): distinct run param .. e.g.  20191101 if the run is for the 20191101 submission window
            status (str): initial TaskStatus pending/running
            complete (bool): if the workflow is complete (when it's created) ... just for management really
            result (dict): result bag
            parent_param (str): distinct parent id param .
        Returns:
            Task: the stored task object
        """

        return self._create_task(
            task_id=self.create_task_id(workflow_name, run_param),
            task_name=workflow_name,
            parent_id=self.create_task_id(workflow_name, parent_param),
            status=status,
            complete=complete,
            result=result
        )

    def _create_task(
            self, task_id: str, task_name: str, parent_id: Optional[str], status: str = TaskStatus.Pending,
            complete: bool = False, result: Dict[str, Union[str, List[str]]] = None
    ) -> Task:
        """

        Args:
            task_id (str): task id
            task_name (str): name of the current task
            parent_id (str): parent task id
            status (str): initial TaskStatus pending/running
            complete (bool): if the task is complete (when it's created) ... will mark as finished and successful
            result (dict): result bag
        Returns:
            Task: the stored task object
        """

        now = datetime.utcnow()
        finished = None
        if complete:
            finished = now
            status = TaskStatus.Success

        task = Task(
            dict(
                id=task_id,
                name=task_name,
                status=status,
                complete=complete,
                created=now,
                parent_id=parent_id,
                finished=finished,
                last_updated=now,
                result=result,
            )
        )

        response = self.put(
            model=task,
            condition_expression="attribute_not_exists(id)"
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        return task

    def get_by_id(
            self, task_id: str, consistent_read: bool = False, raise_if_not_found: bool = False
    ) -> Task:
        return self.get(key=dict(id=task_id), consistent_read=consistent_read, raise_if_not_found=raise_if_not_found)

    def delete_by_id(self, task_id: str) -> None:
        return self.delete(key=dict(id=task_id))

    @log_action(log_args=['parent_id'])
    @dynamodb_retry_backoff()
    def get_by_parent_id(
            self, parent_id: str, consistent_read: bool = False, predicate: Callable[[Dict[str, Any]], bool] = None
    ) -> Generator[Task, None, None]:
        response = self.table.query(
            IndexName=Indexes.PARENT_ID_ID,
            KeyConditionExpression=Key('parent_id').eq(parent_id),
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        items = response.get('Items', [])

        for item in items:

            if predicate and not predicate(item):
                continue

            item = self.get_by_id(item['id'], consistent_read=consistent_read)
            if item.parent_id != parent_id:
                continue
            yield Task(item)

    @log_action(log_args=['workflow_id'])
    def get_by_workflow_id(self, workflow_id: str, consistent_read: bool = False,
                           predicate: Callable[[Dict[str, Any]], bool] = None) \
            -> Generator[Task, None, None]:
        for task in self.get_by_parent_id(workflow_id, consistent_read, predicate):
            yield task
            yield from self.get_by_workflow_id(task.id, consistent_read, predicate)

    @log_action(log_args=['status'])
    @dynamodb_retry_backoff()
    def get_status_count(self, status: str) -> int:
        return ddb_query_paginated_count(
            TableName=self.table.name,
            IndexName=Indexes.STATUS_ID,
            KeyConditionExpression=Key('status').eq(status)
        )

    @log_action()
    def get_status_counts(self) -> Dict[str, int]:
        counts = {
            status: self.get_status_count(status) for status in TaskStatus.all_statuses()
        }
        return counts

    @log_action(log_args=['status'])
    @dynamodb_retry_backoff()
    def get_all_by_status(
            self, status: str, predicate: Callable[[Dict[str, Any]], bool] = None
    ) -> Generator[Task, None, None]:

        response = self.table.query(
            IndexName=Indexes.STATUS_ID,
            KeyConditionExpression=Key('status').eq(status),
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        items = response.get('Items', [])

        for item in items:
            if predicate and not predicate(item):
                continue
            item = self.get_by_id(item['id'], consistent_read=True)
            if item.status != status:
                continue
            yield Task(item)

    def mark_pending(self, task_id: str) -> bool:
        return self.update_status(
            task_id,
            TaskStatus.Pending,
            result={},  # reset result
        )

    def mark_running(self, task_id: str) -> bool:
        return self.update_status(
            task_id,
            TaskStatus.Running,
            check_existing_status_is=TaskStatus.Pending,
            raise_if_condition_fails=True
        )

    def mark_failed(self, task_id: str, exception: Exception = None) -> bool:
        result = {}
        if exception:
            result['exception'] = ''.join(traceback.TracebackException.from_exception(exception).format()).strip()

        return self.update_status(
            task_id,
            TaskStatus.Failed,
            result=result
        )

    def mark_complete(self, task_id: str, result: MutableMapping[str, Any]) -> bool:
        return self.update_status(
            task_id,
            TaskStatus.Success,
            result=result,
            check_existing_status_is=TaskStatus.Running,
            raise_if_condition_fails=True,
            complete=True
        )

    @log_action(log_args=['task_id', 'status', 'check_existing_status_is'])
    @dynamodb_retry_backoff()
    def update_status(
            self, task_id: str, status: str,
            check_existing_status_is: Union[str, AbstractSet[str]] = None,
            raise_if_condition_fails: bool = True, complete: bool = False, finish: bool = False,
            result: MutableMapping[str, Any] = None
    ) -> bool:

        finish = finish or complete

        update_expression = "SET last_updated = :val1, #status = :val2"

        expression_attribs = {
            ':val1': datetime.utcnow().isoformat(),
            ':val2': status,
        }

        reserved_attrs = {
            # reserved names
            '#status': 'status',
        }

        if finish:
            update_expression = f'{update_expression}, finished = :val1'

        if complete:
            update_expression = f'{update_expression}, complete = :val3'
            expression_attribs[':val3'] = True

        if result is not None:
            update_expression = f'{update_expression}, #result = :val4'
            expression_attribs[':val4'] = result
            reserved_attrs['#result'] = 'result'

        args = dict(
            Key=dict(id=task_id),
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribs,
            ExpressionAttributeNames=reserved_attrs
        )

        if check_existing_status_is:
            if isinstance(check_existing_status_is, str):
                check_existing_status_is = {check_existing_status_is}

            value_names = []  # type: List[str]
            for index, expected_status in enumerate(check_existing_status_is):
                value_name = ':val{}'.format(index + len(expression_attribs) + 1)
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


Tasks = TasksStore()
