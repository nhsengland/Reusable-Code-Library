import datetime
import os
from operator import itemgetter
from typing import Iterator, List, Generator, AbstractSet, Union

from boto3.dynamodb.conditions import Key

from nhs_reusable_code_library.resuable_codes.shared.aws import dynamodb_retry_backoff, ddb_query_paginated_count
from nhs_reusable_code_library.resuable_codes.shared.constants import PATHS, JOBS
from nhs_reusable_code_library.resuable_codes.shared.logger import log_action
from nhs_reusable_code_library.resuable_codes.shared.models import DeIdJob, DeIdJobStatus, DeIdJobSourceFormat, Run
from nhs_reusable_code_library.resuable_codes.shared.store.base import BaseStore
from nhs_reusable_code_library.resuable_codes.shared.store.blocks import BlockStore, Blocks
from nhs_reusable_code_library.resuable_codes.shared.store.counters import Counters


class Indexes:
    SubmissionId = 'ix_submission_id'
    Status = 'ix_status'
    Domain = 'ix_domain'
    StatusDomain = 'ix_status_domain'


class DeIdJobStore(BaseStore[DeIdJob]):
    _table_name = 'deid_jobs'
    _keys = ['id']

    def __init__(self, table=None, blocks_store: BlockStore = None):
        super().__init__(DeIdJob, table)
        self._blocks_store = blocks_store or Blocks

    class SchemaMigrations:
        def from_0_to_1(self, item):
            item['runs'] = []
            return True


    @log_action(log_args=['submission_id', 'domain', 'location'])
    @dynamodb_retry_backoff()
    def create_deid_job(self, location: str, submission_id: int = 0, domain: str = 'Domain-0', path: str = None,
                        resume_deid_submission: bool = False, complete_after_retokenisation: bool = False) -> int:
        """Create a new de-id job in the dynamodb table and return the unique ID given"""
        job_id = Counters.get_next_deid_job_id()
        out_path = os.path.join(location, PATHS.DEID, path if path is not None else str(job_id))
        deid_job = DeIdJob(dict(
            id=job_id,
            submission_id=submission_id,
            status=DeIdJobStatus.Pending,
            created=datetime.datetime.now(),
            domain=domain,
            source=out_path,
            source_format=DeIdJobSourceFormat.Parquet,
            resume_deid_submission=resume_deid_submission,
            complete_after_retokenisation=complete_after_retokenisation
        ))
        self.put(deid_job)
        return job_id

    @log_action(log_args=['id'])
    @dynamodb_retry_backoff()
    def get_by_id(self, id: int) -> DeIdJob:
        return self.get({'id': id}, True)

    @log_action(log_args=['submission_id'])
    @dynamodb_retry_backoff()
    def get_all_by_submission_id(self, submission_id: int) -> Iterator[DeIdJob]:
        response = self.table.query(
            IndexName=Indexes.SubmissionId,
            KeyConditionExpression=Key('submission_id').eq(submission_id),
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        items = response.get('Items', [])

        for item in items:
            deid_job = self.get_by_id(item['id'])
            if deid_job.submission_id != submission_id:
                continue
            yield deid_job

    @log_action()
    @dynamodb_retry_backoff()
    def get_all_by_status(self, status: str, *other_statuses: str) -> Iterator[DeIdJob]:
        def query_status(status_to_query: str) -> Generator[DeIdJob, None, None]:
            response = self.table.query(
                IndexName=Indexes.Status,
                KeyConditionExpression=Key('status').eq(status_to_query),
            )

            assert response['ResponseMetadata']['HTTPStatusCode'] == 200

            items = response.get('Items', [])  # List[Dict]

            for item in sorted(items, key=itemgetter('id')):
                deid_job = self.get_by_id(item['id'])
                if deid_job.status != status_to_query:
                    continue

                yield deid_job

        yield from query_status(status)
        for other_status in other_statuses:
            yield from query_status(other_status)

    @log_action(log_args=['domain'])
    @dynamodb_retry_backoff()
    def get_all_by_domain(self, domain: str) -> Iterator[DeIdJob]:
        response = self.table.query(
            IndexName=Indexes.Domain,
            KeyConditionExpression=Key('domain').eq(domain),
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        items = response.get('Items', [])  # List[Dict]

        for item in items:
            deid_job = self.get_by_id(item['id'])
            if deid_job.domain != domain:
                continue
            yield deid_job

    @log_action()
    @dynamodb_retry_backoff()
    def get_all_by_status_and_domain(self, status: str, domain: str) -> Iterator[DeIdJob]:
        response = self.table.query(
            IndexName=Indexes.StatusDomain,
            KeyConditionExpression=Key('status').eq(status) & Key('domain').eq(domain),
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        items = response.get('Items', [])  # List[Dict]

        for item in sorted(items, key=itemgetter('id')):
            deid_job = self.get_by_id(item['id'])
            if deid_job.status != status or deid_job.domain != domain:
                continue

            yield deid_job

    @log_action()
    @dynamodb_retry_backoff()
    def get_all_ready(self, domain: str) -> Iterator[DeIdJob]:
        return self.get_all_by_status_and_domain(DeIdJobStatus.Ready, domain)

    @log_action()
    @dynamodb_retry_backoff()
    def get_all_non_blocked_ready(self, domain: str) -> Iterator[DeIdJob]:
        blocks = self._blocks_store.get_blocks(JOBS.DE_ID)
        for ready in self.get_all_ready(domain):
            if blocks.is_blocked(ready.get_block_group(), None, ready.created):
                continue

            yield ready

    @log_action()
    def get_all_non_blocked_ready_count_with_limit(self, domain: str, count_limit: int) -> int:
        count = 0
        for _ in self.get_all_non_blocked_ready(domain):
            count += 1
            if count == count_limit:
                break
        return count

    @log_action(log_args=['id', 'status'])
    @dynamodb_retry_backoff()
    def mark_as_status(
        self,
        id: int,
        status: str,
        run_id: int = None,
        valid_previous_statuses: Union[AbstractSet[str], List[str]] = None,
        raise_if_condition_fails=True) -> bool:
        args = dict(
            Key={'id': id},
            UpdateExpression="SET #status = :status",
            ExpressionAttributeValues={
                ':status': status,
            },
            ExpressionAttributeNames={
                '#status': 'status',
            }
        )

        if run_id is not None:
            new_run = Run(dict(run_id=run_id)).to_primitive()
            args['UpdateExpression'] += ', #runs = list_append(runs, :runs)'
            args['ExpressionAttributeValues'][':runs'] = [new_run]
            args['ExpressionAttributeNames']['#runs'] = 'runs'

        if valid_previous_statuses:
            value_names = []  # type: List[str]
            for index, expected_status in enumerate(valid_previous_statuses):
                value_name = ':val{}'.format(index + 2)
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

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        return True

    @log_action(log_args=['id'])
    @dynamodb_retry_backoff()
    def mark_ready(self, id: int):
        return self.mark_as_status(id, status=DeIdJobStatus.Ready, valid_previous_statuses=[
            DeIdJobStatus.Pending, DeIdJobStatus.Failed, DeIdJobStatus.Success
        ])

    @log_action(log_args=['id'])
    @dynamodb_retry_backoff()
    def mark_processing(self, id: int, run_id: int):
        return self.mark_as_status(id, status=DeIdJobStatus.Processing, valid_previous_statuses=[DeIdJobStatus.Ready],
                                   run_id=run_id)

    @log_action(log_args=['id'])
    @dynamodb_retry_backoff()
    def mark_failed(self, id: int):
        return self.mark_as_status(
            id,
            status=DeIdJobStatus.Failed,
            valid_previous_statuses=[DeIdJobStatus.Pending, DeIdJobStatus.Processing]
        )

    @log_action(log_args=['id'])
    @dynamodb_retry_backoff()
    def mark_success(self, id: int):
        return self.mark_as_status(id, status=DeIdJobStatus.Success, valid_previous_statuses=[DeIdJobStatus.Processing])

    @log_action(log_args=['id'])
    @dynamodb_retry_backoff()
    def mark_cancelled(self, id: int):
        return self.mark_as_status(id, status=DeIdJobStatus.Cancelled, valid_previous_statuses=[
            DeIdJobStatus.Pending, DeIdJobStatus.Failed
        ])

    @log_action()
    @dynamodb_retry_backoff()
    def get_status_domain_count(self, status: str, domain: str) -> int:
        return ddb_query_paginated_count(
            TableName=self.table.name,
            IndexName=Indexes.StatusDomain,
            KeyConditionExpression=Key('status').eq(status) & Key('domain').eq(domain)
        )

    @log_action(log_args=['id', 'extract_id'])
    @dynamodb_retry_backoff()
    def set_extract_id(self, id: int, extract_id: str):

        response = self.table.update_item(
            Key={'id': id},
            UpdateExpression="SET #extract_id = :extract_id",
            ExpressionAttributeValues={
                ':extract_id': extract_id
            },
            ExpressionAttributeNames={
                '#extract_id': 'extract_id'
            }
        )

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

        return True


DeIdJobs = DeIdJobStore()
