from datetime import datetime
from typing import List, Union

from boto3.dynamodb.conditions import Key

from dsp.shared.aws import dynamodb_retry_backoff
from dsp.shared.constants import BLOCKTYPE
from dsp.shared.logger import log_action, add_fields
from dsp.shared.models import Block
from dsp.shared.store.base import BaseStore


class Indexes:
    DatasetId = "ix_blocks_by_dataset_id"
    BlockGroup = "ix_blocks_by_block_group"


class BlockStore(BaseStore[Block]):

    _table_name = 'blocks'
    _all = Block.ALL

    _keys = ['job_type', 'comp_range_key']

    def __init__(self, table=None):
        super().__init__(Block, table)

    def get_by_key(self, job_type: str, dataset_id: str, block_group: str, consistent_read: bool = False) -> Block:

        return self.get(
            key=dict(
                job_type=job_type, comp_range_key=self.get_range_key(dataset_id, block_group)
            ),
            consistent_read=consistent_read
        )

    @staticmethod
    def get_range_key(dataset_id: str, block_group: str) -> str:
        return dataset_id+':'+block_group

    @log_action()
    @dynamodb_retry_backoff()
    def add_block(self, job_type: str, dataset_id: str, block_group: str, block_type: BLOCKTYPE, **kwargs):

        item_dict = dict(
            job_type=job_type,
            block_group=block_group,
            dataset_id=dataset_id,
            comp_range_key=self.get_range_key(dataset_id, block_group),
            block_type=block_type
        )

        for key, value in kwargs.items():
            item_dict[key] = value

        add_fields(
            **item_dict
        )
        self.table.put_item(
            Item=self.model_to_item(Block(item_dict))
        )

    @log_action(log_args=['block_group'])
    @dynamodb_retry_backoff()
    def get_by_block_group(self, block_group: str) -> List[Block]:
        response = self.table.query(
            IndexName=Indexes.BlockGroup,
            KeyConditionExpression=Key('block_group').eq(block_group)
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        items = response.get('Items', [])

        return self.items_to_models(items)

    def add_super_block_for_job(self, job_type: str, **kwargs):

        return self.add_block(job_type, self._all, self._all, BLOCKTYPE.SUPER, **kwargs)

    @log_action(log_args=['job_type'])
    @dynamodb_retry_backoff()
    def get_by_job_type(self, job_type: str) -> List[Block]:

        response = self.table.query(

            KeyConditionExpression=Key('job_type').eq(job_type)
        )
        assert 200 == response['ResponseMetadata']['HTTPStatusCode']
        items = response.get('Items', [])
        return self.items_to_models(items)

    def remove_job_super_block(self, job_type: str):
        self.remove_block(job_type, self._all, self._all)

    @log_action(log_args=['job_type', 'dataset_id', 'block_group'])
    @dynamodb_retry_backoff()
    def remove_block(self, job_type: str, dataset_id: str, block_group: str):
        self.table.delete_item(
            Key={
                'job_type': job_type,
                'comp_range_key': self.get_range_key(dataset_id, block_group)
            }
        )

    @log_action(log_args=['job_type'])
    @dynamodb_retry_backoff()
    def get_blocks(self, job_type: str):
        # query table for all blocks for job_type
        sorted_items = sorted(self.get_by_job_type(job_type), key=lambda k: k.block_type)
        return BlockEvaluator(sorted_items)


Blocks = BlockStore()


class BlockEvaluator:

    def __init__(self, blocks: List[Block]):
        self._blocks = blocks

    def is_blocked(
        self,
        block_group: Union[str, None] = None,
        dataset_id: Union[str, None] = None,
        submission_date: datetime = None
    ) -> bool:

        return any(block.is_blocked(block_group, dataset_id, submission_date) for block in self._blocks)
