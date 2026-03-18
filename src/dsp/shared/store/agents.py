from typing import List, Union

from boto3.dynamodb.conditions import Key

from dsp.shared.aws import dynamodb_retry_backoff
from dsp.shared.logger import log_action, debug_fields
from dsp.shared.models import Agent
from dsp.shared.store.base import BaseStore


class Indexes:

    CommonName = "ix_agents_by_common_name"


class AgentsStore(BaseStore[Agent]):

    _table_name = 'agents'
    _keys = ['id']

    class SchemaMigrations:
        def from_0_to_1(self, item):
            return True

    def __init__(self, table=None):
        super().__init__(Agent, table)

    def get_by_key(self, agent_id: int, consistent_read: bool = False) -> Agent:

        return self.get(key=dict(id=agent_id), consistent_read=consistent_read)

    @log_action(log_args=['agent'])
    @dynamodb_retry_backoff()
    def create_agent(self, agent: Agent):

        debug_fields(lambda: agent.to_primitive())

        response = self.table.put_item(Item=agent.to_primitive())

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        return agent.id

    @log_action(log_args=['cn'])
    @dynamodb_retry_backoff()
    def get_by_cn(self, cn) -> List[Agent]:
        response = self.table.query(
            IndexName=Indexes.CommonName,
            KeyConditionExpression=Key('common_name').eq(cn),
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        return self.items_to_models(response.get('Items', []))

    @log_action(log_args=['agent_id', 'projection_expr', 'attribute_names', 'consistent_read'])
    @dynamodb_retry_backoff()
    def project_by_id(
            self, agent_id: str, projection_expr: str, attribute_names: dict = None, consistent_read: bool = False
    ) -> Union[Agent, None]:

        args = dict(
            Key=dict(id=agent_id),
            ProjectionExpression=projection_expr,
            ConsistentRead=consistent_read
        )

        if attribute_names:
            args['ExpressionAttributeNames'] = attribute_names

        response = self.table.get_item(**args)

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            return None

        return self.item_to_model(response.get('Item'))


Agents = AgentsStore()
