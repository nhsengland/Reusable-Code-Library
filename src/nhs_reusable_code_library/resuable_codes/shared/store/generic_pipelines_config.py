from typing import List
from boto3.dynamodb.conditions import Key

from src.nhs_reusable_code_library.resuable_codes.shared.models import GenericPipelineConfig
from src.nhs_reusable_code_library.resuable_codes.shared.store.base import BaseStore
from src.nhs_reusable_code_library.resuable_codes.shared.logger import log_action, add_fields
from src.nhs_reusable_code_library.resuable_codes.shared.aws import dynamodb_retry_backoff


class Indexes:
    Workflow = "ix_generic_pipeline_config_by_workflow"


class GenericPipelinesConfigStore(BaseStore[GenericPipelineConfig]):

    _table_name = 'generic_pipelines_config'
    _keys = ['pipeline_name']

    def __init__(self, table=None):
        super().__init__(GenericPipelineConfig, table)

    def get_by_key(self, pipeline_name: str, consistent_read: bool = False) -> GenericPipelineConfig:

        return self.get(key=dict(pipeline_name=pipeline_name), consistent_read=consistent_read)

    def get_all(self, consistent_read: bool = False) -> List[GenericPipelineConfig]:

        configs = [
            self.item_to_model(item) for item in self.table.scan(
                ConsistentRead=consistent_read
            )['Items']
        ]

        return configs

    @log_action(log_args=['workflow'])
    @dynamodb_retry_backoff()
    def get_by_workflow(self, workflow: str) -> List[GenericPipelineConfig]:

        response = self.table.query(
            IndexName=Indexes.Workflow,
            KeyConditionExpression=Key('workflow').eq(workflow),
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        results = response.get('Items', [])

        results.sort(key=lambda i: i['created'])

        add_fields(results=[i['workflow'] for i in results])

        return [GenericPipelineConfig(res) for res in results]


GenericPipelinesConfig = GenericPipelinesConfigStore()
