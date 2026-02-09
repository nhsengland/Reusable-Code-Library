from nhs_reusable_code_library.resuable_codes.shared.aws import ddb_table, dynamodb_retry_backoff
from nhs_reusable_code_library.resuable_codes.shared.common import enumlike_values
from nhs_reusable_code_library.resuable_codes.shared.logger import app_logger, log_action
from nhs_reusable_code_library.resuable_codes.shared.store.common import replace_decimals


class CounterAttributes:
    AgentId = "agent_id"
    ExtractId = "extract_id"
    PipelineStateId = "pipeline_state_id"
    SubmissionId = "submission_id"
    ExportId = "export_id"
    DeIdJobId = "deid_job_id"
    S3SubmissionId = "s3_submission_id"
    AdhocMPSSubmissionId = 'adhoc_mps_submission_id'
    DarsMessageId = "dars_message_id"


_COUNTERS = enumlike_values(CounterAttributes)
_COUNTERS_RECORD_ID = 0


class CountersStore:

    def __init__(self, table=None):
        self._table = table

    def __getstate__(self):
        attributes = self.__dict__.copy()
        attributes["_table"] = None
        return attributes

    @property
    def table(self):
        if not self._table:
            self._table = ddb_table('counters')
            self._bootstrap_table()

        return self._table

    @log_action()
    @dynamodb_retry_backoff()
    def _bootstrap_table(self):

        response = self.table.get_item(Key=dict(id=_COUNTERS_RECORD_ID), ConsistentRead=True)

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        if not response.get('Item'):
            counters = dict(id=_COUNTERS_RECORD_ID)

            self.table.put_item(
                Item=counters,
                ConditionExpression="attribute_not_exists(#id)",
                ExpressionAttributeNames={
                    # reserved names
                    '#id': 'id',
                }
            )

            assert 200 == response['ResponseMetadata']['HTTPStatusCode']

    @log_action(log_args=['counter_attribute', 'increment'])
    @dynamodb_retry_backoff()
    def _increment_counter(self, counter_attribute: str, increment: int = 1) -> int:

        app_logger.debug(lambda: dict(message='increment_counter', counter_attribute=counter_attribute, increment=increment))

        response = self.table.update_item(
            Key=dict(id=_COUNTERS_RECORD_ID),
            UpdateExpression="ADD {attr} :incr".format(attr=counter_attribute),
            ExpressionAttributeValues={
                ':incr': increment,
            },
            ReturnValues="UPDATED_NEW"
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        return replace_decimals(response['Attributes'][counter_attribute])

    @log_action(log_args=['counter_attribute', 'consistent_read'])
    @dynamodb_retry_backoff()
    def _get_current_counter_value(self, counter_attribute: str, consistent_read: bool = False):

        response = self.table.get_item(
            Key=dict(id=_COUNTERS_RECORD_ID),
            ProjectionExpression=counter_attribute,
            ConsistentRead=consistent_read
        )

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

        return replace_decimals(response['Item'][counter_attribute])

    def get_next_agent_id(self) -> int:
        return self._increment_counter(CounterAttributes.AgentId)

    def get_last_agent_id(self):
        return self._get_current_counter_value(CounterAttributes.AgentId)

    def get_next_submission_id(self) -> int:
        return self._increment_counter(CounterAttributes.SubmissionId)

    def get_last_submission_id(self):
        return self._get_current_counter_value(CounterAttributes.SubmissionId)

    def get_next_extract_id(self) -> int:
        return self._increment_counter(CounterAttributes.ExtractId)

    def get_last_extract_id(self) -> int:
        return self._get_current_counter_value(CounterAttributes.ExtractId)

    def get_next_pipeline_state_id(self) -> int:
        return self._increment_counter(CounterAttributes.PipelineStateId)

    def get_last_pipeline_state_id(self):
        return self._get_current_counter_value(CounterAttributes.PipelineStateId)

    def get_next_export_id(self) -> int:
        return self._increment_counter(CounterAttributes.ExportId)

    def get_last_export_id(self) -> int:
        return self._get_current_counter_value(CounterAttributes.ExportId)

    def get_next_deid_job_id(self) -> int:
        return self._increment_counter(CounterAttributes.DeIdJobId)

    def get_last_deid_job_id(self) -> int:
        return self._get_current_counter_value(CounterAttributes.DeIdJobId)

    def get_next_s3_submission_id(self) -> int:
        return self._increment_counter(CounterAttributes.S3SubmissionId)

    def get_last_s3_submission_id(self):
        return self._get_current_counter_value(CounterAttributes.S3SubmissionId)

    def get_next_adhoc_mps_submission_id(self) -> int:
        return self._increment_counter(CounterAttributes.AdhocMPSSubmissionId)

    def get_last_adhoc_mps_submission_id(self):
        return self._get_current_counter_value(CounterAttributes.AdhocMPSSubmissionId)

    def get_next_dars_message_id(self) -> int:
        return self._increment_counter(CounterAttributes.DarsMessageId)


Counters = CountersStore()
