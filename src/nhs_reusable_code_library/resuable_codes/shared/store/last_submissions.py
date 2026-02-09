from boto3.dynamodb.conditions import Attr
from botocore import exceptions as botocore_exceptions

from nhs_reusable_code_library.resuable_codes.shared import aws
from nhs_reusable_code_library.resuable_codes.shared import logger
from nhs_reusable_code_library.resuable_codes.shared import models
from nhs_reusable_code_library.resuable_codes.shared.constants import DS
from nhs_reusable_code_library.resuable_codes.shared.models import LastSubmission
from nhs_reusable_code_library.resuable_codes.shared.store.base import BaseStore


class OutOfOrderException(ValueError):
    pass


class LastSubmissionsStore(BaseStore[LastSubmission]):

    _table_name = 'last_submissions'
    _keys = ['sender_id', 'dataset_id']

    class SchemaMigrations:
        pass

    def __init__(self, table=None):
        super().__init__(LastSubmission, table)

    def get_by_key(
            self, sender_id: str, dataset_id: str, consistent_read: bool = False, raise_if_not_found: bool = True
    ) -> LastSubmission:

        return self.get(
            key=dict(sender_id=sender_id, dataset_id=dataset_id),
            consistent_read=consistent_read,
            raise_if_not_found=raise_if_not_found
        )

    @logger.log_action()
    @aws.dynamodb_retry_backoff()
    def put(self, last_submission: models.LastSubmission):
        item = self.model_to_item(last_submission)
        logger.add_fields(**item)

        try:
            response = self.table.put_item(
                Item=item,
                ConditionExpression='attribute_not_exists(submitted_timestamp) OR :new_ts >= submitted_timestamp',
                ExpressionAttributeValues={':new_ts': last_submission.submitted_timestamp}
            )
        except botocore_exceptions.ClientError as exc:
            if exc.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise OutOfOrderException
            raise

        assert 200 == response['ResponseMetadata']['HTTPStatusCode']

    @logger.log_action()
    def delete_non_gp_data(self):
        assert self._keys is not None

        for item in self.table.scan(
                ProjectionExpression=",".join(self._keys),
                FilterExpression=Attr('dataset_id').ne(DS.GP_DATA),
                ConsistentRead=True
        )['Items']:
            self.table.delete_item(
                Key={k: item[k] for k in self._keys}
            )


LastSubmissions = LastSubmissionsStore()
