from botocore import exceptions as botocore_exceptions

from nhs_reusable_code_library.resuable_codes.shared import aws
from nhs_reusable_code_library.resuable_codes.shared import logger
from nhs_reusable_code_library.resuable_codes.shared.logger import add_fields
from nhs_reusable_code_library.resuable_codes.shared.models import LastSequenceNumber
from nhs_reusable_code_library.resuable_codes.shared.store.base import BaseStore


class OutOfOrderTimeStampOrSequenceNumberException(ValueError):
    pass


class InvalidChangeToSequenceHeadException(ValueError):
    pass


class ResetNotAllowedException(ValueError):
    pass


class LastSequenceNumbersStore(BaseStore[LastSequenceNumber]):
    """ Storage of the head number seen for submission sequences, per channel per dataset"""

    _table_name = 'last_sequence_numbers'
    _keys = ['channel_id', 'dataset_id']

    class SchemaMigrations:
        pass

    def __init__(self, table=None):
        super().__init__(LastSequenceNumber, table)

    def get_by_key(
            self, channel_id: str, dataset_id: str, consistent_read: bool = False, raise_if_not_found: bool = True
    ) -> LastSequenceNumber:
        """ Get last sequence number for given channel id and dataset id

        Args:
            channel_id (str): channel id to search for
            dataset_id (str): dataset id to search for
            consistent_read (bool): whether to issue a consistent read
            raise_if_not_found (bool): whether to raise an error if key not found

        Returns:
            LastSequenceNumber: model containing sequence head for given channel id and dataset id
        """

        return self.get(
            key=dict(channel_id=channel_id, dataset_id=dataset_id),
            consistent_read=consistent_read,
            raise_if_not_found=raise_if_not_found
        )

    @logger.log_action()
    @aws.dynamodb_retry_backoff()
    def put(self, last_sequence_number: LastSequenceNumber):
        """ Store sequence number head for channel id and dataset id

        Args:
            last_sequence_number (LastSequenceNumber): model containing details of last sequence number seen for channel
        """

        item = self.model_to_item(last_sequence_number)
        logger.add_fields(**item)

        try:
            response = self.table.put_item(
                Item=item,
                ConditionExpression='(attribute_not_exists(submitted_timestamp) OR :new_ts >= submitted_timestamp) AND '
                                    '(attribute_not_exists(head_sequence_number) OR :new_sn >= head_sequence_number)',
                ExpressionAttributeValues={
                    ':new_ts': last_sequence_number.submitted_timestamp,
                    ':new_sn': last_sequence_number.head_sequence_number
                }
            )
        except botocore_exceptions.ClientError as exc:
            if exc.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise OutOfOrderTimeStampOrSequenceNumberException
            raise

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    @logger.log_action()
    @aws.dynamodb_retry_backoff()
    def update_alerted(self, last_sequence_number: LastSequenceNumber):
        """ update the alerted flag to true for the sequence head of a given dataset and channel

        Args:
            last_sequence_number (LastSequenceNumber): model containing details of last sequence number seen for channel
        """

        try:
            response = self.table.update_item(
                Key=dict(channel_id=last_sequence_number.channel_id, dataset_id=last_sequence_number.dataset_id),
                ConditionExpression='attribute_not_exists(head_sequence_number) OR :seq_num = head_sequence_number',
                UpdateExpression="set alerted = :alrt, head_sequence_number = :seq_num, "
                                 "last_received_sequence_number = :last_rec_seq_num, "
                                 "last_received_submission_id = :last_rec_id",
                ExpressionAttributeValues={
                    ':alrt': True,
                    ':seq_num': last_sequence_number.head_sequence_number,
                    ':last_rec_seq_num': last_sequence_number.last_received_sequence_number,
                    ':last_rec_id': last_sequence_number.last_received_submission_id
                }
            )
        except botocore_exceptions.ClientError as exc:
            if exc.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise InvalidChangeToSequenceHeadException
            raise

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    @logger.log_action()
    @aws.dynamodb_retry_backoff()
    def reset_sequence(self, channel_id: str, dataset_id: str):
        """ reset sequence head for a given channel and dataset

        Args:
            channel_id (str): channel id to use as key
            dataset_id (str): dataset id to use as key
        """

        add_fields(channel_id=channel_id, dataset_id=dataset_id)

        try:
            response = self.table.update_item(
                Key=dict(channel_id=channel_id, dataset_id=dataset_id),
                ConditionExpression='attribute_exists(head_sequence_number) AND '
                                    'attribute_exists(last_received_sequence_number) '
                                    'AND attribute_exists(last_received_submission_id) AND alerted = :alrt '
                                    'AND head_sequence_number <> last_received_sequence_number',
                UpdateExpression="set head_sequence_number = last_received_sequence_number, "
                                 "head_submission_id = last_received_submission_id",
                ExpressionAttributeValues={':alrt': True}
            )
        except botocore_exceptions.ClientError as exc:
            if exc.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise ResetNotAllowedException
            raise

        assert response['ResponseMetadata']['HTTPStatusCode'] == 200


LastSequenceNumbers = LastSequenceNumbersStore()
