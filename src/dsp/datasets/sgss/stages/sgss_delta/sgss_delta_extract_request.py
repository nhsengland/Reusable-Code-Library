from typing import List

from dsp.shared.constants import ExtractConsumers, Receivers
from dsp.shared.extracts.extract_constants import ExtractDetailsFields, SGSSExtractDetailsFields
from dsp.shared.extracts.request_extract import request_extract
from dsp.shared.logger import log_action, add_fields
from dsp.shared.models import ExtractPriority, ExtractType


@log_action(log_args=['submission_id', 'submission_working_folder', 'submission_file_name',
                      'submission_workflow_id', 'submission_sender_mailbox', 'submission_local_id',
                      'country_categories'])
def request_sgss_delta_extract(submission_id: int, submission_working_folder: str, submission_file_name: str,
                               submission_workflow_id: str, submission_sender_mailbox: str,
                               submission_local_id: str, country_categories: List[str]):
    """Submits an SGSS delta extract request

    Args:
        submission_id: The out file of this submission is the data to be filtered.
        submission_working_folder: The working folder for the submission to create an extract out of.
        submission_file_name: The file name of the originating submission
        submission_workflow_id: The workflow ID of the originating submission
        submission_sender_mailbox: The mailbox used to send the originating submission
        submission_local_id: The local ID of the originating submission
        country_categories: The list of countries to send the extract for

    Returns:
        Extract id of the submitted SGSS delta extract
    """

    request = {
        ExtractDetailsFields.SUBMISSION_ID: submission_id,
        ExtractDetailsFields.SUBMISSION_WORKING_FOLDER: submission_working_folder,
        SGSSExtractDetailsFields.SUBMISSION_FILE_NAME: submission_file_name,
        SGSSExtractDetailsFields.SUBMISSION_WORKFLOW_ID: submission_workflow_id,
        SGSSExtractDetailsFields.SUBMISSION_SENDER_MAILBOX: submission_sender_mailbox,
        SGSSExtractDetailsFields.SUBMISSION_LOCAL_ID: submission_local_id,
        SGSSExtractDetailsFields.COUNTRY_CATEGORIES: country_categories
    }

    extract_id = request_extract(
        ExtractConsumers.DSP,
        Receivers.MESH,
        request,
        ExtractPriority.High,
        ExtractType.SGSSDeltaExtract
    )

    add_fields(extract_id=extract_id)
    return extract_id
