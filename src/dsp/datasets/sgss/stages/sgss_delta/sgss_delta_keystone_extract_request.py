from dsp.shared.constants import ExtractConsumers, Receivers
from dsp.shared.extracts.extract_constants import ExtractDetailsFields
from dsp.shared.extracts.request_extract import request_extract
from dsp.shared.logger import log_action, add_fields
from dsp.shared.models import ExtractPriority, ExtractType


@log_action(log_args=['submission_id', 'submission_working_folder'])
def request_sgss_delta_keystone_extract(submission_id: int, submission_working_folder: str):
    """Submits an sgss_delta Keystone extract request

    Args:
        submission_id: The out file of this submission is the data to be filtered.
        submission_working_folder: The working folder for the submission to create an extract out of.

    Returns:
        Extract id of the submitted sgss_delta Keystone extract
    """

    request = {
        ExtractDetailsFields.SUBMISSION_ID: submission_id,
        ExtractDetailsFields.SUBMISSION_WORKING_FOLDER: submission_working_folder
    }

    extract_id = request_extract(
        ExtractConsumers.DSP,
        Receivers.SFTP,
        request,
        ExtractPriority.High,
        ExtractType.SGSSDeltaKeystoneExtract
    )

    add_fields(extract_id=extract_id)
    return extract_id
