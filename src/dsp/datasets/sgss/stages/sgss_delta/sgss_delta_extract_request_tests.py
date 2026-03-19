from dsp.datasets.sgss.stages.sgss_delta.sgss_delta_extract_request import request_sgss_delta_extract
from dsp.shared.constants import Receivers, ExtractConsumers
from dsp.shared.extracts.extract_constants import ExtractDetailsFields, SGSSExtractDetailsFields
from dsp.shared.models import ExtractType, ExtractPriority
from dsp.shared.store.extract_requests import ExtractRequests


def test_request_sgss_delta_extract():
    extract_id = request_sgss_delta_extract(10, 'test/path/', 'file_name.csv', 'workflow_id', 'sender_mailbox',
                                            'local_id', ['wales'])

    assert extract_id
    extract_request = ExtractRequests.get_by_id(extract_id)
    assert extract_request.type == ExtractType.SGSSDeltaExtract
    assert extract_request.request[ExtractDetailsFields.SUBMISSION_ID] == 10
    assert extract_request.request[ExtractDetailsFields.SUBMISSION_WORKING_FOLDER] == 'test/path/'
    assert extract_request.request[SGSSExtractDetailsFields.SUBMISSION_FILE_NAME] == 'file_name.csv'
    assert extract_request.request[SGSSExtractDetailsFields.SUBMISSION_WORKFLOW_ID] == 'workflow_id'
    assert extract_request.request[SGSSExtractDetailsFields.SUBMISSION_SENDER_MAILBOX] == 'sender_mailbox'
    assert extract_request.request[SGSSExtractDetailsFields.SUBMISSION_LOCAL_ID] == 'local_id'
    assert extract_request.request[SGSSExtractDetailsFields.COUNTRY_CATEGORIES] == ['wales']
    assert extract_request.sender == ExtractConsumers.DSP
    assert extract_request.consumer == Receivers.MESH
    assert extract_request.priority == ExtractPriority.High
