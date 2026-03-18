from dsp.extracts.sgss_delta_keystone_extract_request import request_sgss_delta_keystone_extract
from shared.constants import Receivers, ExtractConsumers
from shared.extracts.extract_constants import ExtractDetailsFields
from shared.models import ExtractType, ExtractPriority
from shared.store.extract_requests import ExtractRequests


def test_request_sgss_delta_keystone_extract():
    extract_id = request_sgss_delta_keystone_extract(10, 'test/path/')

    assert extract_id
    extract_request = ExtractRequests.get_by_id(extract_id)
    assert extract_request.type == ExtractType.SGSSDeltaKeystoneExtract
    assert extract_request.request[ExtractDetailsFields.SUBMISSION_ID] == 10
    assert extract_request.request[ExtractDetailsFields.SUBMISSION_WORKING_FOLDER] == 'test/path/'
    assert extract_request.sender == ExtractConsumers.DSP
    assert extract_request.consumer == Receivers.SFTP
    assert extract_request.priority == ExtractPriority.High
