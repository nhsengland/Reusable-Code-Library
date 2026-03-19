import os

import pytest
import botocore
from pytest_mock import MockFixture
from typing import Dict
from mock import Mock

import dsp.pipeline.deidentify.base as deidentify_base
from dsp.pipeline.deidentify.base import DeidentifyBase


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


@pytest.fixture(scope='function')
def test_deid_base(tmp_path) -> DeidentifyBase:
    test_deidjob = DeidentifyBase('test.privacy.data.digital.nhs.uk')
    test_deidjob.p12_secret = 'secret'
    test_deidjob.ca_cert_path = os.path.join(tmp_path, 'ca_cert')
    test_deidjob.p12_cert_path = os.path.join(tmp_path, 'p12_cert')
    test_deidjob.aws_p12_path = '/client-certs'

    return test_deidjob


@pytest.fixture(scope='function')
def secret_value_mock(mocker: MockFixture) -> Mock:
    secret_value = mocker.patch.object(deidentify_base, 'secret_value', autospec=True)

    return secret_value


@pytest.fixture(scope='function')
def secret_binary_value_mock(mocker: MockFixture) -> Mock:
    secret_binary_value = mocker.patch.object(deidentify_base, 'secret_binary_value', autospec=True)

    return secret_binary_value


def test_load_certs(secret_value_mock, secret_binary_value_mock, test_deid_base):
    mock_pem_contents = 'MOCK_PEM_CONTENTS'
    mock_p12_contents = bytearray(b'\x00\x0F')

    secret_value_mock.return_value = mock_pem_contents
    secret_binary_value_mock.return_value = mock_p12_contents

    test_deid_base.load_certs()

    secret_value_mock.assert_called_once_with('/ca/test.privacy.data.digital.nhs.uk/crt')
    secret_binary_value_mock.assert_called_once_with('/client-certs/test.privacy.data.digital.nhs.uk/p12')

    # Check the files have been written to the folders specified by the path params
    f = open(test_deid_base.ca_cert_path, 'r')
    assert f.mode == 'r'

    contents = f.read()

    assert contents == mock_pem_contents

    f = open(test_deid_base.p12_cert_path, 'rb')
    assert f.mode == 'rb'

    contents = f.read()

    assert contents == mock_p12_contents

def test_permissions_exception_capture_in_cert_config(log_capture, secret_value_mock, secret_binary_value_mock, test_deid_base):

    def bad_secrets_request(*args):
        raise botocore.exceptions.ClientError(
        {
            'Error': {
                'Code': 'AccessDeniedException',
                'Message': 'User does not have access'
            }
        },
        'GetSecretValue'
    )

    mock_pem_contents = 'MOCK_PEM_CONTENTS'

    secret_value_mock.return_value = mock_pem_contents
    secret_binary_value_mock.side_effect = bad_secrets_request
    with pytest.raises(botocore.exceptions.ClientError):
        test_deid_base.load_certs()
    _, stderr = log_capture
    assert stderr[0]['action'] == 'DeidentifyBase.load_certs'
    assert 'AccessDeniedException' in stderr[0]['ex']


def test_permissions_exception_capture_in_create_config(log_capture, secret_value_mock, test_deid_base):

    def bad_secrets_request(*args):
        raise botocore.exceptions.ClientError(
        {
            'Error': {
                'Code': 'AccessDeniedException',
                'Message': 'User does not have access'
            }
        },
        'GetSecretValue'
    )

    secret_value_mock.side_effect = bad_secrets_request
    with pytest.raises(botocore.exceptions.ClientError):
        test_deid_base.create_json_config()
    _, stderr = log_capture
    assert stderr[0]['action'] == 'DeidentifyBase.create_json_config'
    assert 'AccessDeniedException' in stderr[0]['ex']


def create_mock_request(new_response_items: Dict):
    """create_mock_request used to generate mock responses to privitar api."""
    response_items = {  # Baseclass defaults
        'policies': [
            {'name': DeidentifyBase.pod_policy_name, 'id': 'policy123'}
        ],
        'dataflow': [],
        'jobs': [
            {'id': 'abc000', 'pdd': {
                'name': 'Domain-0'
            }},
            {'id': 'ddd111', 'pdd': {
                'name': 'Domain-1'
            }},
            {'id': 'ppp123', 'pdd': {
                'name': 'PDD-1'
            }}
        ],
        'pdd': [
            {
                'id': 'abc000',
                'name': 'Domain-0'
            },
            {
                'id': 'ddd111',
                'name': 'Domain-1'
            },
            {
                'id': 'ppp123',
                'name': 'PDD-1'
            }
        ]
    }
    response_items.update(new_response_items)

    def mocked_requests_get(*args, **kwargs):
        if args[0] == 'https://policy.test.privacy.data.digital.nhs.uk/policy-manager/api/v3/policies':
            return MockResponse({'items': response_items['policies']}, 200)
        elif args[0] == 'https://policy.test.privacy.data.digital.nhs.uk/policy-manager/api/v3/jobs/dataflow':
            return MockResponse({'items': response_items['dataflow']}, 200)
        elif args[0] == 'https://policy.test.privacy.data.digital.nhs.uk/policy-manager/api/v3/jobs/pod?policy-id=policy123&page-size=200&page=0':
            return MockResponse({'items': response_items['jobs']}, 200)
        elif args[0] == 'https://policy.test.privacy.data.digital.nhs.uk/policy-manager/api/v3/pdds?closed=false&page-size=200&page=0':
            return MockResponse({'items': response_items['pdd']}, 200)
        return MockResponse(None, 404)

    return mocked_requests_get

mocked_requests_get = create_mock_request({})


def test_get_policy_id(requests_pkcs12_get_mock, test_deid_base):
    requests_pkcs12_get_mock.side_effect = mocked_requests_get

    policy_id = test_deid_base.get_policy_id()

    assert policy_id == 'policy123'


def test_get_policy_id_conection_failure(requests_pkcs12_get_mock, test_deid_base):
    requests_pkcs12_get_mock.return_value = MockResponse(None, 404)

    with pytest.raises(ConnectionError, match='Bad response from.*'):
        test_deid_base.get_policy_id()


def test_get_policy_id_not_found_failure(requests_pkcs12_get_mock, test_deid_base):
    test_deid_base.pod_policy_name = 'not present'
    requests_pkcs12_get_mock.side_effect = mocked_requests_get
    with pytest.raises(RuntimeError, match='Unable to find policy ID.*'):
        test_deid_base.get_policy_id()


def test_get_pod_jobs(requests_pkcs12_get_mock, test_deid_base):
    requests_pkcs12_get_mock.side_effect = mocked_requests_get

    pod_jobs = test_deid_base.get_pod_jobs('policy123')

    assert len(pod_jobs) == 3
    assert pod_jobs[0]['id'] == 'abc000'
    assert pod_jobs[1]['id'] == 'ddd111'
    assert pod_jobs[2]['id'] == 'ppp123'


def test_get_open_pdds(requests_pkcs12_get_mock, test_deid_base):
    requests_pkcs12_get_mock.side_effect = mocked_requests_get

    open_pdds = test_deid_base.get_open_pdds()

    assert len(open_pdds) == 3
    assert open_pdds[0] == 'Domain-0'
    assert open_pdds[1] == 'Domain-1'
    assert open_pdds[2] == 'PDD-1'