import datetime

from pytest import raises
from schematics.exceptions import DataError

from dsp.datasets.models.mpsaas import MPSRequestHeaderRecord


def test_good_record():
    header = MPSRequestHeaderRecord({
        'request_reference': 'MPTREQ_19700101000000',
        'workflow_id': 'SPINE_MPTPHASE4_TRACE',
        'request_timestamp': '19700101000000',
        'no_of_data_records': 100
    })

    assert header.request_reference == 'MPTREQ_19700101000000'
    assert header.workflow_id == 'SPINE_MPTPHASE4_TRACE'
    assert header.request_timestamp == datetime.datetime(1970, 1, 1, 0, 0, 0)
    assert header.no_of_data_records == 100


def test_bad_date():
    with raises(DataError):
        MPSRequestHeaderRecord({
            'request_reference': 'MPTREQ_19700101000000',
            'workflow_id': 'SPINE_MPTPHASE4_TRACE',
            'request_timestamp': 'INVALID_DATE',
            'no_of_data_records': 100
        })


def test_bad_reference():
    with raises(DataError):
        MPSRequestHeaderRecord({
            'request_reference': 'BAD_REFERENCE',
            'workflow_id': 'SPINE_MPTPHASE4_TRACE',
            'request_timestamp': '19700101000000',
            'no_of_data_records': 100
        })


def test_bad_workflow_id():
    with raises(DataError):
        MPSRequestHeaderRecord({
            'request_reference': 'MPTREQ_19700101000000',
            'workflow_id': 'BAD_WORKFLOW_ID',
            'request_timestamp': '19700101000000',
            'no_of_data_records': 100
        })


def test_too_few():
    with raises(DataError):
        MPSRequestHeaderRecord({
            'request_reference': 'MPTREQ_19700101000000',
            'workflow_id': 'SPINE_MPTPHASE4_TRACE',
            'request_timestamp': '19700101000000',
            'no_of_data_records': -1
        })


def test_too_many():
    with raises(DataError):
        MPSRequestHeaderRecord({
            'request_reference': 'MPTREQ_19700101000000',
            'workflow_id': 'SPINE_MPTPHASE4_TRACE',
            'request_timestamp': '19700101000000',
            'no_of_data_records': 500001
        })
