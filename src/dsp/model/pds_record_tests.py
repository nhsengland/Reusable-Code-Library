import json
from datetime import datetime
from typing import MutableMapping

import pytest

from dsp.model.pds_record import PDSRecordPaths, PDSRecord, PDSDataImportException

sequence_number = 123  # type: int
activity_date = "20160223171219"
nhs_number = '12341234567'


@pytest.fixture()
def full_pds_record_data() -> MutableMapping:
    return {
        "name_history": [

            {
                "scn": 4,
                "givenName": [
                    {
                        "name": "LUDMILA"
                    },
                    {
                        "name": ""
                    }
                ],
                "familyName": "SINIAKOVA",
                "from": 20100112,
                "to": 20141223
            },
            {
                "scn": 5,
                "givenName": [
                    {
                        "name": "LUDMILLIA"
                    },
                    {
                        "name": "ARIEL"
                    }
                ],
                "familyName": "SINIAKOVAK",
                "from": 20141223
            }
        ],
        "mobilePhone": "0703991991",
        "dob": 20040129,
        "dod": 20190130,
        "migrant_data": [
            {
                "scn": 4,
                "from": 20121014,
                "to": 20130203,
                "visa_to": 20130203,
                "visa_status": "01",
                "brp_no": "123456123",
                "home_office_ref_no": "1234543210",
                "visa_from": 20121102,
                "nationality": "DEU"
            },
            {
                "scn": 5,
                "from": 20140811,
                "visa_to": 20150107,
                "visa_status": "01",
                "brp_no": "654321123",
                "home_office_ref_no": "5436543210",
                "visa_from": 20140903,
                "nationality": "DEU"
            }
        ],
        "telephone": "(01234)567890",
        "mpsid_details": [
            {
                "mpsID": "X110000001",
                "scn": 4,
                "localPatientID": "X000000001"
            },
            {
                "mpsID": "X928382011",
                "scn": 4,
                "localPatientID": "X000000002"
            }
        ],
        "sensitive": True,
        "address_history": [
            {
                "postalCode": "M19 3HD",
                "scn": 4,
                "from": 20120313,
                "to": 20150503,
                "addr": [
                    {
                        "line": ""
                    },
                    {
                        "line": "236 DSP MPS ROAD"
                    },
                    {
                        "line": "DSP"
                    },
                    {
                        "line": "MPS"
                    },
                    {
                        "line": ""
                    }
                ]
            },
            {
                "postalCode": "LS1 4BT",
                "scn": 5,
                "from": 20150503,
                "addr": [
                    {
                        "line": "My House"
                    },
                    {
                        "line": "14 Some Street"
                    },
                    {
                        "line": "This Area"
                    },
                    {
                        "line": "That town"
                    },
                    {
                        "line": "County"
                    }
                ]
            }
        ],
        "gender_history": [
            {
                "gender": "2",
                "scn": 4,
                "from": 20110316,
                "to": 20120401
            },
            {
                "gender": "1",
                "scn": 5,
                "from": 20120401
            }
        ],
        "emailAddress": "hellomps@emailaddress.com",
        "replaced_by": None,
        "gp_codes": [
            {
                "scn": 4,
                "from": 20081001,
                "to": 20090111,
                "val": "B00099"
            },

            {
                "scn": 5,
                "from": 20090111,
                "val": "D34099"
            }
        ],
        "death_status": "1",
        "confidentiality": [
            {
                "scn": 4,
                "from": 20141012,
                "to": 20150507,
                "val": "S"
            },
            {
                "scn": 5,
                "from": 20150507,
                "val": "I"
            }
        ]
    }


@pytest.fixture()
def full_pds_record_data_v2() -> MutableMapping:
    return {
        "name_history": [

            {
                "scn": 4,
                "givenNames": ["LUDMILA"],
                "familyName": "SINIAKOVA",
                "from": 20100112,
                "to": 20141223
            },
            {
                "scn": 5,
                "givenNames": ["LUDMILLIA", "ARIEL", "A"],
                "familyName": "SINIAKOVAK",
                "from": 20141223
            }
        ],
        "mobilePhone": "0703991991",
        "dob": 20040129,
        "dod": 20190130,
        "migrant_data": [
            {
                "scn": 4,
                "from": 20121014,
                "to": 20130203,
                "visa_to": 20130203,
                "visa_status": "01",
                "brp_no": "123456123",
                "home_office_ref_no": "1234543210",
                "visa_from": 20121102,
                "nationality": "DEU"
            },
            {
                "scn": 5,
                "from": 20140811,
                "visa_to": 20150107,
                "visa_status": "01",
                "brp_no": "654321123",
                "home_office_ref_no": "5436543210",
                "visa_from": 20140903,
                "nationality": "DEU"
            }
        ],
        "telephone": "(01234)567890",
        "mpsid_details": [
            {
                "mpsID": "X110000001",
                "scn": 4,
                "localPatientID": "X000000001"
            },
            {
                "mpsID": "X928382011",
                "scn": 4,
                "localPatientID": "X000000002"
            }
        ],
        "sensitive": True,
        "address_history": [
            {
                "postCode": "M19 3HD",
                "scn": 4,
                "from": 20120313,
                "to": 20150503,
                "lines": ["236 DSP MPS ROAD", "DSP", "MPS"]
            },
            {
                "postCode": "LS1 4BT",
                "scn": 5,
                "from": 20150503,
                "lines": ["My House", "14 Some Street", "This Area", "That Town", "County"]
            }
        ],
        "gender_history": [
            {
                "gender": "2",
                "scn": 4,
                "from": 20110316,
                "to": 20120401
            },
            {
                "gender": "1",
                "scn": 5,
                "from": 20120401
            }
        ],
        "emailAddress": "hellomps@emailaddress.com",
        "replaced_by": None,
        "gp_codes": [
            {
                "scn": 4,
                "from": 20081001,
                "to": 20090111,
                "val": "B00099"
            },

            {
                "scn": 5,
                "from": 20090111,
                "val": "D34099"
            }
        ],
        "death_status": "1",
        "confidentiality": [
            {
                "scn": 4,
                "from": 20141012,
                "to": 20150507,
                "val": "S"
            },
            {
                "scn": 5,
                "from": 20150507,
                "val": "I"
            }
        ]
    }


@pytest.fixture()
def full_pds_record(full_pds_record_data: str) -> PDSRecord:
    return _create_pds_record(full_pds_record_data)


@pytest.fixture()
def full_pds_record_v2(full_pds_record_data_v2: str) -> PDSRecord:
    return _create_pds_record(full_pds_record_data_v2)


def _create_pds_record(record_data: MutableMapping) -> PDSRecord:
    stringified_data = json.dumps(record_data)
    record = PDSRecord.from_mesh_line(
        '20190212155510',
        4,
        '9912004345',
        stringified_data,
        mandatory_field_check=True
    )
    return record


def _at(dtime: int) -> datetime:
    return datetime.strptime(str(dtime), '%Y%m%d%H%M%S')


@pytest.fixture()
def pds_record_data_with_different_schema() -> MutableMapping:
    return {
        "name_history": [

            {
                "scn": 4,
                "givenName": [
                    {
                        "name": "LUDMILA"
                    },
                    {
                        "name": ""
                    }
                ],
                "familyName": "SINIAKOVA",
                "prefix": "MS",
                "suffix": "PhD",
                "randomKey": "abc",
                "from": 20100112,
                "to": 20141223
            },
            {
                "scn": 5,
                "givenName": [
                    {
                        "name": "LUDMILLIA"
                    },
                    {
                        "name": "ARIEL"
                    }
                ],
                "familyName": "SINIAKOVAK",
                "prefix": "MS",
                "suffix": "PhD",
                "randomKey": "abc",
                "from": 20141223
            }
        ],
        "mobilePhone": "0703991991",
        "dob": 20040129,
        "dod": 20190130,
        "migrant_data": [
            {
                "scn": 4,
                "from": 20121014,
                "to": 20130203,
                "visa_to": 20130203,
                "visa_status": "01",
                "brp_no": "123456123",
                "home_office_ref_no": "1234543210",
                "visa_from": 20121102,
                "nationality": "DEU"
            },
            {
                "scn": 5,
                "from": 20140811,
                "visa_to": 20150107,
                "visa_status": "01",
                "brp_no": "654321123",
                "home_office_ref_no": "5436543210",
                "visa_from": 20140903,
                "nationality": "DEU"
            }
        ],
        "telephone": "(01234)567890",
        "mpsid_details": [
            {
                "mpsID": "X110000001",
                "scn": 4,
                "localPatientID": "X000000001"
            },
            {
                "mpsID": "X928382011",
                "scn": 4,
                "localPatientID": "X000000002"
            }
        ],
        "sensitive": True,
        "address_history": [
            {
                "postalCode": "M19 3HD",
                "scn": 4,
                "from": 20120313,
                "to": 20150503,
                "addr": [
                    {
                        "line": ""
                    },
                    {
                        "line": "236 DSP MPS ROAD"
                    },
                    {
                        "line": "DSP"
                    },
                    {
                        "line": "MPS"
                    },
                    {
                        "line": ""
                    }
                ],
                "addressKey": "A"
            },
            {
                "postalCode": "LS1 4BT",
                "scn": 5,
                "from": 20150503,
                "addr": [
                    {
                        "line": "My House"
                    },
                    {
                        "line": "14 Some Street"
                    },
                    {
                        "line": "This Area"
                    },
                    {
                        "line": "That town"
                    },
                    {
                        "line": "County"
                    }
                ],
                "addressKey": "A"
            }
        ],
        "gender_history": [
            {
                "gender": "2",
                "scn": 4,
                "from": 20110316,
                "to": 20120401
            },
            {
                "gender": "1",
                "scn": 5,
                "from": 20120401
            }
        ],
        "emailAddress": "hellomps@emailaddress.com",
        "replaced_by": None,
        "gp_codes": [
            {
                "scn": 4,
                "from": 20081001,
                "to": 20090111,
                "val": "B00099"
            },

            {
                "scn": 5,
                "from": 20090111,
                "val": "D34099"
            }
        ],
        "death_status": "1",
        "confidentiality": [
            {
                "scn": 4,
                "from": 20141012,
                "to": 20150507,
                "val": "S"
            },
            {
                "scn": 5,
                "from": 20150507,
                "val": "I"
            }
        ],
        "telecom_history": [
            {
                "scn": 4,
                "from": 20141012,
                "to": 20150507,
                "class": "S",
                "type": "tel",
                "tel": "0123"
            }
        ]
    }


###############################################################################
#
# NHS Number tests
#
###############################################################################
def test_nhs_number(full_pds_record_data):
    record = PDSRecord.from_mesh_line(
        '20190212155510',
        4,
        '9912004345',
        json.dumps(full_pds_record_data)
    )
    assert record.nhs_number() == '9912004345'


def test_nhs_number_none(full_pds_record_data):
    with pytest.raises(PDSDataImportException) as excinfo:
        PDSRecord.from_mesh_line(
            '20190212155510',
            4,
            None,
            json.dumps(full_pds_record_data)
        )
    assert str(excinfo.value) == 'key: PDS Record missing NHS Number'


def test_nhs_number_blank(full_pds_record_data):
    with pytest.raises(PDSDataImportException) as excinfo:
        PDSRecord.from_mesh_line(
            '20190212155510',
            4,
            '',
            json.dumps(full_pds_record_data)
        )
    assert str(excinfo.value) == 'key: PDS Record missing NHS Number'


###############################################################################
#
# Activity Date tests
#
###############################################################################
def test_activity_date(full_pds_record_data):
    record = PDSRecord.from_mesh_line(
        '20190212155510',
        4,
        '9912004345',
        json.dumps(full_pds_record_data)
    )
    assert record.activity_date() == '20190212155510'


def test_activity_date_none(full_pds_record_data):
    record = PDSRecord.from_mesh_line(
        None,
        4,
        '9912004345',
        json.dumps(full_pds_record_data)
    )
    assert not record.activity_date()


def test_activity_date_blank(full_pds_record_data):
    record = PDSRecord.from_mesh_line(
        '',
        4,
        '9912004345',
        json.dumps(full_pds_record_data)
    )
    assert record.activity_date() == ''


###############################################################################
#
# SCN tests
#
###############################################################################
def test_sequence_number(full_pds_record_data):
    record = PDSRecord.from_mesh_line(
        '20190212155510',
        4,
        '9912004345',
        json.dumps(full_pds_record_data)
    )
    assert record.sequence_number() == 4


def test_sequence_number_none(full_pds_record_data):
    with pytest.raises(PDSDataImportException) as excinfo:
        PDSRecord.from_mesh_line(
            '20190212155510',
            None,
            '9912004345',
            json.dumps(full_pds_record_data)
        )
    assert str(excinfo.value) == 'key: PDS Record missing SerialChangeNumber'


def test_sequence_number_blank(full_pds_record_data):
    with pytest.raises(PDSDataImportException) as excinfo:
        PDSRecord.from_mesh_line(
            '20190212155510',
            '',
            '9912004345',
            json.dumps(full_pds_record_data)
        )
    assert str(excinfo.value) == 'key: PDS Record missing SerialChangeNumber'


def test_scn_temporal_missing(full_pds_record_data):
    del full_pds_record_data[PDSRecordPaths.CONFIDENTIALITY][0][PDSRecordPaths.SERIAL_CHANGE_NUMBER]
    with pytest.raises(PDSDataImportException) as excinfo:
        PDSRecord.from_mesh_line('20160223171219', 123, '12341234567',
                                 json.dumps(full_pds_record_data))
    assert str(excinfo.value) == 'key: 12341234567 - PDS Temporal item missing scn'


###############################################################################
#
# Date Validation tests
#
###############################################################################
@pytest.mark.parametrize('the_date, expected_exc_msg', [
    ('2017 Jan 01', 'key: 9912004345 - PDS Temporal item invalid from, item: "2017 Jan 01"'),
    (float(0.001), 'key: 9912004345 - PDS Temporal item invalid from, item: "0.001"'),
    (0o01, 'key: 9912004345 - PDS Temporal item invalid from, item: "1"'),
    (99999999, 'key: 9912004345 - PDS date item unrealistic "99999999"'),
    (18000100, 'key: 9912004345 - PDS date item unrealistic "18000100"'),
])
def test_date_validation(full_pds_record_data, the_date, expected_exc_msg):
    full_pds_record_data[PDSRecordPaths.CONFIDENTIALITY][0][PDSRecordPaths.EFFECTIVE_FROM] = the_date
    with pytest.raises(PDSDataImportException) as excinfo:
        _create_pds_record(full_pds_record_data)
    assert str(excinfo.value) == expected_exc_msg


@pytest.mark.parametrize('the_date, expected', [
    ('201403', 20140301),
    ('201404', 20140401),
    (201405, 20140501),
    (20146, 20140601),
    (2014, 20140101),
    (20140619, 20140619),
    ('20140619', 20140619),
    ('20140619134', 20140619),
])
def test_date_cleansing(full_pds_record_data, the_date, expected):
    full_pds_record_data[PDSRecordPaths.CONFIDENTIALITY][0][PDSRecordPaths.EFFECTIVE_FROM] = the_date
    record = _create_pds_record(full_pds_record_data)
    assert record.confidentiality()[1][PDSRecordPaths.EFFECTIVE_FROM] == expected


def test_date_missing_from(full_pds_record_data):
    del full_pds_record_data[PDSRecordPaths.CONFIDENTIALITY][0][PDSRecordPaths.EFFECTIVE_FROM]
    with pytest.raises(PDSDataImportException) as excinfo:
        _create_pds_record(full_pds_record_data)
    assert str(excinfo.value) == 'key: 9912004345 - PDS Temporal item missing from'


###############################################################################
#
# Email Address tests
#
###############################################################################
def test_email_address(full_pds_record: PDSRecord):
    assert full_pds_record.email_address() == 'hellomps@emailaddress.com'


def test_email_address_missing(full_pds_record_data):
    del full_pds_record_data[PDSRecordPaths.EMAIL_ADDRESS]
    record = _create_pds_record(full_pds_record_data)
    assert not record.email_address()


def test_email_address_empty(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.EMAIL_ADDRESS] = ''
    record = _create_pds_record(full_pds_record_data)
    assert record.email_address() == ''


###############################################################################
#
# Telephone tests
#
###############################################################################
def test_telephone(full_pds_record: PDSRecord):
    assert full_pds_record.telephone() == '(01234)567890'


def test_telephone_missing(full_pds_record_data):
    del full_pds_record_data[PDSRecordPaths.TELEPHONE]
    record = _create_pds_record(full_pds_record_data)
    assert not record.telephone()


def test_telephone_empty(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.TELEPHONE] = ''
    record = _create_pds_record(full_pds_record_data)
    assert record.telephone() == ''


###############################################################################
#
# Mobile Phone tests
#
###############################################################################
def test_mobile_phone(full_pds_record: PDSRecord):
    assert full_pds_record.mobile_phone() == '0703991991'


def test_mobile_phone_missing(full_pds_record_data):
    del full_pds_record_data[PDSRecordPaths.MOBILE_PHONE]
    record = _create_pds_record(full_pds_record_data)
    assert not record.mobile_phone()


def test_mobile_phone_empty(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.MOBILE_PHONE] = ''
    record = _create_pds_record(full_pds_record_data)
    assert record.mobile_phone() == ''


###############################################################################
#
# Name History tests
#
###############################################################################
def test_name_history(full_pds_record: PDSRecord):
    history = full_pds_record.name_history()
    assert len(history) == 2
    # Check order (most recent first)
    assert history[0][PDSRecordPaths.SERIAL_CHANGE_NUMBER] == 5
    assert history[1][PDSRecordPaths.SERIAL_CHANGE_NUMBER] == 4


def test_name_history_missing(full_pds_record_data: MutableMapping):
    del full_pds_record_data[PDSRecordPaths.NAME_HISTORY]
    record = _create_pds_record(full_pds_record_data)
    assert record.name_history() == []


def test_name_history_empty(full_pds_record_data: MutableMapping):
    full_pds_record_data[PDSRecordPaths.NAME_HISTORY] = []
    record = _create_pds_record(full_pds_record_data)
    assert record.name_history() == []


def test_family_name_at(full_pds_record: PDSRecord):
    # Before
    assert not full_pds_record.family_name_at(_at(20100111000000))

    # First
    assert full_pds_record.family_name_at(_at(20100112000000)) == 'SINIAKOVA'
    assert full_pds_record.family_name_at(_at(20141222235959)) == 'SINIAKOVA'

    # Second
    assert full_pds_record.family_name_at(_at(20141223000000)) == 'SINIAKOVAK'
    assert full_pds_record.family_name_at(_at(20150506000000)) == 'SINIAKOVAK'

    # Confidentiality Invalid test
    assert not full_pds_record.family_name_at(_at(20150507000000))


def test_given_name_at(full_pds_record: PDSRecord):
    # Before
    assert not full_pds_record.given_names_at(_at(20100111000000))

    # First
    assert full_pds_record.given_names_at(_at(20100112000000)) == ['LUDMILA', '']
    assert full_pds_record.given_names_at(_at(20141222235959)) == ['LUDMILA', '']

    # Second
    assert full_pds_record.given_names_at(_at(20141223000000)) == ['LUDMILLIA', 'ARIEL']
    assert full_pds_record.given_names_at(_at(20150506000000)) == ['LUDMILLIA', 'ARIEL']

    # Confidentiality Invalid test
    assert not full_pds_record.given_names_at(_at(20150507000000))


def test_given_name_at_v2(full_pds_record_v2: PDSRecord):
    # Before
    assert not full_pds_record_v2.given_names_at(_at(20100111000000))

    # First
    assert full_pds_record_v2.given_names_at(_at(20100112000000)) == ['LUDMILA']
    assert full_pds_record_v2.given_names_at(_at(20141222235959)) == ['LUDMILA']

    # Second
    assert full_pds_record_v2.given_names_at(_at(20141223000000)) == ['LUDMILLIA', 'ARIEL', 'A']
    assert full_pds_record_v2.given_names_at(_at(20150506000000)) == ['LUDMILLIA', 'ARIEL', 'A']

    # Confidentiality Invalid test
    assert not full_pds_record_v2.given_names_at(_at(20150507000000))

###############################################################################
#
# MPS ID Details tests
#
###############################################################################
def test_mps_id_details(full_pds_record: PDSRecord):
    details = full_pds_record.mps_id_details()
    assert len(details) == 2
    # Submitted order
    assert details[0][PDSRecordPaths.MPS_ID] == 'X110000001'
    assert details[1][PDSRecordPaths.MPS_ID] == 'X928382011'
    assert details[0][PDSRecordPaths.LOCAL_PATIENT_ID] == 'X000000001'
    assert details[1][PDSRecordPaths.LOCAL_PATIENT_ID] == 'X000000002'


def test_mps_id_details_missing(full_pds_record_data: MutableMapping):
    del full_pds_record_data[PDSRecordPaths.MPS_ID_DETAILS]
    record = _create_pds_record(full_pds_record_data)
    assert record.mps_id_details() == []


def test_mps_id_details_empty(full_pds_record_data: MutableMapping):
    full_pds_record_data[PDSRecordPaths.MPS_ID_DETAILS] = []
    record = _create_pds_record(full_pds_record_data)
    assert record.mps_id_details() == []


def test_mps_id_details_missing_mps_id(full_pds_record_data: MutableMapping):
    del full_pds_record_data[PDSRecordPaths.MPS_ID_DETAILS][0][PDSRecordPaths.MPS_ID]
    with pytest.raises(PDSDataImportException) as excinfo:
        _create_pds_record(full_pds_record_data)
    assert str(excinfo.value) == 'key: 9912004345 - PDS Record MPS ID detail missing mpsID'


###############################################################################
#
# Address History tests
#
###############################################################################
def test_address_history(full_pds_record: PDSRecord):
    history = full_pds_record.address_history()
    assert len(history) == 2
    # Check order (most recent first)
    assert history[0][PDSRecordPaths.SERIAL_CHANGE_NUMBER] == 5
    assert history[1][PDSRecordPaths.SERIAL_CHANGE_NUMBER] == 4


def test_address_history_missing(full_pds_record_data: MutableMapping):
    del full_pds_record_data[PDSRecordPaths.ADDRESS_HISTORY]
    record = _create_pds_record(full_pds_record_data)
    assert record.address_history() == []


def test_address_history_empty(full_pds_record_data: MutableMapping):
    full_pds_record_data[PDSRecordPaths.ADDRESS_HISTORY] = []
    record = _create_pds_record(full_pds_record_data)
    assert record.address_history() == []


def test_postcode_at(full_pds_record: PDSRecord):
    # Before
    assert not full_pds_record.postcode_at(_at(20100111000000))

    # First
    assert full_pds_record.postcode_at(_at(20120313000000)) == 'M19 3HD'
    assert full_pds_record.postcode_at(_at(20150502235959)) == 'M19 3HD'

    # Second
    assert full_pds_record.postcode_at(_at(20150503000000)) == 'LS1 4BT'
    assert full_pds_record.postcode_at(_at(20150506000000)) == 'LS1 4BT'

    # Confidentiality Invalid test
    assert not full_pds_record.postcode_at(_at(20150507000000))

def test_postcode_at_v2(full_pds_record_v2: PDSRecord):
    # Before
    assert not full_pds_record_v2.postcode_at(_at(20100111000000))

    # First
    assert full_pds_record_v2.postcode_at(_at(20120313000000)) == 'M19 3HD'
    assert full_pds_record_v2.postcode_at(_at(20150502235959)) == 'M19 3HD'

    # Second
    assert full_pds_record_v2.postcode_at(_at(20150503000000)) == 'LS1 4BT'
    assert full_pds_record_v2.postcode_at(_at(20150506000000)) == 'LS1 4BT'

    # Confidentiality Invalid test
    assert not full_pds_record_v2.postcode_at(_at(20150507000000))


def test_address_at(full_pds_record: PDSRecord):
    # Before
    assert not full_pds_record.address_at(_at(20100111000000))

    # First
    assert full_pds_record.address_at(_at(20120313000000)) == ['', '236 DSP MPS ROAD', 'DSP', 'MPS', '']
    assert full_pds_record.address_at(_at(20150502235959)) == ['', '236 DSP MPS ROAD', 'DSP', 'MPS', '']

    # Second
    assert full_pds_record.address_at(_at(20150503000000)) == ['My House', '14 Some Street', 'This Area', 'That town',
                                                               'County']
    assert full_pds_record.address_at(_at(20150506000000)) == ['My House', '14 Some Street', 'This Area', 'That town',
                                                               'County']

    # Confidentiality Invalid test
    assert not full_pds_record.address_at(_at(20150507000000))


def test_address_history_v2(full_pds_record_v2: PDSRecord):
    # Act
    history = full_pds_record_v2.address_history()

    # Assert
    assert len(history) == 2
    # Check order (most recent first)
    assert history[0][PDSRecordPaths.SERIAL_CHANGE_NUMBER] == 5
    assert history[1][PDSRecordPaths.SERIAL_CHANGE_NUMBER] == 4


def test_address_at_v2(full_pds_record_v2: PDSRecord):
    # Before
    assert not full_pds_record_v2.address_at(_at(20100111000000))

    # First
    assert full_pds_record_v2.address_at(_at(20120313000000)) == ['236 DSP MPS ROAD', 'DSP', 'MPS', '', '']
    assert full_pds_record_v2.address_at(_at(20150502235959)) == ['236 DSP MPS ROAD', 'DSP', 'MPS', '', '']

    # Second
    assert full_pds_record_v2.address_at(_at(20150503000000)) == ['My House', '14 Some Street', 'This Area', 'That Town',
                                                               'County']
    assert full_pds_record_v2.address_at(_at(20150506000000)) == ['My House', '14 Some Street', 'This Area', 'That Town',
                                                               'County']

    # Confidentiality Invalid test
    assert not full_pds_record_v2.address_at(_at(20150507000000))

###############################################################################
#
# Date of Birth tests
#
###############################################################################
def test_date_of_birth(full_pds_record: PDSRecord):
    assert full_pds_record.date_of_birth() == 20040129


def test_date_of_birth_missing(full_pds_record_data):
    del full_pds_record_data[PDSRecordPaths.DOB]
    with pytest.raises(PDSDataImportException) as excinfo:
        _create_pds_record(full_pds_record_data)
    assert str(excinfo.value) == 'key: 9912004345 - PDS Record missing dob'


def test_date_of_birth_empty(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.DOB] = ''
    with pytest.raises(PDSDataImportException) as excinfo:
        _create_pds_record(full_pds_record_data)
    assert str(excinfo.value) == 'key: 9912004345 - PDS Record invalid dob, item: ""'


def test_date_of_birth_too_early(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.DOB] = '17991231'
    with pytest.raises(PDSDataImportException) as excinfo:
        _create_pds_record(full_pds_record_data)
    assert str(excinfo.value) == 'key: 9912004345 - PDS date item unrealistic "17991231"'


def test_date_of_birth_earliest(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.DOB] = '18000101'
    record = _create_pds_record(full_pds_record_data)
    assert record.date_of_birth() == 18000101


def test_date_of_birth_latest(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.DOB] = '30001231'
    record = _create_pds_record(full_pds_record_data)
    assert record.date_of_birth() == 30001231


def test_date_of_birth_too_late(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.DOB] = '30010101'
    with pytest.raises(PDSDataImportException) as excinfo:
        _create_pds_record(full_pds_record_data)
    assert str(excinfo.value) == 'key: 9912004345 - PDS date item unrealistic "30010101"'


###############################################################################
#
# Date of Death tests
#
###############################################################################
def test_data_of_death_missing(full_pds_record_data):
    del full_pds_record_data[PDSRecordPaths.DOD]
    record = _create_pds_record(full_pds_record_data)
    assert not record.date_of_death()


def test_data_of_death_empty(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.DOD] = ''
    record = _create_pds_record(full_pds_record_data)
    assert not record.date_of_death()


def test_data_of_death_too_early(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.DOD] = '17991231'
    with pytest.raises(PDSDataImportException) as excinfo:
        _create_pds_record(full_pds_record_data)
    assert str(excinfo.value) == 'key: 9912004345 - PDS date item unrealistic "17991231"'


def test_data_of_death_earliest(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.DOD] = '18000101'
    record = _create_pds_record(full_pds_record_data)
    assert record.date_of_death() == 18000101


def test_data_of_death_latest(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.DOD] = '30001231'
    record = _create_pds_record(full_pds_record_data)
    assert record.date_of_death() == 30001231


def test_data_of_death_too_late(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.DOD] = '30010101'
    with pytest.raises(PDSDataImportException) as excinfo:
        _create_pds_record(full_pds_record_data)
    assert str(excinfo.value) == 'key: 9912004345 - PDS date item unrealistic "30010101"'


###############################################################################
#
# Migrant Data tests
#
###############################################################################
def test_migrant_data(full_pds_record: PDSRecord):
    history = full_pds_record.migrant_data()
    assert len(history) == 2
    # Check order (most recent first)
    assert history[0][PDSRecordPaths.SERIAL_CHANGE_NUMBER] == 5
    assert history[1][PDSRecordPaths.SERIAL_CHANGE_NUMBER] == 4


def test_migrant_data_missing(full_pds_record_data: MutableMapping):
    del full_pds_record_data[PDSRecordPaths.MIGRANT_DATA]
    record = _create_pds_record(full_pds_record_data)
    assert record.migrant_data() == []


def test_migrant_data_empty(full_pds_record_data: MutableMapping):
    full_pds_record_data[PDSRecordPaths.MIGRANT_DATA] = []
    record = _create_pds_record(full_pds_record_data)
    assert record.migrant_data() == []


def test_migrant_data_visa_status_at(full_pds_record: PDSRecord):
    # Before
    assert not full_pds_record.visa_status_at(_at(20121013000000))

    # First
    assert full_pds_record.visa_status_at(_at(20121014000000)) == '01'
    assert full_pds_record.visa_status_at(_at(20130203235959)) == '01'

    # Handle gaps
    assert not full_pds_record.visa_status_at(_at(20130204000000))
    assert not full_pds_record.visa_status_at(_at(20140810235959))

    # Second
    assert full_pds_record.visa_status_at(_at(20140811000000)) == '01'
    assert full_pds_record.visa_status_at(_at(20150506000000)) == '01'

    # Confidentiality Invalid test
    assert not full_pds_record.visa_status_at(_at(20150507000000))


def test_migrant_data_missing_home_office_ref(full_pds_record_data: MutableMapping):
    del full_pds_record_data[PDSRecordPaths.MIGRANT_DATA][0][PDSRecordPaths.HOME_OFFICE_REF_NO]

    with pytest.raises(PDSDataImportException) as excinfo:
        _create_pds_record(full_pds_record_data)

    assert str(excinfo.value) == 'key: 9912004345 - PDS Temporal item missing home_office_ref_no'


def test_migrant_data_missing_visa_status(full_pds_record_data: MutableMapping):
    del full_pds_record_data[PDSRecordPaths.MIGRANT_DATA][0][PDSRecordPaths.VISA_STATUS]

    _create_pds_record(full_pds_record_data)


def test_migrant_data_missing_visa_from(full_pds_record_data: MutableMapping):
    del full_pds_record_data[PDSRecordPaths.MIGRANT_DATA][0][PDSRecordPaths.VISA_FROM]

    _create_pds_record(full_pds_record_data)


###############################################################################
#
# Sensitivity tests
#
###############################################################################
def test_sensitive_flag(full_pds_record: PDSRecord):
    assert full_pds_record.record_dict[PDSRecordPaths.SENSITIVE]


def test_sensitive_at(full_pds_record: PDSRecord):
    assert not full_pds_record.sensitive_at(_at(20141011235959))
    assert full_pds_record.sensitive_at(_at(20141012000000))
    assert full_pds_record.sensitive_at(_at(20150506235959))
    assert not full_pds_record.sensitive_at(_at(20150507000000))


def test_sensitive_at_legacy(full_pds_record_data: MutableMapping):
    assert full_pds_record_data[PDSRecordPaths.CONFIDENTIALITY][0][PDSRecordPaths.VAL] == 'S'
    full_pds_record_data[PDSRecordPaths.CONFIDENTIALITY][0][PDSRecordPaths.VAL] = 'Y'
    record = _create_pds_record(full_pds_record_data)

    assert not record.sensitive_at(_at(20141011235959))
    assert record.sensitive_at(_at(20141012000000))
    assert record.sensitive_at(_at(20150506235959))
    assert not record.sensitive_at(_at(20150507000000))


###############################################################################
#
# Invalid tests
#
###############################################################################
def test_invalid_at(full_pds_record: PDSRecord):
    assert not full_pds_record.invalid_at(_at(20141011235959))
    assert not full_pds_record.invalid_at(_at(20141012000000))
    assert not full_pds_record.invalid_at(_at(20150506235959))
    assert full_pds_record.invalid_at(_at(20150507000000))


def test_invalid_not_set(full_pds_record_data: MutableMapping):
    del full_pds_record_data[PDSRecordPaths.CONFIDENTIALITY][1]
    record = _create_pds_record(full_pds_record_data)

    assert not record.invalid_at(_at(20150507000000))
    assert record.visa_status_at(_at(20150507000000)) == '01'
    assert record.gp_code_at(_at(29990506000000)) == 'D34099'
    assert record.gender_at(_at(29990506000000)) == '1'
    assert record.family_name_at(_at(29990506000000)) == 'SINIAKOVAK'
    assert record.given_names_at(_at(29990506000000)) == ['LUDMILLIA', 'ARIEL']


###############################################################################
#
# Gender History tests
#
###############################################################################
def test_gender_history(full_pds_record: PDSRecord):
    history = full_pds_record.gender_history()
    assert len(history) == 2
    # Check order (most recent first)
    assert history[0][PDSRecordPaths.SERIAL_CHANGE_NUMBER] == 5
    assert history[1][PDSRecordPaths.SERIAL_CHANGE_NUMBER] == 4


def test_gender_history_missing(full_pds_record_data: MutableMapping):
    del full_pds_record_data[PDSRecordPaths.GENDER_HISTORY]
    with pytest.raises(PDSDataImportException) as excinfo:
        _create_pds_record(full_pds_record_data)
    assert str(excinfo.value) == 'key: 9912004345 - PDS Record missing an entry in gender_history'


def test_gender_history_at(full_pds_record: PDSRecord):
    assert not full_pds_record.gender_at(_at(20110315235959))

    assert full_pds_record.gender_at(_at(20110316000000)) == '2'
    assert full_pds_record.gender_at(_at(20120331235959)) == '2'

    assert full_pds_record.gender_at(_at(20120401000000)) == '1'
    assert full_pds_record.gender_at(_at(20150506235959)) == '1'

    assert not full_pds_record.gender_at(_at(20150507000000))


def test_gender_history_empty(full_pds_record_data: MutableMapping):
    full_pds_record_data[PDSRecordPaths.GENDER_HISTORY] = []
    with pytest.raises(PDSDataImportException) as excinfo:
        _create_pds_record(full_pds_record_data)
    assert str(excinfo.value) == 'key: 9912004345 - PDS Record missing an entry in gender_history'


###############################################################################
#
# Replaced By tests
#
###############################################################################
def test_replaced_by(full_pds_record: PDSRecord):
    assert full_pds_record.replaced_by_nhs_number() == None


def test_replaced_by_missing(full_pds_record_data: MutableMapping):
    del full_pds_record_data[PDSRecordPaths.REPLACED_BY]
    record = _create_pds_record(full_pds_record_data)
    assert not record.replaced_by_nhs_number()


def test_replaced_by_empty(full_pds_record_data: MutableMapping):
    full_pds_record_data[PDSRecordPaths.REPLACED_BY] = ''
    record = _create_pds_record(full_pds_record_data)
    assert record.replaced_by_nhs_number() == ''


###############################################################################
#
# GP Codes tests
#
###############################################################################
def test_gp_codes(full_pds_record_data: MutableMapping):
    gp_codes = [
        {
            "from": 20140101,
            "to": "20140101",
            "val": "1234",
            "scn": 1
        },
        {
            "from": 20140102,
            "to": "20140103",
            "val": "2345",
            "scn": 2
        },
        {
            "from": "20140103",
            "to": "20140104",
            "val": "3456",
            "scn": 3
        },
        {
            "from": 20140106,
            "to": 20140107,
            "val": "4567",
            "scn": 4
        },
        {
            "from": "20140201",
            "val": "CURRENT",
            "scn": 5
        },
    ]
    full_pds_record_data[PDSRecordPaths.GP_CODES] = gp_codes
    record = _create_pds_record(full_pds_record_data)

    assert not record.gp_code_at(_at(20131231235959))

    assert record.gp_code_at(_at(20140101000000)) == '1234'
    assert record.gp_code_at(_at(20140101235959)) == '1234'

    assert record.gp_code_at(_at(20140102000000)) == '2345'
    assert record.gp_code_at(_at(20140102235959)) == '2345'

    assert record.gp_code_at(_at(20140103000000)) == '3456'
    assert record.gp_code_at(_at(20140104235959)) == '3456'

    assert not record.gp_code_at(_at(20140105000000))
    assert not record.gp_code_at(_at(20140105235959))

    assert record.gp_code_at(_at(20140106000000)) == '4567'
    assert record.gp_code_at(_at(20140107235959)) == '4567'

    assert not record.gp_code_at(_at(20140108000000))
    assert not record.gp_code_at(_at(20140131235959))

    assert record.gp_code_at(_at(20140201000000)) == 'CURRENT'

    # Invalid
    assert not record.gp_code_at(_at(20160506000000))


###############################################################################
#
# Confidentiality tests
#
###############################################################################
def test_confidentiality(full_pds_record: PDSRecord):
    assert len(full_pds_record.confidentiality()) == 2
    assert full_pds_record.confidentiality()[0][PDSRecordPaths.SERIAL_CHANGE_NUMBER] == 5
    assert full_pds_record.confidentiality()[1][PDSRecordPaths.SERIAL_CHANGE_NUMBER] == 4


def test_confidentiality_missing(full_pds_record_data: MutableMapping):
    del full_pds_record_data[PDSRecordPaths.CONFIDENTIALITY]
    record = _create_pds_record(full_pds_record_data)
    assert record.confidentiality() == []


def test_confidentiality_empty(full_pds_record_data: MutableMapping):
    full_pds_record_data[PDSRecordPaths.CONFIDENTIALITY] = []
    record = _create_pds_record(full_pds_record_data)
    assert record.confidentiality() == []


def test_confidentiality_at(full_pds_record: PDSRecord):
    assert not full_pds_record.confidentiality_at(_at(20141011235959))
    assert full_pds_record.confidentiality_at(_at(20141012000000)) == 'S'
    assert full_pds_record.confidentiality_at(_at(20150506235959)) == 'S'
    assert full_pds_record.confidentiality_at(_at(20150507000000)) == 'I'
    assert full_pds_record.confidentiality_at(_at(29990101000000)) == 'I'


###############################################################################
#
# Death Status tests
#
###############################################################################
def test_death_status(full_pds_record):
    assert full_pds_record.death_status() == "1"


def test_death_status_missing(full_pds_record_data):
    del full_pds_record_data[PDSRecordPaths.DEATH_STATUS]
    record = _create_pds_record(full_pds_record_data)
    assert not record.death_status()


def test_death_status_empty(full_pds_record_data):
    full_pds_record_data[PDSRecordPaths.DEATH_STATUS] = ''
    record = _create_pds_record(full_pds_record_data)
    assert not record.death_status() == ''


###############################################################################
#
# Builder tests
#
###############################################################################
def test_build_record(full_pds_record: PDSRecord):
    orig = full_pds_record.to_json()
    record = PDSRecord.build_record(json.loads(orig))
    new = record.to_json()
    assert orig == new


def test_build_jayne_pipey():
    parsed = PDSRecord.from_json(
        '{"name_history":[{"scn":16,"givenName":[{"name":"Jane|Pipey"}],"from":20180521,"familyName":"Bow"},{"scn":17,"givenName":[{"name":"Janeinactive"}],"from":20160521,"to":20170521,"familyName":"Bowinactive"},{"scn":13,"givenName":[{"name":"Janebaddata"}],"from":20170521,"familyName":"Bow"}],"dob":20180415,"address_history":[{"postalCode":"BD7 3AD","scn":16,"from":20190225,"addr":[{"line":"270"},{"line":""},{"line":"Great Horton Road"},{"line":"Bradford"},{"line":""}]},{"postalCode":"LS1 4BT","scn":1,"from":20180521,"addr":[{"line":"Bt Global Services"},{"line":"1 Sovereign Street"},{"line":""},{"line":"LEEDS"},{"line":""}]},{"postalCode":"EX24 6EJ","to":20190220,"scn":3,"from":20180605,"addr":[{"line":"BRAMLEY FARM"},{"line":"2 MAIN ROAD"},{"line":"FARWAY"},{"line":"COLYTON"},{"line":"DEVON"}]},{"postalCode":"EX24 6EJ","to":20190220,"scn":4,"from":20190220,"addr":[{"line":"BRAMLEY FARM"},{"line":"2 MAIN ROAD"},{"line":""},{"line":"COLYTON"},{"line":""}]},{"postalCode":"BS10 5EN","to":20190220,"scn":5,"from":20190220,"addr":[{"line":""},{"line":"270 Southmead Road"},{"line":"Westbury-on-Trym"},{"line":"Bristol"},{"line":""}]},{"postalCode":"BS10 5EN","to":20190220,"scn":6,"from":20190220,"addr":[{"line":""},{"line":"270 Southmead Road"},{"line":"Westbury"},{"line":"Bristol"},{"line":""}]},{"postalCode":"BS10 5EN","to":20190225,"scn":7,"from":20190220,"addr":[{"line":"27 Southmead"},{"line":""},{"line":"Westbury"},{"line":"Bristol"},{"line":""}]},{"postalCode":"BS10 5EN","to":20190225,"scn":8,"from":20190225,"addr":[{"line":""},{"line":"27 Southmead Road"},{"line":"Southmead"},{"line":"Bristol"},{"line":""}]},{"postalCode":"BD7 3AD","to":20190225,"scn":9,"from":20190225,"addr":[{"line":""},{"line":"240 Swinton Place"},{"line":""},{"line":"Bradford"},{"line":"West Yorkshire"}]},{"postalCode":"BD7 3AD","to":20190225,"scn":10,"from":20190225,"addr":[{"line":""},{"line":"240 Swint Place"},{"line":""},{"line":"Bradford"},{"line":"West Yorkshire"}]},{"postalCode":"BD7 3AD","to":20190225,"scn":11,"from":20190225,"addr":[{"line":""},{"line":"270 swi place"},{"line":"Great Horton Road"},{"line":"Bradford"},{"line":""}]},{"postalCode":"BD7 3AD","to":20190225,"scn":12,"from":20190225,"addr":[{"line":""},{"line":"270 s place"},{"line":"Great Horton Road"},{"line":"Bradford"},{"line":""}]},{"postalCode":"BD7 3AD","to":20190225,"scn":13,"from":20190225,"addr":[{"line":""},{"line":"270 place"},{"line":"Great Horton Road"},{"line":"Bradford"},{"line":""}]},{"postalCode":"BD7 3AD","to":20190225,"scn":14,"from":20190225,"addr":[{"line":""},{"line":"270 place"},{"line":"Great Horton"},{"line":"Bradford"},{"line":""}]},{"postalCode":"BD7 3AD","to":20190225,"scn":17,"from":20190225,"addr":[{"line":"270DoorNo"},{"line":""},{"line":"Great Horton Road"},{"line":"Bradford"},{"line":""}]}],"gender_history":[{"gender":"2","scn":16,"from":20180521},{"gender":"1","scn":1,"from":20170521},{"gender":"1","scn":17,"from":20170521,"to":20170522}],"gp_codes":[{"scn":16,"from":20180605,"val":"B82020"},{"scn":1,"from":20170605,"val":"B82020"},{"scn":17,"from":20170605,"to":20170606,"val":"B82020"}],"sensitive":true,"death_status":"2","mpsid_details":[{"mpsID":"mps_id_test1","scn": 69,"localPatientID":"lpi_test1"}]}')

    assert parsed


###############################################################################
#
# Schema tests
#
###############################################################################
def test_create_pds_record_with_actual_schema(full_pds_record_data: MutableMapping):
    string_pds_record_data = json.dumps(full_pds_record_data)

    # Record is parsed and validated
    record = PDSRecord.from_mesh_line(
        '20210705112010',
        4,
        '9912004345',
        string_pds_record_data,
        mandatory_field_check=True
    )

    name_history = record.name_history()

    assert len(name_history) == 2
    assert name_history[0][PDSRecordPaths.FAMILY_NAME] == "SINIAKOVAK"


def test_create_pds_record_with_different_schema(pds_record_data_with_different_schema: MutableMapping):
    string_pds_record_data = json.dumps(pds_record_data_with_different_schema)

    # Record is parsed and validated with a different schema
    record = PDSRecord.from_mesh_line(
        '20210705112010',
        1,
        '9912004348',
        string_pds_record_data,
        mandatory_field_check=True
    )

    name_history = record.name_history()
    address_history = record.address_history()

    assert len(name_history) == 2
    assert len(address_history) == 2
    assert name_history[0][PDSRecordPaths.FAMILY_NAME] == "SINIAKOVAK"
    assert name_history[0]["prefix"] == "MS"
    assert address_history[0]["addressKey"] == "A"
    assert name_history[0]["randomKey"] == "abc"