import random
import string
from datetime import datetime
from typing import Any, Mapping, Optional, Dict, Tuple

import pytest
from dsp.common import verhoeff
from dsp.udfs.misc import is_valid_nhs_number, is_valid_snomed_ct, sorted_json_string, \
    timestamp_to_time_from_unix_epoch, datetime_to_iso8601_string, geography_codes_of_residence_and_registration, \
    RESIDENCE_AND_REGISTRATION_COLUMNS, expand_pds_record_at, EXPANDED_PDS_RECORD_COLUMNS, \
    nhs_number_status_description, patient_source_setting_description, suppress_value
from dsp.udfs.referring_org import is_empty_or_valid_referring_org
from nhs_reusable_code_library.standard_data_validations.nhsNumberValidation.validate_nhsnumber import validate_nhsnumber, validate_nhsnumber_palindrome


@pytest.mark.parametrize("org_code, point_in_time, expected", [
    (None, '20180101', True),  # if None returns true as it is not mandatory field
    ("3", '20180101', False),
    ("", '20180101', True),  # if empty returns true as it is not mandatory field
    (" ", '20180101', True),  # if space returns true as it is not mandatory field
    ("V12505", '20180101', True),  # valid open org
    ("H82059", '20120101', True),  # valid open org
    ("4AY97", '20010101', True),  # to': 20010331, 'from': 20001001
    ("X99998", None, True),  # X99998 - Referring ORGANISATION CODE not applicable
    ("X99999", None, True),  # X99999 - Referring ORGANISATION CODE is a default code
    ("X99999 ", None, True),  # Trailing whitespace with default code should pass validation
    (" X99999", None, True),  # Leading whitespace default code should pass validation
    (" X99999 ", None, True),  # Leading and trailing whitespace with valid code should pass validation
    ("V12505", None, True),  # Valid code with point_in_time = None
    ("V12505", '20090331', False),  # Valid code outside applicable period('from': 20090401)
    (None, None, True),  # Validation should pass if all parameters are None
    ("5PW", None, True)  # Valid code with point_in_time = None
])
def test_is_empty_valid_referring_org(org_code: str, point_in_time: str, expected: bool):
    assert expected == is_empty_or_valid_referring_org(org_code, point_in_time)


# @pytest.mark.parametrize("nhs_num, expected, palindrome_check", [
#     ("123", False, True),
#     ("4010232137", True, True),
#     ("40102321307", False, True),  # too long
#     ("1234567890", False, True),   # invalid check digit 10
#     ("1234554321", False, True),
#     ("1234554321", True, False),
#     ("9345665439", True, False),
#     ("0000000000", True, False),
# ])
# def test_is_valid_nhs_number(nhs_num: str, expected: bool, palindrome_check: bool):
#     assert expected == is_valid_nhs_number(nhs_num, palindrome_check)
#     assert expected == validate_nhsnumber_palindrome(nhs_num, palindrome_check)


@pytest.mark.parametrize("nhs_num, expected", [
    ("123", False),
    ("4010232137", True),
    ("40102321307", False),  # too long
    ("1234567890", False),   # invalid check digit 10
    ("1234554321", False),
    #("1234554321", True),
    ("9345665439", False),
    ("0000000000", False),
])
def test_is_valid_nhs_number(nhs_num: str, expected: bool):
    assert expected == is_valid_nhs_number(nhs_num) #, palindrome_check)
    #assert expected == validate_nhsnumber(nhs_num) #, palindrome_check)


@pytest.mark.parametrize("nhs_number_status_ind, expected", [
    ('01','Number present and verified'),
    ('02','Number present but not traced'),
    ('03','Trace required'),
    ('04','Trace attempted - No match or multiple match found'),
    ('05','Trace needs to be resolved (NHS Number or patient detail conflict)'),
    ('06','Trace in progress'),
    ('07','Number not present and trace not required'),
    ('08','Trace postponed (baby under six weeks old)'),
    # ('09','This is just testing'),
    # ('','This is just another test'),
    (None, None)

])
def test_nhs_number_status_description(nhs_number_status_ind, expected):
    nhs_status_description = nhs_number_status_description(nhs_number_status_ind)
    assert expected == nhs_status_description


@pytest.mark.parametrize("patient_source_setting, expected", [
    ('01','Admitted Patient Care - Inpatient (this Health Care Provider)'),
    ('02','Admitted Patient Care - Day case (this Health Care Provider)'),
    ('03','Out-patient (this Health Care Provider)'),
    ('04','GP Direct Access'),
    ('05','Accident and Emergency Department (this Health Care Provider)'),
    ('06','Other Health Care Provider'),
    ('07','Other'),
    (None, None)
])

def test_patient_source_setting_description(patient_source_setting, expected):
    patient_source_description = patient_source_setting_description(patient_source_setting)
    assert expected == patient_source_description


@pytest.mark.parametrize("input_data, expected", [
    # already in order
    ({'a': 9, 'b': 4, 'c': 7}, '{"a": 9, "b": 4, "c": 7}'),
    # out of order
    ({'b': 4, 'c': 7, 'a': 9}, '{"a": 9, "b": 4, "c": 7}'),
    # nested in order
    ({'a': 9, 'b': 4, 'c': 7, 'd': {'e': 4, 'f': 2}}, '{"a": 9, "b": 4, "c": 7, "d": {"e": 4, "f": 2}}'),
    # nested out of order second level
    ({'a': 9, 'b': 4, 'c': 7, 'd': {'f': 2, 'e': 4}}, '{"a": 9, "b": 4, "c": 7, "d": {"e": 4, "f": 2}}'),
    # complex for coverage
    ({
         'b': 4,
         'a': 9,
         'd': {
             'b': 2,
             'i': {
                 'z': {
                     '9': 'q',
                     '5': 2,
                     '6': 't'
                 },
                 'p': 4,
                 'n': {
                     'w': '3'
                 },
                 'a': {}
             },
             'h': 's',
             'a': 4
         },
         'c': 7
     },
     '{"a": 9, "b": 4, "c": 7, "d": '
     '{"a": 4, "b": 2, "h": "s", "i": {"a": {}, "n": {"w": "3"}, "p": 4, "z": {"5": 2, "6": "t", "9": "q"}}}'
     '}')

])
def test_sorted_json_string(input_data: Mapping[Any, Any], expected: str):
    actual = sorted_json_string(input_data)
    assert actual == expected


@pytest.mark.parametrize("dt, expected", [
    (datetime(2018, 2, 3, 12, 15, 30), datetime(1970, 1, 1, 12, 15, 30)),
    (None, None),
])
def test_timestamp_to_time_from_unix_epoch(dt: datetime, expected: Optional[datetime]):
    assert expected == timestamp_to_time_from_unix_epoch(dt)


@pytest.mark.parametrize("dt, expected", [
    (datetime(2015, 7, 1, 10, 15, 30), "2015-07-01T10:15:30"),
    (None, None),
])
def test_timestamp_to_time_from_unix_epoch(dt: datetime, expected: Optional[datetime]):
    assert expected == datetime_to_iso8601_string(dt)


def snomed_ct(length: int, valid: bool = True) -> str:
    base = ''.join(random.choices(string.digits, k=length-3)) + '10'
    checksum = verhoeff.checksum(base)
    if not valid:
        checksum = (checksum + 1) % 10
    snomed = base + str(checksum)

    assert len(snomed) == length
    assert verhoeff.verify(snomed) == valid
    return snomed


VALID_SNOMED_ATTRIBUTE = '363589002'
INVALID_SNOMED_ATTRIBUTE = '11111111'


@pytest.mark.parametrize("snomed_ct_args, min_length, max_length, expected", [
    ({"length":8}, 6, 18, True),  # check single snomed ct value
    ({"length":8, "valid":False}, 6, 18, False),  # invalid snomed ct value
    ({"length":18}, 6, 56, True),  # single snomed in post coordinated snomed ct column
    ({"length":10}, 10, 12, True),  # different lengths
    ({"length":12}, 10, 12, True),  # different lengths
    ({"length":9}, 10, 12, False),  # invalid length
    ({"length":13}, 10, 12, False),  # invalid length
    ({"length":5}, 1, 100, False),  # invalid length (6 to 18 for single snomed ct)
    ({"length":19}, 1, 100, False),  # invalid length (6 to 18 for single snomed ct)
])
def test_is_valid_snomed_ct__single_snomed_ct(snomed_ct_args: Dict[str, Any], min_length: int, max_length: int, expected: bool):
    _snomed_ct=snomed_ct(**snomed_ct_args)
    assert expected == is_valid_snomed_ct(_snomed_ct, min_length, max_length)


@pytest.mark.parametrize('pc_snomed_ct_args, expected, separators, prefix, sufix', [
    (({"length": 8}, VALID_SNOMED_ATTRIBUTE, {"length": 8}), True, None, None, None), # happy case
    (({"length": 6}, VALID_SNOMED_ATTRIBUTE, {"length": 6}), True, None, None, None),# minimal internal lengths
    (({"length": 18}, VALID_SNOMED_ATTRIBUTE, {"length": 18}), True, None, None, None),  # maximal internal lengths
    (({"length": 5}, VALID_SNOMED_ATTRIBUTE, {"length": 8}), False, None, None, None), # internal length
    (({"length": 8}, VALID_SNOMED_ATTRIBUTE, {"length": 5}), False, None, None, None),  # internal length
    (({"length": 19}, VALID_SNOMED_ATTRIBUTE, {"length": 8}), False, None, None, None), # internal length
    (({"length": 8}, VALID_SNOMED_ATTRIBUTE, {"length": 19}), False, None, None, None), # internal length
    (({"length": 8, "valid": False}, VALID_SNOMED_ATTRIBUTE, {"length": 8, "valid": False}), False, None, None, None), # invalid concept identifier
    (({"length": 8}, INVALID_SNOMED_ATTRIBUTE, {"length": 8}), False, None, None, None),# invalid attribute
    (({"length": 8}, VALID_SNOMED_ATTRIBUTE, {"length": 8, "valid": False}), False, None, None, None), # invalid value
    (({"length": 8}, VALID_SNOMED_ATTRIBUTE, {"length": 8}), False, ("=", ":"), None, None), # wrong separator
    (({"length": 8}, VALID_SNOMED_ATTRIBUTE, {"length": 8}), False, (" : ", " = "), None, None), # whitespace
    (({"length": 8}, VALID_SNOMED_ATTRIBUTE, {"length": 8}), False, None, " ", None), # whitespace (prefix)
    (({"length": 8}, VALID_SNOMED_ATTRIBUTE, {"length": 8}), False, None, None, " "), # whitespace (sufix)
])
def test_is_valid_snomed_ct__post_coordinated(
        pc_snomed_ct_args: Tuple[Dict[str, Any], str, Dict[str, Any]],
        expected: bool, separators: Optional[Tuple[str, str]],
        prefix: Optional[str], sufix: Optional[str]
):
    _sparators = separators if separators else (":", "=")
    _prefix = prefix if prefix else ""
    _sufix = sufix if sufix else ""
    pc_snomed_ct = f'{_prefix}{snomed_ct(**pc_snomed_ct_args[0])}{_sparators[0]}{pc_snomed_ct_args[1]}{_sparators[1]}{snomed_ct(**pc_snomed_ct_args[2])}{_sufix}'
    assert expected == is_valid_snomed_ct(pc_snomed_ct, 6, 56)



def test_expand_pds_record_at_with_no_record():
    # Arrange
    record = ""
    datetime_value = datetime(2010, 1, 1, 12, 0)

    expected_output = {col_name: None for col_name in EXPANDED_PDS_RECORD_COLUMNS}

    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_partial_valid_record():
    # Arrange
    record = """{"address_history":[{"addr":[],"from":20001018,"postalCode":"Test Post Code","scn":5}],"date":"20200101103400","dob":19050307,"gender_history":[{"from":20001020,"gender":"1","scn":5}],"gp_codes":[{"from":20160307,"scn":5,"val":"Test GP Code 2"},{"from":19170810,"scn":1,"to":20160307,"val":"Test GP Code 1"}],"name_history":[{"familyName":"family name","from":20000315,"givenName":[{"name":"given name 1"},{"name":"given name 2"}],"scn":5}],"nhs_number":"9454551671","scn":5}"""
    datetime_value = datetime(2010, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "Test Post Code",
        "PDS_DATE_OF_BIRTH": 19050307,
        "PDS_GENDER": "1",
        "PDS_GPCODE": "Test GP Code 1",
        "PDS_NHS_NUMBER": "9454551671",
        "PDS_ADDRESS": [],
        "PDS_DEATH_STATUS": None,
        "PDS_DATE_OF_DEATH": None,
        "PDS_FAMILY_NAME": "family name",
        "PDS_GIVEN_NAMES": ["given name 1", "given name 2"],
        "PDS_VISA_STATUS": None,
        "PDS_INVALID": False,
        "PDS_SENSITIVE": False,
        "PDS_CONFIDENTIALITY": None,
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_sensitive_and_confidentiality_record_in_date():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20160321,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20160510,"scn":1,"val":"Y"}],"date":"20170908160209","dob":20150429,"gender_history":[{"from":20150914,"gender":"1","scn":1}],"gp_codes":[{"from":20150630,"scn":1,"val":"A10000"}],"name_history":[{"familyName":"a nice surname","from":20150914,"givenName":[{"name":"a"},{"name":"b"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2017, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "AA1 BB2",
        "PDS_DATE_OF_BIRTH": 20150429,
        "PDS_GENDER": "1",
        "PDS_GPCODE": "A10000",
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": ["", "1 SOME ROAD", "", "SOME PLACE", "SOME CITY"],
        "PDS_DEATH_STATUS": None,
        "PDS_DATE_OF_DEATH": None,
        "PDS_FAMILY_NAME": "a nice surname",
        "PDS_GIVEN_NAMES": ["a", "b"],
        "PDS_VISA_STATUS": None,
        "PDS_INVALID": False,
        "PDS_SENSITIVE": True,
        "PDS_CONFIDENTIALITY": "Y",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_date_out_of_record_range():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20160321,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20160510,"scn":1,"val":"Y"}],"date":"20170908160209","dob":20150429,"gender_history":[{"from":20150914,"gender":"1","scn":1}],"gp_codes":[{"from":20150630,"scn":1,"val":"A10000"}],"name_history":[{"familyName":"a nice surname","from":20150914,"givenName":[{"name":"a"},{"name":"b"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2015, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": None,
        "PDS_DATE_OF_BIRTH": 20150429,
        "PDS_GENDER": None,
        "PDS_GPCODE": None,
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": None,
        "PDS_DEATH_STATUS": None,
        "PDS_DATE_OF_DEATH": None,
        "PDS_FAMILY_NAME": None,
        "PDS_GIVEN_NAMES": None,
        "PDS_VISA_STATUS": None,
        "PDS_INVALID": False,
        "PDS_SENSITIVE": False,
        "PDS_CONFIDENTIALITY": None,
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_full_record():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20010101,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20010101,"scn":1,"val":"Y"}],"date":"20010101000000","death_status":"1","dob":19000101,"dod":20100101,"gender_history":[{"from":19000101,"gender":"1","scn":1}],"gp_codes":[{"from":19000101,"to":19991231,"scn":1,"val":"A10000"}, {"from":20000101,"scn":1,"val":"B20000"}],"migrant_data":[{"visa_status":"02","brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"familyName":"surname","from":19000101,"givenName":[{"name":"a"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "AA1 BB2",
        "PDS_DATE_OF_BIRTH": 19000101,
        "PDS_GENDER": "1",
        "PDS_GPCODE": "B20000",
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": ["", "1 SOME ROAD", "", "SOME PLACE", "SOME CITY"],
        "PDS_DEATH_STATUS": "1",
        "PDS_DATE_OF_DEATH": 20100101,
        "PDS_FAMILY_NAME": "surname",
        "PDS_GIVEN_NAMES": ["a"],
        "PDS_VISA_STATUS": "02",
        "PDS_INVALID": False,
        "PDS_SENSITIVE": True,
        "PDS_CONFIDENTIALITY": "Y",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_full_invalid_record():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20010101,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20010101,"scn":1,"val":"I"}],"date":"20010101000000","death_status":"1","dob":19000101,"dod":20100101,"gender_history":[{"from":19000101,"gender":"1","scn":1}],"gp_codes":[{"from":19000101,"to":19991231,"scn":1,"val":"A10000"}, {"from":20000101,"scn":1,"val":"B20000"}],"migrant_data":[{"visa_status":"02","brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"familyName":"surname","from":19000101,"givenName":[{"name":"a"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": None,
        "PDS_DATE_OF_BIRTH": 19000101,
        "PDS_GENDER": None,
        "PDS_GPCODE": None,
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": None,
        "PDS_DEATH_STATUS": "1",
        "PDS_DATE_OF_DEATH": 20100101,
        "PDS_FAMILY_NAME": None,
        "PDS_GIVEN_NAMES": None,
        "PDS_VISA_STATUS": None,
        "PDS_INVALID": True,  # This is true because the confidentiality code is "I"
        "PDS_SENSITIVE": False,
        "PDS_CONFIDENTIALITY": "I",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_missing_visa_status_record():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20010101,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20010101,"scn":1,"val":"Y"}],"date":"20010101000000","death_status":"1","dob":19000101,"dod":20100101,"gender_history":[{"from":19000101,"gender":"1","scn":1}],"gp_codes":[{"from":19000101,"to":19991231,"scn":1,"val":"A10000"}, {"from":20000101,"scn":1,"val":"B20000"}],"migrant_data":[{"brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"familyName":"surname","from":19000101,"givenName":[{"name":"a"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "AA1 BB2",
        "PDS_DATE_OF_BIRTH": 19000101,
        "PDS_GENDER": "1",
        "PDS_GPCODE": "B20000",
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": ["", "1 SOME ROAD", "", "SOME PLACE", "SOME CITY"],
        "PDS_DEATH_STATUS": "1",
        "PDS_DATE_OF_DEATH": 20100101,
        "PDS_FAMILY_NAME": "surname",
        "PDS_GIVEN_NAMES": ["a"],
        "PDS_VISA_STATUS": None,
        "PDS_INVALID": False,
        "PDS_SENSITIVE": True,
        "PDS_CONFIDENTIALITY": "Y",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_missing_postcode_record():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20010101,"scn":1}],"confidentiality":[{"from":20010101,"scn":1,"val":"Y"}],"date":"20010101000000","death_status":"1","dob":19000101,"dod":20100101,"gender_history":[{"from":19000101,"gender":"1","scn":1}],"gp_codes":[{"from":19000101,"to":19991231,"scn":1,"val":"A10000"}, {"from":20000101,"scn":1,"val":"B20000"}],"migrant_data":[{"visa_status":"02","brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"familyName":"surname","from":19000101,"givenName":[{"name":"a"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": None,
        "PDS_DATE_OF_BIRTH": 19000101,
        "PDS_GENDER": "1",
        "PDS_GPCODE": "B20000",
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": ["", "1 SOME ROAD", "", "SOME PLACE", "SOME CITY"],
        "PDS_DEATH_STATUS": "1",
        "PDS_DATE_OF_DEATH": 20100101,
        "PDS_FAMILY_NAME": "surname",
        "PDS_GIVEN_NAMES": ["a"],
        "PDS_VISA_STATUS": "02",
        "PDS_INVALID": False,
        "PDS_SENSITIVE": True,
        "PDS_CONFIDENTIALITY": "Y",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_missing_address_record():
    # Arrange
    record = """{"address_history":[{"from":20010101,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20010101,"scn":1,"val":"Y"}],"date":"20010101000000","death_status":"1","dob":19000101,"dod":20100101,"gender_history":[{"from":19000101,"gender":"1","scn":1}],"gp_codes":[{"from":19000101,"to":19991231,"scn":1,"val":"A10000"}, {"from":20000101,"scn":1,"val":"B20000"}],"migrant_data":[{"visa_status":"02","brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"familyName":"surname","from":19000101,"givenName":[{"name":"a"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "AA1 BB2",
        "PDS_DATE_OF_BIRTH": 19000101,
        "PDS_GENDER": "1",
        "PDS_GPCODE": "B20000",
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": None,
        "PDS_DEATH_STATUS": "1",
        "PDS_DATE_OF_DEATH": 20100101,
        "PDS_FAMILY_NAME": "surname",
        "PDS_GIVEN_NAMES": ["a"],
        "PDS_VISA_STATUS": "02",
        "PDS_INVALID": False,
        "PDS_SENSITIVE": True,
        "PDS_CONFIDENTIALITY": "Y",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_missing_gp_code_record():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20010101,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20010101,"scn":1,"val":"Y"}],"date":"20010101000000","death_status":"1","dob":19000101,"dod":20100101,"gender_history":[{"from":19000101,"gender":"1","scn":1}],"migrant_data":[{"visa_status":"02","brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"familyName":"surname","from":19000101,"givenName":[{"name":"a"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "AA1 BB2",
        "PDS_DATE_OF_BIRTH": 19000101,
        "PDS_GENDER": "1",
        "PDS_GPCODE": None,
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": ["", "1 SOME ROAD", "", "SOME PLACE", "SOME CITY"],
        "PDS_DEATH_STATUS": "1",
        "PDS_DATE_OF_DEATH": 20100101,
        "PDS_FAMILY_NAME": "surname",
        "PDS_GIVEN_NAMES": ["a"],
        "PDS_VISA_STATUS": "02",
        "PDS_INVALID": False,
        "PDS_SENSITIVE": True,
        "PDS_CONFIDENTIALITY": "Y",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_missing_death_status_record():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20010101,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20010101,"scn":1,"val":"Y"}],"date":"20010101000000","dob":19000101,"dod":20100101,"gender_history":[{"from":19000101,"gender":"1","scn":1}],"gp_codes":[{"from":19000101,"to":19991231,"scn":1,"val":"A10000"}, {"from":20000101,"scn":1,"val":"B20000"}],"migrant_data":[{"visa_status":"02","brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"familyName":"surname","from":19000101,"givenName":[{"name":"a"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "AA1 BB2",
        "PDS_DATE_OF_BIRTH": 19000101,
        "PDS_GENDER": "1",
        "PDS_GPCODE": "B20000",
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": ["", "1 SOME ROAD", "", "SOME PLACE", "SOME CITY"],
        "PDS_DEATH_STATUS": None,
        "PDS_DATE_OF_DEATH": 20100101,
        "PDS_FAMILY_NAME": "surname",
        "PDS_GIVEN_NAMES": ["a"],
        "PDS_VISA_STATUS": "02",
        "PDS_INVALID": False,
        "PDS_SENSITIVE": True,
        "PDS_CONFIDENTIALITY": "Y",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_missing_date_of_death_record():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20010101,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20010101,"scn":1,"val":"Y"}],"date":"20010101000000","death_status":"1","dob":19000101,"gender_history":[{"from":19000101,"gender":"1","scn":1}],"gp_codes":[{"from":19000101,"to":19991231,"scn":1,"val":"A10000"}, {"from":20000101,"scn":1,"val":"B20000"}],"migrant_data":[{"visa_status":"02","brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"familyName":"surname","from":19000101,"givenName":[{"name":"a"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "AA1 BB2",
        "PDS_DATE_OF_BIRTH": 19000101,
        "PDS_GENDER": "1",
        "PDS_GPCODE": "B20000",
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": ["", "1 SOME ROAD", "", "SOME PLACE", "SOME CITY"],
        "PDS_DEATH_STATUS": "1",
        "PDS_DATE_OF_DEATH": None,
        "PDS_FAMILY_NAME": "surname",
        "PDS_GIVEN_NAMES": ["a"],
        "PDS_VISA_STATUS": "02",
        "PDS_INVALID": False,
        "PDS_SENSITIVE": True,
        "PDS_CONFIDENTIALITY": "Y",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_missing_date_of_birth_record():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20010101,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20010101,"scn":1,"val":"Y"}],"date":"20010101000000","death_status":"1","dod":20100101,"gender_history":[{"from":19000101,"gender":"1","scn":1}],"gp_codes":[{"from":19000101,"to":19991231,"scn":1,"val":"A10000"}, {"from":20000101,"scn":1,"val":"B20000"}],"migrant_data":[{"visa_status":"02","brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"familyName":"surname","from":19000101,"givenName":[{"name":"a"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "AA1 BB2",
        "PDS_DATE_OF_BIRTH": None,
        "PDS_GENDER": "1",
        "PDS_GPCODE": "B20000",
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": ["", "1 SOME ROAD", "", "SOME PLACE", "SOME CITY"],
        "PDS_DEATH_STATUS": "1",
        "PDS_DATE_OF_DEATH": 20100101,
        "PDS_FAMILY_NAME": "surname",
        "PDS_GIVEN_NAMES": ["a"],
        "PDS_VISA_STATUS": "02",
        "PDS_INVALID": False,
        "PDS_SENSITIVE": True,
        "PDS_CONFIDENTIALITY": "Y",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_missing_gender_record():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20010101,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20010101,"scn":1,"val":"Y"}],"date":"20010101000000","death_status":"1","dob":19000101,"dod":20100101,"gender_history":[{"from":19000101,"scn":1}],"gp_codes":[{"from":19000101,"to":19991231,"scn":1,"val":"A10000"}, {"from":20000101,"scn":1,"val":"B20000"}],"migrant_data":[{"visa_status":"02","brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"familyName":"surname","from":19000101,"givenName":[{"name":"a"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "AA1 BB2",
        "PDS_DATE_OF_BIRTH": 19000101,
        "PDS_GENDER": None,
        "PDS_GPCODE": "B20000",
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": ["", "1 SOME ROAD", "", "SOME PLACE", "SOME CITY"],
        "PDS_DEATH_STATUS": "1",
        "PDS_DATE_OF_DEATH": 20100101,
        "PDS_FAMILY_NAME": "surname",
        "PDS_GIVEN_NAMES": ["a"],
        "PDS_VISA_STATUS": "02",
        "PDS_INVALID": False,
        "PDS_SENSITIVE": True,
        "PDS_CONFIDENTIALITY": "Y",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_missing_family_name_record():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20010101,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20010101,"scn":1,"val":"Y"}],"date":"20010101000000","death_status":"1","dob":19000101,"dod":20100101,"gender_history":[{"from":19000101,"gender":"1","scn":1}],"gp_codes":[{"from":19000101,"to":19991231,"scn":1,"val":"A10000"}, {"from":20000101,"scn":1,"val":"B20000"}],"migrant_data":[{"visa_status":"02","brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"from":19000101,"givenName":[{"name":"a"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "AA1 BB2",
        "PDS_DATE_OF_BIRTH": 19000101,
        "PDS_GENDER": "1",
        "PDS_GPCODE": "B20000",
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": ["", "1 SOME ROAD", "", "SOME PLACE", "SOME CITY"],
        "PDS_DEATH_STATUS": "1",
        "PDS_DATE_OF_DEATH": 20100101,
        "PDS_FAMILY_NAME": None,
        "PDS_GIVEN_NAMES": ["a"],
        "PDS_VISA_STATUS": "02",
        "PDS_INVALID": False,
        "PDS_SENSITIVE": True,
        "PDS_CONFIDENTIALITY": "Y",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_missing_given_names_record():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20010101,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20010101,"scn":1,"val":"Y"}],"date":"20010101000000","death_status":"1","dob":19000101,"dod":20100101,"gender_history":[{"from":19000101,"gender":"1","scn":1}],"gp_codes":[{"from":19000101,"to":19991231,"scn":1,"val":"A10000"}, {"from":20000101,"scn":1,"val":"B20000"}],"migrant_data":[{"visa_status":"02","brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"familyName":"surname","from":19000101,"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "AA1 BB2",
        "PDS_DATE_OF_BIRTH": 19000101,
        "PDS_GENDER": "1",
        "PDS_GPCODE": "B20000",
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": ["", "1 SOME ROAD", "", "SOME PLACE", "SOME CITY"],
        "PDS_DEATH_STATUS": "1",
        "PDS_DATE_OF_DEATH": 20100101,
        "PDS_FAMILY_NAME": "surname",
        "PDS_GIVEN_NAMES": None,
        "PDS_VISA_STATUS": "02",
        "PDS_INVALID": False,
        "PDS_SENSITIVE": True,
        "PDS_CONFIDENTIALITY": "Y",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_missing_NHS_number_record():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20010101,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20010101,"scn":1,"val":"Y"}],"date":"20010101000000","death_status":"1","dob":19000101,"dod":20100101,"gender_history":[{"from":19000101,"gender":"1","scn":1}],"gp_codes":[{"from":19000101,"to":19991231,"scn":1,"val":"A10000"}, {"from":20000101,"scn":1,"val":"B20000"}],"migrant_data":[{"visa_status":"02","brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"familyName":"surname","from":19000101,"givenName":[{"name":"a"}],"scn":1}],"scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "AA1 BB2",
        "PDS_DATE_OF_BIRTH": 19000101,
        "PDS_GENDER": "1",
        "PDS_GPCODE": "B20000",
        "PDS_NHS_NUMBER": None,
        "PDS_ADDRESS": ["", "1 SOME ROAD", "", "SOME PLACE", "SOME CITY"],
        "PDS_DEATH_STATUS": "1",
        "PDS_DATE_OF_DEATH": 20100101,
        "PDS_FAMILY_NAME": "surname",
        "PDS_GIVEN_NAMES": ["a"],
        "PDS_VISA_STATUS": "02",
        "PDS_INVALID": False,
        "PDS_SENSITIVE": True,
        "PDS_CONFIDENTIALITY": "Y",
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


def test_expand_pds_record_at_with_missing_Confidentiality_record():
    # Arrange
    record = """{"address_history":[{"addr":[{"line":""},{"line":"1 SOME ROAD"},{"line":""},{"line":"SOME PLACE"},{"line":"SOME CITY"}],"from":20010101,"postalCode":"AA1 BB2","scn":1}],"confidentiality":[{"from":20010101,"scn":1}],"date":"20010101000000","death_status":"1","dob":19000101,"dod":20100101,"gender_history":[{"from":19000101,"gender":"1","scn":1}],"gp_codes":[{"from":19000101,"to":19991231,"scn":1,"val":"A10000"}, {"from":20000101,"scn":1,"val":"B20000"}],"migrant_data":[{"visa_status":"02","brp_no":null,"home_office_ref_no":"A/MERGED","nationality":"AGO","visa_from":20010101,"visa_to":20160101,"from":20010101,"scn":1}],"name_history":[{"familyName":"surname","from":19000101,"givenName":[{"name":"a"}],"scn":1}],"nhs_number":"9475705309","scn":1,"sensitive":true}"""
    datetime_value = datetime(2005, 1, 1, 12, 0)

    expected_output = {
        "PDS_POSTCODE": "AA1 BB2",
        "PDS_DATE_OF_BIRTH": 19000101,
        "PDS_GENDER": "1",
        "PDS_GPCODE": "B20000",
        "PDS_NHS_NUMBER": "9475705309",
        "PDS_ADDRESS": ["", "1 SOME ROAD", "", "SOME PLACE", "SOME CITY"],
        "PDS_DEATH_STATUS": "1",
        "PDS_DATE_OF_DEATH": 20100101,
        "PDS_FAMILY_NAME": "surname",
        "PDS_GIVEN_NAMES": ["a"],
        "PDS_VISA_STATUS": "02",
        "PDS_INVALID": False,
        "PDS_SENSITIVE": False,
        "PDS_CONFIDENTIALITY": None,
    }
    # Act
    output = expand_pds_record_at(record, datetime_value)

    # Assert
    assert expected_output == output


@pytest.mark.parametrize(
    "postcode, gpcode, event_date, expected",
    [
        pytest.param(
            "L6 4DB", "P84064", datetime(2021, 1, 1), {
                "CCG_OF_RESIDENCE": "99A",
                "ICS_OF_RESIDENCE": "QYG",
                "LA_OF_RESIDENCE": "E08000012",
                "LSOA_OF_RESIDENCE": "E01006767",
                "CCG_OF_REGISTRATION": "14L",
                "ICS_OF_REGISTRATION": "QOP",
                "LA_OF_REGISTRATION": "E08000003",
                "LSOA_OF_REGISTRATION": "E01033663"
            }, id="valid_postcode_and_gp_code"
        ),
        pytest.param(
            "LS1 1AB", "A86041", datetime(2021, 1, 1), {
                "CCG_OF_RESIDENCE": "15F",
                "ICS_OF_RESIDENCE": "QWO",
                "LA_OF_RESIDENCE": "E08000035",
                "LSOA_OF_RESIDENCE": "E01033015",
                "CCG_OF_REGISTRATION": "99C",  # return ODS ccg code first
                "ICS_OF_REGISTRATION": "QHM",
                "LA_OF_REGISTRATION": "E08000021",
                "LSOA_OF_REGISTRATION": "E01008320"
            }, id="return_ccg_of_reg_from_ODS_table_not_ONS"
        ),
        pytest.param(
            "YO1 6LN", "A99912", datetime(2021, 1, 1), {
                "CCG_OF_RESIDENCE": "03Q",
                "ICS_OF_RESIDENCE": "QOQ",
                "LA_OF_RESIDENCE": "E06000014",
                "LSOA_OF_RESIDENCE": "E01033069",
                "CCG_OF_REGISTRATION": "03Q",  # event date before ODS record date, return assumed relationship from ONS
                "ICS_OF_REGISTRATION": "QOQ",  # event date before ODS record date, return assumed relationship from ONS
                "LA_OF_REGISTRATION": "E06000014",
                "LSOA_OF_REGISTRATION": "E01013348"
            }, id="ccg_of_reg_from_ODS_null_return_ccg_from_ONS"
        ),
        pytest.param(
            "YO1 6LN", "A99912", datetime(2022, 1, 1), {
                "CCG_OF_RESIDENCE": "03Q",
                "ICS_OF_RESIDENCE": "QOQ",
                "LA_OF_RESIDENCE": "E06000014",
                "LSOA_OF_RESIDENCE": "E01033069",
                "CCG_OF_REGISTRATION": "42D",  # event date after ODS record start date, ODS ccg code returned
                "ICS_OF_REGISTRATION": "QOQ",  # event date after ODS record start date, ODS ICB code returned
                "LA_OF_REGISTRATION": "E06000014",
                "LSOA_OF_REGISTRATION": "E01013348"
            }, id="return_ccg_of_reg_from_ODS_valid_event_date"
        ),
        pytest.param(
            "LS1 6AB", "C82076", datetime(2000, 3, 31), {
                col: None for col in RESIDENCE_AND_REGISTRATION_COLUMNS
            }, id="valid_postcode_and_gp_code_event_date_before_ONS_ODS_record"
        ),
        pytest.param(
            "InvalidPostcode9999999", "InvalidGPCode9999999999", datetime(2021, 1, 1), {
                col: None for col in RESIDENCE_AND_REGISTRATION_COLUMNS
            }, id="invalid_postcode_gp_code"
        ),
    ],
)
def test_geography_codes_of_residence_and_registration(postcode, gpcode, event_date, expected):
    output = geography_codes_of_residence_and_registration(postcode, gpcode, event_date)
    assert output == expected


@pytest.mark.parametrize(
     ("valuein, rc, upper, expected"),
     [
        (0, "*", 100000000, "0"), # Original test case. Original func found at https://github.com/NHSDigital/codonPython/blob/master/codonPython/tests/suppression_test.py
        (2, "*", 100000000, "*"), # Original test case
        (5, "*", 100000000, "*"), # Original test case
        (8, "*", 100000000, "10"), # Original test case
        (16, "*", 100000000, "15"), # Original test case
        (57, "*", 100000000, "55"), # Original test case
        (10023, "*", 100000000,"10025"), # Original test case
        (3 , "*", 100000000, "*"),  # Test 1: valueingreater than 0 and less than or equal to 7, expect suppression
        (24, "*", 100000000, "25"),  # Test 2: valuein greater than 7, expect rounding to nearest 5
        (0, "*", 100000000, "0"),  # Test 3: valuein is 0, expect 0
        (-5, "*", 100000000, ValueError),  # Test 4: vlauein is less than zero, expect value error
        (-3.6, "*", 10000000, ValueError),  #5 Test 5: valuein is less than zero and also a non integer float, expect value error
        (20, "*", 10, ValueError), # Test 6: vlauein is postive integer but larger than upper limit parameter, expect value error
        (-20, "*", -30, ValueError), # Test 7: valuein is negative integer and larger than upper,  expect value error
        ("!3", "*", 10000000, ValueError) , # Test 8: valuein is string, 10000000
        (None, "*", 100000000, ValueError), # Test 9: valuein is null, therefore non integer, expect value error
        (7, "*", 100000000, "*"), # Test 10: valein is 7 and needs to be suppressed as expected
        (6.999999, "*", 10000000, ValueError), # Test 11: valuein is positive float, expect value error
        (2, "0", 10000000, '0'), # Test 12: valuein is another string o
        (10, 0, 10000000, ValueError), # Test 13: replacement character is defined as an integer 0, not string type 0 which is a common rc, expect ValueError
    ]
)
def test_suppress_value(valuein, rc, upper, expected):
    if expected == ValueError:
        with pytest.raises(ValueError):
            suppress_value(valuein,rc, upper)
    else:
        assert expected == suppress_value(valuein, rc, upper)

