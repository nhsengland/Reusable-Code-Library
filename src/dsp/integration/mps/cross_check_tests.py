import copy
import decimal
import json
from datetime import datetime, date
from typing import Dict, Tuple, List, MutableMapping, Any

import pytest
from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from dsp.datasets.common import PDSCohortFields
from dsp.datasets.enrichments.mhsds_v5 import mps_request
from dsp.datasets.fields.mps.request import Fields as MPSRequestFields
from dsp.datasets.models.mhsds_v5 import Referral
# noinspection PyUnresolvedReferences
from dsp.datasets.models.mhsds_v5_tests.mhsds_v5_helper_tests import referral  # pylint:disable=W0621,W0611
from dsp.integration.mps.cross_check import cross_check, do_cross_check_trace
from dsp.integration.mps.mps_schema import mps_schema
from dsp.integration.pds.pds_discovery import set_pds_source
from dsp.model.pds_record import PDSRecordPaths, PDSRecord
# noinspection PyUnresolvedReferences
from dsp.model.pds_record_tests import full_pds_record_data  # pylint:disable=W0621,W0611

schema = StructType([
    StructField(PDSCohortFields.SERIAL_CHANGE_NUMBER, StringType(), True),
    StructField(PDSCohortFields.NHS_NUMBER, StringType(), True),
    StructField(PDSCohortFields.RECORD, StringType(), True)
])


@pytest.fixture()
def pds_records(full_pds_record_data) -> List[Tuple[str, str, str]]:
    record_1 = copy.deepcopy(full_pds_record_data)
    record_1[PDSRecordPaths.NHS_NUMBER] = '9464886888'
    record_1[PDSRecordPaths.DOB] = 20081015
    record_1[PDSRecordPaths.EMAIL_ADDRESS] = 'person1@example.com'

    record_2 = copy.deepcopy(full_pds_record_data)
    record_2[PDSRecordPaths.NHS_NUMBER] = '9464886889'
    record_2[PDSRecordPaths.DOB] = 20081015
    record_2[PDSRecordPaths.EMAIL_ADDRESS] = 'person2@example.com'

    record_3 = copy.deepcopy(full_pds_record_data)
    record_3[PDSRecordPaths.NHS_NUMBER] = '9464886889'
    record_3[PDSRecordPaths.DOB] = 20031001
    record_3[PDSRecordPaths.EMAIL_ADDRESS] = 'person3@example.com'
    record_3[PDSRecordPaths.CONFIDENTIALITY][1]['val'] = 'S'

    record_4 = copy.deepcopy(full_pds_record_data)
    record_4[PDSRecordPaths.NHS_NUMBER] = '9464886890'
    record_4[PDSRecordPaths.DOB] = 20031001
    record_4[PDSRecordPaths.EMAIL_ADDRESS] = 'person3@example.com'
    record_4[PDSRecordPaths.REPLACED_BY] = '9464886891'

    return [
        ('6', record_1[PDSRecordPaths.NHS_NUMBER], PDSRecord.from_json(json.dumps(record_1)).to_json()),
        ('7', record_2[PDSRecordPaths.NHS_NUMBER], PDSRecord.from_json(json.dumps(record_2)).to_json()),
        ('8', record_3[PDSRecordPaths.NHS_NUMBER], PDSRecord.from_json(json.dumps(record_3)).to_json()),
    ]


def create_submission_data(spark: SparkSession, referral: Dict, nhs_number: int, date_of_birth: datetime,
                           date_of_death: datetime):
    referral['Patient']['NHSNumber'] = nhs_number
    referral['Patient']['PersonBirthDate'] = date_of_birth
    referral['Patient']['PersDeathDate'] = date_of_death
    submission_data = Referral(referral)
    submission_data = spark.createDataFrame([submission_data.as_row()], Referral.get_struct())
    return submission_data


def test_cross_check_match_record(spark: SparkSession, referral: Dict,
                                  pds_records: List[Tuple[str, str, str]]):
    set_pds_source(spark.createDataFrame(pds_records, schema))

    submission_data = create_submission_data(
        spark,
        referral,
        nhs_number=9464886888,
        date_of_birth=datetime(2008, 10, 15),
        date_of_death=datetime(2018, 5, 4)
    )
    matched, unmatched = cross_check(spark, mps_request(submission_data))

    matched_record = matched.collect()
    assert len(matched_record) == 1  # Matched record
    assert len(unmatched.collect()) == 0  # UnMatched record
    assert matched_record[0].DATE_OF_BIRTH == 20081015  # Matched DOB


def test_cross_check_nomatch_record(spark: SparkSession, referral: Referral, pds_records: List[Tuple[str, str, str]]):
    set_pds_source(spark.createDataFrame(pds_records, schema))

    submission_data = create_submission_data(spark, referral, 9464886888, datetime(2012, 10, 15), datetime(2015, 5, 7))
    matched, unmatched = cross_check(spark, mps_request(submission_data))

    assert len(matched.collect()) == 0  # Matched record
    unmatched_record = unmatched.collect()
    assert len(unmatched_record) == 1  # UnMatched record

    assert unmatched_record[0].DATE_OF_DEATH == 20150507  # DOD from submitted data
    assert unmatched_record[0].DATE_OF_BIRTH == 20121015  # Unmatched DOB


@pytest.mark.parametrize('as_at, dob, actual, expected, description', [
    (20130613, 20081015, '', '', "Empty check"),
    (20130613, 20081015, 'a', 'a', "Single char check"),
    (20100101, 20081015, 'a', None, "No address matched for as at"),
    (20130613, 20081014, 'a', None, "No match - incorrect DOB"),
])
def test_address_fields(spark: SparkSession, full_pds_record_data: MutableMapping,
                        as_at: datetime,
                        dob: int,
                        actual: str,
                        expected: str,
                        description: str):
    full_pds_record_data[PDSRecordPaths.NHS_NUMBER] = '9464886888'
    full_pds_record_data[PDSRecordPaths.DOB] = dob
    full_pds_record_data[PDSRecordPaths.EMAIL_ADDRESS] = 'person1@example.com'
    full_pds_record_data[PDSRecordPaths.ADDRESS_HISTORY][0][PDSRecordPaths.ADDR] = [
        {'line': actual}, {'line': actual}, {'line': actual}, {'line': actual}, {'line': actual}
    ]
    full_pds_record_data[PDSRecordPaths.ADDRESS_HISTORY][1][PDSRecordPaths.ADDR] = [
        {'line': actual}, {'line': actual}, {'line': actual}, {'line': actual}, {'line': actual}
    ]

    test_row = Row(UNIQUE_REFERENCE='1234:5', NHS_NO=9464886888,
                   DATE_OF_BIRTH=20081015, POSTCODE='B10  0AU',
                   GENDER=9, LOCAL_PATIENT_ID='LPI00000000000000003',
                   DATE_OF_DEATH=20150507, AS_AT_DATE=as_at,
                   GP_PRACTICE_CODE='A81004', SERIAL_CHANGE_NUMBER='6', NHS_NUMBER='9464886888',
                   record=json.dumps(full_pds_record_data))

    result = do_cross_check_trace(test_row, test_row.__fields__)

    assert result[mps_schema.names.index('ADDRESS_LINE1')] == expected
    assert result[mps_schema.names.index('ADDRESS_LINE2')] == expected
    assert result[mps_schema.names.index('ADDRESS_LINE3')] == expected
    assert result[mps_schema.names.index('ADDRESS_LINE4')] == expected
    assert result[mps_schema.names.index('ADDRESS_LINE5')] == expected


@pytest.mark.parametrize('as_at, dob, actual, expected, description', [
    ('20130613', 20081015, 'smith', 'smith', "Regular check"),
    ('20130613', 20081015, '', '', "Empty check"),
    ('20130613', 20081015, None, None, "None check"),
])
def test_name_fields(spark: SparkSession, full_pds_record_data: MutableMapping,
                     as_at: datetime,
                     dob: int,
                     actual: str,
                     expected: str,
                     description: str):
    full_pds_record_data[PDSRecordPaths.NHS_NUMBER] = '9464886888'
    full_pds_record_data[PDSRecordPaths.DOB] = dob
    full_pds_record_data[PDSRecordPaths.EMAIL_ADDRESS] = 'person1@example.com'
    full_pds_record_data[PDSRecordPaths.NAME_HISTORY][0][PDSRecordPaths.FAMILY_NAME] = actual
    full_pds_record_data[PDSRecordPaths.NAME_HISTORY][1][PDSRecordPaths.FAMILY_NAME] = actual

    test_row = Row(UNIQUE_REFERENCE='1234:5', NHS_NO=9464886888,
                   DATE_OF_BIRTH=20081015, POSTCODE='B10  0AU',
                   GENDER=9, LOCAL_PATIENT_ID='LPI00000000000000003',
                   DATE_OF_DEATH=20150507, AS_AT_DATE=as_at,
                   GP_PRACTICE_CODE='A81004', SERIAL_CHANGE_NUMBER='6', NHS_NUMBER='9464886888',
                   record=json.dumps(full_pds_record_data))

    result = do_cross_check_trace(test_row, test_row.__fields__)

    assert result[mps_schema.names.index('FAMILY_NAME')] == expected
    assert result[mps_schema.names.index('DATE_OF_BIRTH')] == 20081015


def test_do_cross_check_trace(pds_records: List[Tuple[str, str, str]]):
    test_row = Row(UNIQUE_REFERENCE='1234:5', NHS_NO='9464886888',
                   DATE_OF_BIRTH=20081015, POSTCODE='B10  0AU',
                   GENDER=9, LOCAL_PATIENT_ID='LPI00000000000000003',
                   DATE_OF_DEATH=20150507, AS_AT_DATE=20130613,
                   GP_PRACTICE_CODE='A81004', SERIAL_CHANGE_NUMBER='6', NHS_NUMBER='9464886888',
                   record=pds_records[0][2])

    result = do_cross_check_trace(test_row, test_row.__fields__)

    assert result[mps_schema.names.index('UNIQUE_REFERENCE')] == '1234:5'
    assert result[mps_schema.names.index('REQ_NHS_NUMBER')] == '9464886888'
    assert result[mps_schema.names.index('FAMILY_NAME')] == 'SINIAKOVA'
    assert result[mps_schema.names.index('GIVEN_NAME')] == 'LUDMILA'
    assert result[mps_schema.names.index('OTHER_GIVEN_NAME')] == ''
    assert result[mps_schema.names.index('GENDER')] == '1'
    assert result[mps_schema.names.index('DATE_OF_BIRTH')] == 20081015
    assert result[mps_schema.names.index('DATE_OF_DEATH')] == 20190130
    assert result[mps_schema.names.index('ADDRESS_LINE1')] == ''
    assert result[mps_schema.names.index('ADDRESS_LINE2')] == '236 DSP MPS ROAD'
    assert result[mps_schema.names.index('ADDRESS_LINE3')] == 'DSP'
    assert result[mps_schema.names.index('ADDRESS_LINE4')] == 'MPS'
    assert result[mps_schema.names.index('ADDRESS_LINE5')] == ''
    assert result[mps_schema.names.index('POSTCODE')] == 'M19 3HD'
    assert result[mps_schema.names.index('GP_PRACTICE_CODE')] == 'D34099'
    assert not result[mps_schema.names.index('NHAIS_POSTING_ID')]
    assert result[mps_schema.names.index('AS_AT_DATE')] == 20130613
    assert result[mps_schema.names.index('LOCAL_PATIENT_ID')] == 'LPI00000000000000003'
    assert result[mps_schema.names.index('INTERNAL_ID')] == '1234:5'
    assert result[mps_schema.names.index('TELEPHONE_NUMBER')] == '(01234)567890'
    assert result[mps_schema.names.index('MOBILE_NUMBER')] == '0703991991'
    assert result[mps_schema.names.index('EMAIL_ADDRESS')] == 'person1@example.com'
    assert not result[mps_schema.names.index('SENSITIVTY_FLAG')]
    assert result[mps_schema.names.index('MPS_ID')] == None
    assert result[mps_schema.names.index('ERROR_SUCCESS_CODE')] == '00'
    assert result[mps_schema.names.index('MATCHED_NHS_NO')] == '9464886888'
    assert result[mps_schema.names.index('MATCHED_ALGORITHM_INDICATOR')] == 1
    assert result[mps_schema.names.index('MATCHED_CONFIDENCE_PERCENTAGE')] == decimal.Decimal('100')


def test_do_cross_check_trace_sensitive(pds_records: List[Tuple[str, str, str]]):
    test_row = Row(UNIQUE_REFERENCE='1234:5', NHS_NO='9464886888',
                   DATE_OF_BIRTH=20081015, POSTCODE='B10  0AU',
                   GENDER=9, LOCAL_PATIENT_ID='LPI00000000000000003',
                   DATE_OF_DEATH=20150507, AS_AT_DATE=20141113,
                   GP_PRACTICE_CODE='A81004', SERIAL_CHANGE_NUMBER='6', NHS_NUMBER='9464886888',
                   record=pds_records[0][2])

    result = do_cross_check_trace(test_row, test_row.__fields__)

    assert result[mps_schema.names.index('UNIQUE_REFERENCE')] == '1234:5'
    assert result[mps_schema.names.index('REQ_NHS_NUMBER')] == '9464886888'
    assert result[mps_schema.names.index('FAMILY_NAME')] == 'SINIAKOVA'
    assert result[mps_schema.names.index('GIVEN_NAME')] == 'LUDMILA'
    assert result[mps_schema.names.index('OTHER_GIVEN_NAME')] == ''
    assert result[mps_schema.names.index('GENDER')] == '1'
    assert result[mps_schema.names.index('DATE_OF_BIRTH')] == 20081015
    assert not result[mps_schema.names.index('DATE_OF_DEATH')]
    assert not result[mps_schema.names.index('ADDRESS_LINE1')]
    assert not result[mps_schema.names.index('ADDRESS_LINE2')]
    assert not result[mps_schema.names.index('ADDRESS_LINE3')]
    assert not result[mps_schema.names.index('ADDRESS_LINE4')]
    assert not result[mps_schema.names.index('ADDRESS_LINE5')]
    assert not result[mps_schema.names.index('POSTCODE')]
    assert not result[mps_schema.names.index('GP_PRACTICE_CODE')]
    assert not result[mps_schema.names.index('NHAIS_POSTING_ID')]
    assert result[mps_schema.names.index('AS_AT_DATE')] == 20141113
    assert result[mps_schema.names.index('LOCAL_PATIENT_ID')] == 'LPI00000000000000003'
    assert result[mps_schema.names.index('INTERNAL_ID')] == '1234:5'
    assert not result[mps_schema.names.index('TELEPHONE_NUMBER')]
    assert not result[mps_schema.names.index('MOBILE_NUMBER')]
    assert not result[mps_schema.names.index('EMAIL_ADDRESS')]
    assert result[mps_schema.names.index('SENSITIVTY_FLAG')] == 'S'
    assert result[mps_schema.names.index('MPS_ID')] == None
    assert result[mps_schema.names.index('ERROR_SUCCESS_CODE')] == '92'
    assert result[mps_schema.names.index('MATCHED_NHS_NO')] == '9464886888'
    assert result[mps_schema.names.index('MATCHED_ALGORITHM_INDICATOR')] == 1
    assert result[mps_schema.names.index('MATCHED_CONFIDENCE_PERCENTAGE')] == decimal.Decimal('100')


def test_do_cross_check_trace_sensitive_no_as_at(pds_records: List[Tuple[str, str, str]]):
    test_row = Row(UNIQUE_REFERENCE='1234:5', NHS_NO='9464886889',
                   DATE_OF_BIRTH=20031001, POSTCODE='B10  0AU',
                   GENDER=9, LOCAL_PATIENT_ID='LPI00000000000000003',
                   DATE_OF_DEATH=20150507, AS_AT_DATE=None,
                   GP_PRACTICE_CODE='A81004', SERIAL_CHANGE_NUMBER='6', NHS_NUMBER='9464886889',
                   record=pds_records[2][2])

    # Just in case we are running at bang on midnight and "today" changes - we check either the date as it was
    # before the test, or the date when the assertion is run
    date_pre_cross_check = PDSRecord.get_point_in_time(date.today())

    result = do_cross_check_trace(test_row, test_row.__fields__)

    assert result[mps_schema.names.index('UNIQUE_REFERENCE')] == '1234:5'
    assert result[mps_schema.names.index('REQ_NHS_NUMBER')] == '9464886889'
    assert result[mps_schema.names.index('FAMILY_NAME')] == 'SINIAKOVAK'
    assert result[mps_schema.names.index('GIVEN_NAME')] == 'LUDMILLIA'
    assert result[mps_schema.names.index('OTHER_GIVEN_NAME')] == 'ARIEL'
    assert result[mps_schema.names.index('GENDER')] == '1'
    assert result[mps_schema.names.index('DATE_OF_BIRTH')] == 20031001
    assert not result[mps_schema.names.index('DATE_OF_DEATH')]
    assert not result[mps_schema.names.index('ADDRESS_LINE1')]
    assert not result[mps_schema.names.index('ADDRESS_LINE2')]
    assert not result[mps_schema.names.index('ADDRESS_LINE3')]
    assert not result[mps_schema.names.index('ADDRESS_LINE4')]
    assert not result[mps_schema.names.index('ADDRESS_LINE5')]
    assert not result[mps_schema.names.index('POSTCODE')]
    assert not result[mps_schema.names.index('GP_PRACTICE_CODE')]
    assert not result[mps_schema.names.index('NHAIS_POSTING_ID')]
    assert (
            result[mps_schema.names.index('AS_AT_DATE')] == PDSRecord.get_point_in_time(date.today()) or
            result[mps_schema.names.index('AS_AT_DATE')] == date_pre_cross_check
    )
    assert result[mps_schema.names.index('LOCAL_PATIENT_ID')] == 'LPI00000000000000003'
    assert result[mps_schema.names.index('INTERNAL_ID')] == '1234:5'
    assert not result[mps_schema.names.index('TELEPHONE_NUMBER')]
    assert not result[mps_schema.names.index('MOBILE_NUMBER')]
    assert not result[mps_schema.names.index('EMAIL_ADDRESS')]
    assert result[mps_schema.names.index('SENSITIVTY_FLAG')] == 'S'
    assert result[mps_schema.names.index('MPS_ID')] == None
    assert result[mps_schema.names.index('ERROR_SUCCESS_CODE')] == '92'
    assert result[mps_schema.names.index('MATCHED_NHS_NO')] == '9464886889'
    assert result[mps_schema.names.index('MATCHED_ALGORITHM_INDICATOR')] == 1
    assert result[mps_schema.names.index('MATCHED_CONFIDENCE_PERCENTAGE')] == decimal.Decimal('100')


def _assert_mps_attributes(spark: SparkSession, referral: Dict, mps_field: str, expected: Any):
    _referral_1 = Referral(referral)
    df_referrals = spark.createDataFrame([
        _referral_1.as_row(),
    ], Referral.get_struct())

    result = mps_request(df_referrals).collect()

    assert len(result) == 1
    assert len(result[0]) == 9
    assert type(result[0][mps_field]) == type(expected)
    assert result[0][mps_field] == expected


@pytest.mark.parametrize("sample, expected", [
    (1234, '1234'),
])
def test_mhsds_mps_attributes_uniq_ref(spark: SparkSession, referral: Dict, sample: int, expected: str):
    referral['Patient']['RecordNumber'] = decimal.Decimal(sample)
    _assert_mps_attributes(spark, referral, MPSRequestFields.UNIQUE_REFERENCE, expected)


@pytest.mark.parametrize("sample, expected", [
    ('9876543210', '9876543210'),
])
def test_mhsds_mps_attributes_nhs_no(spark: SparkSession, referral: Dict, sample: str, expected: str):
    referral['Patient']['NHSNumber'] = sample
    _assert_mps_attributes(spark, referral, MPSRequestFields.NHS_NO, expected)


@pytest.mark.parametrize("sample, expected", [
    (9876543210, '9876543210'),
])
def test_mhsds_mps_attributes_nhs_no(spark: SparkSession, referral: Dict, sample: int, expected: str):
    referral['Patient']['NHSNumber'] = sample
    _assert_mps_attributes(spark, referral, MPSRequestFields.NHS_NO, expected)


@pytest.mark.parametrize("sample, expected", [
    (datetime(2007, 6, 3), 20070603),
])
def test_mhsds_mps_attributes_date_of_birth(spark: SparkSession, referral: Dict, sample: datetime, expected: str):
    referral['Patient']['PersonBirthDate'] = sample
    _assert_mps_attributes(spark, referral, MPSRequestFields.DATE_OF_BIRTH, expected)


@pytest.mark.parametrize("sample, expected", [
    ('LS1 4BT', 'LS1 4BT'),
])
def test_mhsds_mps_attributes_postcode(spark: SparkSession, referral: Dict, sample: str, expected: str):
    referral['Patient']['Postcode'] = sample
    _assert_mps_attributes(spark, referral, MPSRequestFields.POSTCODE, expected)


@pytest.mark.parametrize("sample, expected", [
    ('X', '0'),
    ('1', '1'),
    ('2', '2'),
    ('9', '9'),
])
def test_mhsds_mps_attributes_gender(spark: SparkSession, referral: Dict, sample: str, expected: str):
    referral['Patient']['Gender'] = sample
    _assert_mps_attributes(spark, referral, MPSRequestFields.GENDER, expected)


@pytest.mark.parametrize("sample, expected", [
    ('LPI00000000000038574', 'SAL1:LPI00000000000038574'),
])
def test_mhsds_mps_attributes_local_patient_id(spark: SparkSession, referral: Dict, sample: str, expected: str):
    referral['LocalPatientId'] = sample
    _assert_mps_attributes(spark, referral, MPSRequestFields.LOCAL_PATIENT_ID, expected)


@pytest.mark.parametrize("sample, expected", [
    (datetime(1975, 6, 25), 19750625),
])
def test_mhsds_mps_attributes_date_of_death(spark: SparkSession, referral: Dict, sample: datetime, expected: str):
    referral['Patient']['PersDeathDate'] = sample
    _assert_mps_attributes(spark, referral, MPSRequestFields.DATE_OF_DEATH, expected)


@pytest.mark.parametrize("sample, expected", [
    ('A64573', 'A64573'),
])
def test_mhsds_mps_attributes_gp_practice_code(spark: SparkSession, referral: Dict, sample: str, expected: str):
    referral['Patient']['GPs'][0]['GMPCodeReg'] = sample
    _assert_mps_attributes(spark, referral, MPSRequestFields.GP_PRACTICE_CODE, expected)


def test_cross_check_unmatch_records_with_replaced_by(spark: SparkSession, referral: Dict,
                                  pds_records: List[Tuple[str, str, str]]):
    set_pds_source(spark.createDataFrame(pds_records, schema))

    submission_data = create_submission_data(
        spark,
        referral,
        nhs_number=9464886890,
        date_of_birth=datetime(2008, 10, 15),
        date_of_death=datetime(2015, 5, 7)
    )
    matched, unmatched = cross_check(spark, mps_request(submission_data))

    assert matched.count() == 0  # Matched record
    assert unmatched.count() == 1  # UnMatched record
