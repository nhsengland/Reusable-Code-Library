import copy
import json
from math import floor
from typing import Dict, List, Tuple, Union
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

from dsp.datasets.common import PDSCohortFields
from dsp.datasets.sgss.stages.common.sgss_pds_cross_check import sgss_cross_check
from dsp.shared.pds.pds_discovery import set_pds_source
from dsp.model.pds_record import PDSRecordPaths, PDSRecord
# noinspection PyUnresolvedReferences
from dsp.model.pds_record_tests import full_pds_record_data
from dsp.dam import mps_request
from dsp.structured_model.structured_model import META

from dsp.shared.constants import DS

pds_schema = StructType([
    StructField(PDSCohortFields.SERIAL_CHANGE_NUMBER, StringType(), True),
    StructField(PDSCohortFields.NHS_NUMBER, StringType(), True),
    StructField(PDSCohortFields.DATE_OF_BIRTH, IntegerType(), True),
    StructField(PDSCohortFields.RECORD, StringType(), True),
    StructField(PDSCohortFields.NAME, StructType([
        StructField('givenNames', ArrayType(StringType(), True), True),
        StructField('familyName', StringType(), True)
    ]), True),
    StructField(PDSCohortFields.GENDER, StructType([
        StructField('gender', StringType(), True)
    ]), True),
    StructField(PDSCohortFields.ADDRESS, StructType([
        StructField('postCode', StringType(), True)
    ]), True),
    StructField(PDSCohortFields.MOBILE_PHONE, StringType(), True),
    StructField(PDSCohortFields.EMAIL_ADDRESS, StringType(), True),
    StructField(PDSCohortFields.DATE_OF_DEATH, StringType(), True),
    StructField(PDSCohortFields.REPLACED_BY, StringType(), True),
    StructField(PDSCohortFields.YEAR_OF_BIRTH, IntegerType(), True),
    StructField(PDSCohortFields.BIRTH_DECADE, IntegerType(), True)
])

SGSS_SCHEMA_FIELDS = [
    'Lab_Report_Date',
    'Patient_NHS_Number',
    'Patient_Surname',
    'Patient_Forename',
    'Patient_PostCode',
    'Patient_Sex',
    'Patient_Date_Of_Birth',
    'P2_email',
    'P2_mobile',
]

SGSS_SCHEMA = StructType([
    StructField('META', META.get_struct()),
    *[StructField(field, StringType()) for field in SGSS_SCHEMA_FIELDS]
])

sgss_mps_request = mps_request(DS.SGSS_DELTA)


@pytest.fixture(scope='function')
def sgss_record() -> Dict:
    data = {
        'META': {
            'EVENT_ID': '1:1'
        },
        'Patient_NHS_Number': None,
        'Lab_Report_Date': '2015-05-05T00:00:00',
        'Patient_Date_Of_Birth': '2008-10-15T00:00:00'
    }

    return copy.deepcopy(data)


def create_submission_data(spark: SparkSession, data: Union[Dict, List]):
    if isinstance(data, dict):
        data = [data]
    return spark.createDataFrame(data, SGSS_SCHEMA)


def create_pds_data(records: List[dict]) -> List[Tuple]:
    def _pop_temp_fields(d: dict) -> dict:
        d.pop('parsed_name', None)
        d.pop('parsed_address', None)
        return d

    return [
        (
            str(i),
            r[PDSRecordPaths.NHS_NUMBER],
            r[PDSRecordPaths.DOB],
            PDSRecord.from_json(json.dumps(_pop_temp_fields(copy.deepcopy(r)))).to_json(),
            r.get('parsed_name'),
            r[PDSRecordPaths.GENDER_HISTORY][0],
            r.get('parsed_address'),
            r[PDSRecordPaths.MOBILE_PHONE],
            r[PDSRecordPaths.EMAIL_ADDRESS],
            r[PDSRecordPaths.DOD],
            r[PDSRecordPaths.REPLACED_BY],
            floor(r[PDSRecordPaths.DOB] / 10000),
            floor(r[PDSRecordPaths.DOB] / 100000) * 10
        ) for i, r in enumerate(records)
    ]


def test_cross_check_match_record(spark: SparkSession, sgss_record: Dict, full_pds_record_data: Dict):
    record = copy.deepcopy(full_pds_record_data)
    record[PDSRecordPaths.NHS_NUMBER] = '9701716655'
    record[PDSRecordPaths.DOB] = 20081015
    record[PDSRecordPaths.EMAIL_ADDRESS] = 'person1@example.com'
    record['parsed_name'] = {
        'givenNames': [record[PDSRecordPaths.NAME_HISTORY][0]['givenName'][0]['name']],
        'familyName': record[PDSRecordPaths.NAME_HISTORY][0]['familyName']
    }
    record['parsed_address'] = {
        'postCode': record[PDSRecordPaths.ADDRESS_HISTORY][0]['postalCode'],
    }

    sgss_record['Patient_NHS_Number'] = '9701716655'

    set_pds_source(spark.createDataFrame(create_pds_data([record]), pds_schema))

    df_mps_request = create_submission_data(spark, sgss_record)

    matched, unmatched = sgss_cross_check(spark, sgss_mps_request(df_mps_request))

    matched_record = matched.collect()
    assert len(matched_record) == 1  # Matched record
    assert len(unmatched.collect()) == 0  # UnMatched record

    assert matched_record[0].UNIQUE_REFERENCE == '1-1'
    assert matched_record[0].REQ_NHS_NUMBER == '9701716655'
    assert matched_record[0].DATE_OF_BIRTH == 20081015  # Matched DOB
    assert matched_record[0].FAMILY_NAME == 'SINIAKOVAK'  # Familyname from PDS data
    assert matched_record[0].PdsCrossCheckCondition == 'default'  # crosscheck condition


def test_cross_check_nomatch_record(spark: SparkSession, sgss_record: Dict, full_pds_record_data: Dict):
    record = copy.deepcopy(full_pds_record_data)
    record[PDSRecordPaths.NHS_NUMBER] = '9920851116'

    set_pds_source(spark.createDataFrame(create_pds_data([record]), pds_schema))

    sgss_record['Patient_NHS_Number'] = '9920851116'
    sgss_record['Patient_Date_Of_Birth'] = '2012-10-15T00:00:00'  # Non-matching DOB

    df_mps_request = create_submission_data(spark, sgss_record)

    matched, unmatched = sgss_cross_check(spark, sgss_mps_request(df_mps_request))

    assert len(matched.collect()) == 0  # Matched record
    unmatched_record = unmatched.collect()
    assert len(unmatched_record) == 1  # UnMatched record

    assert unmatched_record[0].UNIQUE_REFERENCE == '1-1'  # Unique reference from submitted data
    assert unmatched_record[0].NHS_NO == '9920851116'  # NHS number from submitted data
    assert unmatched_record[0].DATE_OF_BIRTH == 20121015  # Unmatched DOB


def test_cross_check_match_fail_on_multiple_pds_matches(spark: SparkSession, sgss_record: Dict,
                                                        full_pds_record_data: Dict):
    record1 = copy.deepcopy(full_pds_record_data)
    record1[PDSRecordPaths.NHS_NUMBER] = '9154918782'
    record1[PDSRecordPaths.DOB] = 20031004
    record1[PDSRecordPaths.EMAIL_ADDRESS] = 'person1@example.com'
    record1[PDSRecordPaths.MOBILE_PHONE] = '+1234567890'
    record1['parsed_name'] = {
        'givenNames': [record1[PDSRecordPaths.NAME_HISTORY][0]['givenName'][0]['name']],
        'familyName': record1[PDSRecordPaths.NAME_HISTORY][0]['familyName']
    }
    record1[PDSRecordPaths.DOD] = None
    record1[PDSRecordPaths.REPLACED_BY] = None

    record2 = copy.deepcopy(full_pds_record_data)
    record2[PDSRecordPaths.NHS_NUMBER] = '9154918783'
    record2[PDSRecordPaths.DOB] = 20031004
    record2[PDSRecordPaths.EMAIL_ADDRESS] = 'person1@example.com'
    record2[PDSRecordPaths.MOBILE_PHONE] = '+1234567890'
    record2['parsed_name'] = {
        'givenNames': [record2[PDSRecordPaths.NAME_HISTORY][0]['givenName'][0]['name']],
        'familyName': record2[PDSRecordPaths.NAME_HISTORY][0]['familyName']
    }
    record2[PDSRecordPaths.DOD] = None
    record2[PDSRecordPaths.REPLACED_BY] = None

    set_pds_source(spark.createDataFrame(create_pds_data([record1, record2]), pds_schema))

    sgss_record['Patient_NHS_Number'] = None  # Non-matching NHS Number
    sgss_record['Patient_Forename'] = 'LUDMILA'
    sgss_record['Patient_Surname'] = 'SINIAKOVA'
    sgss_record['Patient_Sex'] = 'Female'
    sgss_record['P2_mobile'] = '+1234567890'
    sgss_record['Patient_Date_Of_Birth'] = '2003-10-04T00:00:00'

    df_mps_request = create_submission_data(spark, sgss_record)

    matched, unmatched = sgss_cross_check(spark, sgss_mps_request(df_mps_request))

    unmatched_record = unmatched.collect()
    assert len(matched.collect()) == 0  # Matched record
    assert len(unmatched_record) == 1  # Should be sent to mps if duplicate match

    assert unmatched_record[0].UNIQUE_REFERENCE == '1-1'  # Unique reference from submitted data
    assert unmatched_record[0].GIVEN_NAME == 'LUDMILA'
    assert unmatched_record[0].FAMILY_NAME == 'SINIAKOVA'
    assert unmatched_record[0].DATE_OF_BIRTH == 20031004
    assert unmatched_record[0].GENDER == '2'


def test_cross_check_ok_multiple_records_match_to_single_pds(spark: SparkSession, sgss_record: Dict,
                                                             full_pds_record_data: Dict):
    record = copy.deepcopy(full_pds_record_data)
    record[PDSRecordPaths.NHS_NUMBER] = '9154918782'
    record[PDSRecordPaths.DOB] = 20031004
    record[PDSRecordPaths.EMAIL_ADDRESS] = 'person1@example.com'
    record[PDSRecordPaths.MOBILE_PHONE] = '+1234567890'
    record['parsed_name'] = {
        'givenNames': [record[PDSRecordPaths.NAME_HISTORY][0]['givenName'][0]['name']],
        'familyName': record[PDSRecordPaths.NAME_HISTORY][0]['familyName']
    }
    record[PDSRecordPaths.DOD] = None
    record[PDSRecordPaths.REPLACED_BY] = None

    sgss_record1 = copy.deepcopy(sgss_record)
    sgss_record1['Patient_NHS_Number'] = None  # Non-matching NHS Number
    sgss_record1['Patient_Forename'] = 'LUDMILA'
    sgss_record1['Patient_Surname'] = 'SINIAKOVA'
    sgss_record1['Patient_Sex'] = 'Female'
    sgss_record1['P2_mobile'] = '+1234567890'
    sgss_record1['Patient_Date_Of_Birth'] = '2003-10-04T00:00:00'

    sgss_record2 = copy.deepcopy(sgss_record)
    sgss_record2['META'] = {'EVENT_ID': '1:2'}
    sgss_record2['Patient_NHS_Number'] = None  # Non-matching NHS Number
    sgss_record2['Patient_Forename'] = 'LUDMILA'
    sgss_record2['Patient_Surname'] = 'SINIAKOVA'
    sgss_record2['Patient_Sex'] = 'Female'
    sgss_record2['P2_email'] = 'person1@example.com'
    sgss_record2['Patient_Date_Of_Birth'] = '2003-10-04T00:00:00'

    set_pds_source(spark.createDataFrame(create_pds_data([record]), pds_schema))

    df_mps_request = create_submission_data(spark, [sgss_record1, sgss_record2])

    matched, unmatched = sgss_cross_check(spark, sgss_mps_request(df_mps_request))

    matched_record = sorted(matched.collect(), key=lambda x: x.UNIQUE_REFERENCE)
    unmatched_record = unmatched.collect()

    assert len(matched_record) == 2  # Matched record
    assert len(unmatched_record) == 0  # Unmatched record

    assert matched_record[0].UNIQUE_REFERENCE == '1-1'  # Unique reference from submitted data
    assert matched_record[0].MATCHED_NHS_NO == '9154918782'  # NHS number from pds
    assert matched_record[0].DATE_OF_BIRTH == 20031004  # matched DOB
    assert matched_record[0].PdsCrossCheckCondition == 'j'  # crosscheck condition

    assert matched_record[1].UNIQUE_REFERENCE == '1-2'  # Unique reference from submitted data
    assert matched_record[1].MATCHED_NHS_NO == '9154918782'  # NHS number from pds
    assert matched_record[1].DATE_OF_BIRTH == 20031004  # matched DOB
    assert matched_record[1].PdsCrossCheckCondition == 'j'  # crosscheck condition


def test_cross_check_match_scenario_j_match_on_email(spark: SparkSession, sgss_record: Dict,
                                                     full_pds_record_data: Dict):
    # pds cross check scenario j
    # match on dob, first name, last name, gender, and mobile or email
    record = copy.deepcopy(full_pds_record_data)
    record[PDSRecordPaths.NHS_NUMBER] = '9154918782'
    record[PDSRecordPaths.DOB] = 20031004
    record[PDSRecordPaths.EMAIL_ADDRESS] = 'person1@example.com'
    record[PDSRecordPaths.MOBILE_PHONE] = '+1234567890'
    record['parsed_name'] = {
        'givenNames': [record[PDSRecordPaths.NAME_HISTORY][0]['givenName'][0]['name']],
        'familyName': record[PDSRecordPaths.NAME_HISTORY][0]['familyName']
    }
    record[PDSRecordPaths.DOD] = None
    record[PDSRecordPaths.REPLACED_BY] = None

    set_pds_source(spark.createDataFrame(create_pds_data([record]), pds_schema))

    sgss_record['Patient_NHS_Number'] = None  # Non-matching NHS Number
    sgss_record['Patient_Forename'] = 'LU\'DMiLA'
    sgss_record['Patient_Surname'] = 'SINIAKOVA'
    sgss_record['Patient_Sex'] = 'Female'
    sgss_record['P2_email'] = 'person1@example.com'
    sgss_record['Patient_Date_Of_Birth'] = '2003-10-04T00:00:00'

    df_mps_request = create_submission_data(spark, sgss_record)

    matched, unmatched = sgss_cross_check(spark, sgss_mps_request(df_mps_request))

    matched_record = matched.collect()
    assert len(matched_record) == 1  # Matched record
    assert len(unmatched.collect()) == 0  # UnMatched record

    assert matched_record[0].UNIQUE_REFERENCE == '1-1'  # Unique reference from submitted data
    assert matched_record[0].MATCHED_NHS_NO == '9154918782'  # NHS number from pds
    assert matched_record[0].DATE_OF_BIRTH == 20031004  # matched DOB
    assert matched_record[0].PdsCrossCheckCondition == 'j'  # crosscheck condition


def test_cross_check_match_scenario_j_match_on_mobile(spark: SparkSession, sgss_record: Dict,
                                                      full_pds_record_data: Dict):
    # pds cross check scenario j
    # match on dob, first name, last name, gender, and mobile or email
    record = copy.deepcopy(full_pds_record_data)
    record[PDSRecordPaths.NHS_NUMBER] = '9154918782'
    record[PDSRecordPaths.DOB] = 20031004
    record[PDSRecordPaths.EMAIL_ADDRESS] = 'person1@example.com'
    record[PDSRecordPaths.MOBILE_PHONE] = '+1234567890'
    record['parsed_name'] = {
        'givenNames': [record[PDSRecordPaths.NAME_HISTORY][0]['givenName'][0]['name']],
        'familyName': record[PDSRecordPaths.NAME_HISTORY][0]['familyName']
    }
    record[PDSRecordPaths.DOD] = None
    record[PDSRecordPaths.REPLACED_BY] = None

    set_pds_source(spark.createDataFrame(create_pds_data([record]), pds_schema))

    sgss_record['Patient_NHS_Number'] = None  # Non-matching NHS Number
    sgss_record['Patient_Forename'] = 'LUDMILA'
    sgss_record['Patient_Surname'] = 'SINIAKOVA'
    sgss_record['Patient_Sex'] = 'Female'
    sgss_record['P2_mobile'] = '+1234567890'
    sgss_record['Patient_Date_Of_Birth'] = '2003-10-04T00:00:00'

    df_mps_request = create_submission_data(spark, sgss_record)

    matched, unmatched = sgss_cross_check(spark, sgss_mps_request(df_mps_request))

    matched_record = matched.collect()
    assert len(matched_record) == 1  # Matched record
    assert len(unmatched.collect()) == 0  # UnMatched record

    assert matched_record[0].UNIQUE_REFERENCE == '1-1'  # Unique reference from submitted data
    assert matched_record[0].MATCHED_NHS_NO == '9154918782'  # NHS number from pds
    assert matched_record[0].DATE_OF_BIRTH == 20031004  # matched DOB
    assert matched_record[0].PdsCrossCheckCondition == 'j'  # crosscheck condition


def test_cross_check_match_scenario_k_match_on_email(spark: SparkSession, sgss_record: Dict,
                                                     full_pds_record_data: Dict):
    # pds cross check scenario k
    # match on dob, first 3 letters of first name, last name, gender, and mobile or email
    record = copy.deepcopy(full_pds_record_data)
    record[PDSRecordPaths.NHS_NUMBER] = '9154918782'
    record[PDSRecordPaths.DOB] = 20031004
    record[PDSRecordPaths.EMAIL_ADDRESS] = 'person1@example.com'
    record[PDSRecordPaths.MOBILE_PHONE] = '+1234567890'
    record['parsed_name'] = {
        'givenNames': [record[PDSRecordPaths.NAME_HISTORY][0]['givenName'][0]['name']],
        'familyName': record[PDSRecordPaths.NAME_HISTORY][0]['familyName']
    }
    record[PDSRecordPaths.DOD] = None
    record[PDSRecordPaths.REPLACED_BY] = None

    set_pds_source(spark.createDataFrame(create_pds_data([record]), pds_schema))

    sgss_record['Patient_NHS_Number'] = None  # Non-matching NHS Number
    sgss_record['Patient_Forename'] = 'LUD'
    sgss_record['Patient_Surname'] = 'SIN iAKOVA'
    sgss_record['Patient_Sex'] = 'Female'
    sgss_record['P2_email'] = 'person1 +test@example.com'
    sgss_record['Patient_Date_Of_Birth'] = '2003-10-04T00:00:00'

    df_mps_request = create_submission_data(spark, sgss_record)

    matched, unmatched = sgss_cross_check(spark, sgss_mps_request(df_mps_request))

    matched_record = matched.collect()
    assert len(matched_record) == 1  # Matched record
    assert len(unmatched.collect()) == 0  # UnMatched record

    assert matched_record[0].UNIQUE_REFERENCE == '1-1'  # Unique reference from submitted data
    assert matched_record[0].MATCHED_NHS_NO == '9154918782'  # NHS number from pds
    assert matched_record[0].DATE_OF_BIRTH == 20031004  # matched DOB
    assert matched_record[0].PdsCrossCheckCondition == 'k'  # crosscheck condition


def test_cross_check_match_scenario_k_match_on_mobile(spark: SparkSession, sgss_record: Dict,
                                                      full_pds_record_data: Dict):
    # pds cross check scenario k
    # match on dob, first 3 letters of first name, last name, gender, and mobile or email
    record = copy.deepcopy(full_pds_record_data)
    record[PDSRecordPaths.NHS_NUMBER] = '9154918782'
    record[PDSRecordPaths.DOB] = 20031004
    record[PDSRecordPaths.EMAIL_ADDRESS] = 'person1@example.com'
    record[PDSRecordPaths.MOBILE_PHONE] = '+1234567890'
    record['parsed_name'] = {
        'givenNames': [record[PDSRecordPaths.NAME_HISTORY][0]['givenName'][0]['name']],
        'familyName': record[PDSRecordPaths.NAME_HISTORY][0]['familyName']
    }
    record[PDSRecordPaths.DOD] = None
    record[PDSRecordPaths.REPLACED_BY] = None

    set_pds_source(spark.createDataFrame(create_pds_data([record]), pds_schema))

    sgss_record['Patient_NHS_Number'] = None  # Non-matching NHS Number
    sgss_record['Patient_Forename'] = 'LUD'
    sgss_record['Patient_Surname'] = 'SINIAKOVA'
    sgss_record['Patient_Sex'] = 'Female'
    sgss_record['P2_mobile'] = '+12345 67890'
    sgss_record['Patient_Date_Of_Birth'] = '2003-10-04T00:00:00'

    df_mps_request = create_submission_data(spark, sgss_record)

    matched, unmatched = sgss_cross_check(spark, sgss_mps_request(df_mps_request))

    matched_record = matched.collect()
    assert len(matched_record) == 1  # Matched record
    assert len(unmatched.collect()) == 0  # UnMatched record

    assert matched_record[0].UNIQUE_REFERENCE == '1-1'  # Unique reference from submitted data
    assert matched_record[0].MATCHED_NHS_NO == '9154918782'  # NHS number from pds
    assert matched_record[0].DATE_OF_BIRTH == 20031004  # matched DOB
    assert matched_record[0].PdsCrossCheckCondition == 'k'  # crosscheck condition
