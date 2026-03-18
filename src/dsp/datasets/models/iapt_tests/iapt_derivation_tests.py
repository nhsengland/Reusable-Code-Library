from datetime import date, datetime

import pytest

import dsp.datasets.models.iapt as iapt
# noinspection PyUnresolvedReferences
from dsp.datasets.models.iapt_tests.iapt_helper_tests import referral, care_personnel_qualification
from dsp.pipeline.loading import META_SCHEMA


@pytest.mark.parametrize("rp_start_date, expected_uniq_month_id", [
    (date(2020, 1, 1), 1438),
    (date(1950, 9, 11), 606),
    (date(2019, 4, 1), 1429)
])
def test_referral_uniq_month_id(rp_start_date, expected_uniq_month_id):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = rp_start_date
    new_referral = iapt.Referral(record)
    assert expected_uniq_month_id == new_referral.Unique_MonthID


@pytest.mark.parametrize("rp_start_date, expected_uniq_month_id", [
    (date(2020, 1, 1), 1438),
    (date(1950, 9, 11), 606),
    (date(2019, 4, 1), 1429)
])
def test_care_perc_qual__uniq_month_id(rp_start_date, expected_uniq_month_id):
    record = care_personnel_qualification()
    record['Header']['ReportingPeriodStartDate'] = rp_start_date
    new_care_perc_qual = iapt.CarePersonnelQualification(record)
    assert expected_uniq_month_id == new_care_perc_qual.Unique_MonthID


@pytest.mark.parametrize("event_id, row_num, expected", [
    ('1234:567', 11, '1234000000011'),
    ('1234567899', 111111, '1234567899000111111')
])
def test_uniq_table_id(event_id, row_num, expected):
    record = referral()
    record['META']['EVENT_ID'] = event_id
    record["Header"]["RowNumber"] = row_num
    record["Patient"]["RowNumber"] = row_num
    record["Patient"]["GPPracticeRegistrations"][0]["RowNumber"] = row_num
    record["Patient"]["EmploymentStatuses"][0]["RowNumber"] = row_num
    record["Patient"]["DisabilityTypes"][0]["RowNumber"] = row_num
    record["Patient"]["SocialAndPersonalCircumstances"][0]["RowNumber"] = row_num
    record["Patient"]["OverseasVisitorChargingCategories"][0]["RowNumber"] = row_num
    record["RowNumber"] = row_num
    record["OnwardReferrals"][0]["RowNumber"] = row_num
    record["WaitingTimePauses"][0]["RowNumber"] = row_num
    record["CareContacts"][0]["RowNumber"] = row_num
    record["CareContacts"][0]["CareActivities"][0]["RowNumber"] = row_num
    record["InternetTherapyLogs"][0]["RowNumber"] = row_num
    record["LongTermConditions"][0]["RowNumber"] = row_num
    record["PresentingComplaints"][0]["RowNumber"] = row_num
    record["CodedScoredAssessments"][0]["RowNumber"] = row_num
    record["CareContacts"][0]["CareActivities"][0]["CodedScoredAssessments"][0]["RowNumber"] = row_num
    record["Patient"]["CareClusters"][0]["RowNumber"] = row_num
    record1 = care_personnel_qualification()
    record1['META']['EVENT_ID'] = event_id
    record1["RowNumber"] = row_num
    new_referral = iapt.Referral(record)
    new_care_personnel_qualification = iapt.CarePersonnelQualification(record1)

    assert new_referral.Header.UniqueID_IDS000 == int(expected)
    assert new_referral.Patient.UniqueID_IDS001 == int(expected)
    assert new_referral.Patient.GPPracticeRegistrations[0].UniqueID_IDS002 == int(expected)
    assert new_referral.Patient.EmploymentStatuses[0].UniqueID_IDS004 == int(expected)
    assert new_referral.Patient.DisabilityTypes[0].UniqueID_IDS007 == int(expected)
    assert new_referral.Patient.SocialAndPersonalCircumstances[0].UniqueID_IDS011 == int(expected)
    assert new_referral.Patient.OverseasVisitorChargingCategories[0].UniqueID_IDS012 == int(expected)
    assert new_referral.UniqueID_IDS101 == int(expected)
    assert new_referral.OnwardReferrals[0].UniqueID_IDS105 == int(expected)
    assert new_referral.WaitingTimePauses[0].UniqueID_IDS108 == int(expected)
    assert new_referral.CareContacts[0].UniqueID_IDS201 == int(expected)
    assert new_referral.CareContacts[0].CareActivities[0].UniqueID_IDS202 == int(expected)
    assert new_referral.InternetTherapyLogs[0].UniqueID_IDS205 == int(expected)
    assert new_referral.LongTermConditions[0].UniqueID_IDS602 == int(expected)
    assert new_referral.PresentingComplaints[0].UniqueID_IDS603 == int(expected)
    assert new_referral.CodedScoredAssessments[0].UniqueID_IDS606 == int(expected)
    assert new_referral.CareContacts[0].CareActivities[0].CodedScoredAssessments[0].UniqueID_IDS607 == int(expected)
    assert new_referral.Patient.CareClusters[0].UniqueID_IDS803 == int(expected)
    assert new_care_personnel_qualification.UniqueID_IDS902 == int(expected)


@pytest.mark.parametrize("org_id_prov, service_req_id, expected", [
    ('RBB', 'PERF0SRI0002', 'RBBPERF0SRI0002'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)
])
def test_unique_service_request_id(org_id_prov, service_req_id, expected):
    record = referral()
    record['Header']['OrgIDProv'] = org_id_prov
    record['ServiceRequestId'] = service_req_id
    new_referral = iapt.Referral(record)
    assert expected == new_referral.Unique_ServiceRequestID


@pytest.mark.parametrize("org_id_prov, care_contact_id, expected", [
    ('RBB', 'PERF0CCI0001', 'RBBPERF0CCI0001'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)

])
def test_unique_care_contact_id(org_id_prov, care_contact_id, expected):
    record = referral()
    record['Header']['OrgIDProv'] = org_id_prov
    record['CareContacts'][0]['CareContactId'] = care_contact_id
    record['CareContacts'][0]['CareActivities'][0]['CareContactId'] = care_contact_id
    new_referral = iapt.Referral(record)
    uniq_care_contact_id_values = [
        new_referral.CareContacts[0].Unique_CareContactID,
        new_referral.CareContacts[0].CareActivities[0].Unique_CareContactID,
        new_referral.CareContacts[0].CareActivities[0].CodedScoredAssessments[0].Unique_CareContactID,
    ]
    for uniq_care_contact_id in uniq_care_contact_id_values:
        assert expected == uniq_care_contact_id


@pytest.mark.parametrize("org_id_prov, care_activity_id, expected", [
    ('RBB', 'PERF0CAI0001', 'RBBPERF0CAI0001'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)

])
def test_unique_care_activity_id(org_id_prov, care_activity_id, expected):
    record = referral()
    record['Header']['OrgIDProv'] = org_id_prov
    record['CareContacts'][0]['CareActivities'][0]['CareActId'] = care_activity_id
    record['CareContacts'][0]['CareActivities'][0]['CodedScoredAssessments'][0]['CareActId'] = care_activity_id
    new_referral = iapt.Referral(record)
    uniq_care_activity_id_values = [
        new_referral.CareContacts[0].CareActivities[0].Unique_CareActivityID,
        new_referral.CareContacts[0].CareActivities[0].CodedScoredAssessments[0].Unique_CareActivityID,
    ]
    for uniq_care_activity_id in uniq_care_activity_id_values:
        assert expected == uniq_care_activity_id


@pytest.mark.parametrize("org_id_prov, care_personnel_id_local, expected", [
    ('RBB', 'PERF0CAI0001', 'RBBPERF0CAI0001'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)

])
def test_unique_care_personnel_id_local(org_id_prov, care_personnel_id_local, expected):
    record = referral()
    record['Header']['OrgIDProv'] = org_id_prov
    record['CareContacts'][0]['CareActivities'][0]['CarePersLocalId'] = care_personnel_id_local
    record['InternetTherapyLogs'][0]['CarePersLocalId'] = care_personnel_id_local
    record1 = care_personnel_qualification()
    record1['Header']['OrgIDProv'] = org_id_prov
    record1['CarePersLocalId'] = care_personnel_id_local
    new_referral = iapt.Referral(record)
    new_care_personnel_qualification = iapt.CarePersonnelQualification(record1)

    uniq_care_personnel_id_local_values = [
        new_referral.CareContacts[0].CareActivities[0].Unique_CarePersonnelID_Local,
        new_referral.InternetTherapyLogs[0].Unique_CarePersonnelID_Local,
        new_care_personnel_qualification.Unique_CarePersonnelID_Local,
    ]
    for uniq_care_personnel_id_local in uniq_care_personnel_id_local_values:
        assert uniq_care_personnel_id_local == expected


def test_submission_id():
    record = referral()
    record['META']['EVENT_ID'] = '1234:567'
    new_referral = iapt.Referral(record)
    assert 1234 == new_referral.Header.UniqueSubmissionID


@pytest.mark.parametrize("rp_start_date, event_received_ts, expected", [
    (date(2020, 4, 1), datetime(2020, 5, 15, 12, 30, 10), 'Primary'),
    (date(2020, 4, 1), datetime(2020, 6, 15, 12, 30, 45), 'Refresh'),
])
def test_file_type(rp_start_date: date, event_received_ts: date, expected: str):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = rp_start_date
    record['META']['EVENT_RECEIVED_TS'] = event_received_ts
    new_referral = iapt.Referral(record)
    assert new_referral.Header.File_Type == expected


@pytest.mark.parametrize("uni_sub_id, row_num, expected", [
    (1, 1, 1000000001),
    (1234, 1, 1234000000001),
    (678, 54, 678000000054)
])
def test_record_number(uni_sub_id: int, row_num: int, expected: int):
    record = referral()
    record['Header']['UniqueSubmissionID'] = uni_sub_id
    record['Patient']['RowNumber'] = row_num
    new_referral = iapt.Referral(record)
    assert new_referral.Patient.RecordNumber == expected


@pytest.mark.parametrize("findschemeinuse, icd10code, expected_icd10code", [
    ('04', '83482000', '83482000'),  # Set output to '83482000' where findschemeinuse is '04' and icd10 is '8348200'.
    ('01', 'A0.1', '-3'),  # Removing single non-alphanumeric and matching.
    ('01', 'a01', '-3'),  # Converting to upper and matching.
    ('01', 'a0.1', '-3'),  # All cleaning together and matching.
    ('01', 'A33', 'A33X'),  # Add X and lookup success.
    ('01', 'A33X', 'A33X'),  # Keep X and lookup success.
    ('01', 'A00', 'A009'),  # Add 9 and lookup success. A00 is in ref data, but A009 picked ahead
    ('01', 'A009', 'A009'),  # Keep 9 and lookup success.
    ('01', 'A0.1A', '-3'),  # Removing single non-alphanumeric and matching - With asterisk.
    ('01', 'a01A', '-3'),  # Converting to upper and matching - With asterisk.
    ('01', 'a0.1a', '-3'),  # All cleaning together and matching - With asterisk.
    ('01', 'A33A', 'A33XA'),  # Add X and lookup success - With asterisk.
    ('01', 'A0.1D', '-3'),  # Removing single non-alphanumeric and matching - With dagger.
    ('01', 'a01D', '-3'),  # Converting to upper and matching - With dagger.
    ('01', 'a0.1d', '-3'),  # All cleaning together and matching - With dagger.
    ('01', 'A33D', 'A33XD'),  # Add X and lookup success - With dagger.
    ('01', 'A000', 'A000'),  # 4 characters.
    ('01', 'F10.0', 'F100'),  # 4 characters.
    ('01', 'I7000', 'I7000'),  # More than 4 characters.
    ('01', 'I7000d', 'I7000D'),  # More than 4 characters - with dagger
    ('01', 'ZZ9', '-3'),  # Invalid code no match, where findschemeinuse is '1'
    ('03', 'ZZ9', '-3'),  # Invalid code no match, where findschemeinuse is not '4'
    ('01', 'G041', 'G041'),  # Valid code - more than 3 chars - no match, where findschemeinuse is '1'
    ('01', 'F499', '-3'),  # Invalid code - more than 3 chars - no match, where findschemeinuse is '1'
    ('03', 'G041', '-3'),  # Invalid code - more than 3 chars - no match, where findschemeinuse is not '4'
    (None, 'A01', None),  # Where findschemeinuse is not populated, should return None
    ('01', None, None),  # Where submitted icd10code is not populated, should return None
    ('01', None, None),  # Where submitted icd10code is not populated, should return None
    ('01', '!!!!', '-3'),  # testing special characters
    ('01', '@@@@', '-3'),  # testing special characters
    ('01', '^^^^', '-3'),  # testing special characters
    ('01', 'adb,sd''', '-3'),  # testing special characters
    ('01', '~~~~', '-3'),  # testing special characters
    ('01', '++++', '-3'),  # testing special characters
    ('04', '====', '===='),  # testing speciacial characters
    ('01', ',,,,', '-3')  # testing special characters
])
def test_match_clean_icd10code_presentingcomplaints(findschemeinuse, icd10code, expected_icd10code):
    record = referral()
    record['PresentingComplaints'][0]['FindSchemeInUse'] = findschemeinuse
    record['PresentingComplaints'][0]['PresComp'] = icd10code
    new_referral = iapt.Referral(record)
    assert expected_icd10code == new_referral.PresentingComplaints[0].Validated_PresentingComplaint


@pytest.mark.parametrize("findschemeinuse, icd10code, expected_icd10code", [
    ('04', '83482000', '83482000'),  # Set output to '83482000' where findschemeinuse is '04' and icd10 is '8348200'.
    ('01', 'A0.1', '-3'),  # Removing single non-alphanumeric and matching.
    ('01', 'a01', '-3'),  # Converting to upper and matching.
    ('01', 'a0.1', '-3'),  # All cleaning together and matching.
    ('01', 'A33', 'A33X'),  # Add X and lookup success.
    ('01', 'A33X', 'A33X'),  # Keep X and lookup success.
    ('01', 'A00', 'A009'),  # Add 9 and lookup success. A00 is in ref data, but A009 picked ahead
    ('01', 'A009', 'A009'),  # Keep 9 and lookup success.
    ('01', 'A0.1A', '-3'),  # Removing single non-alphanumeric and matching - With asterisk.
    ('01', 'a01A', '-3'),  # Converting to upper and matching - With asterisk.
    ('01', 'a0.1a', '-3'),  # All cleaning together and matching - With asterisk.
    ('01', 'A33A', 'A33XA'),  # Add X and lookup success - With asterisk.
    ('01', 'A0.1D', '-3'),  # Removing single non-alphanumeric and matching - With dagger.
    ('01', 'a01D', '-3'),  # Converting to upper and matching - With dagger.
    ('01', 'a0.1d', '-3'),  # All cleaning together and matching - With dagger.
    ('01', 'A33D', 'A33XD'),  # Add X and lookup success - With dagger.
    ('01', 'A000', 'A000'),  # 4 characters.
    ('01', 'F10.0', 'F100'),  # 4 characters.
    ('01', 'I7000', 'I7000'),  # More than 4 characters.
    ('01', 'I7000d', 'I7000D'),  # More than 4 characters - with dagger
    ('01', 'ZZ9', '-3'),  # Invalid code no match, where findschemeinuse is '1'
    ('03', 'ZZ9', '-3'),  # Invalid code no match, where findschemeinuse is not '4'
    ('01', 'G041', 'G041'),  # Valid code - more than 3 chars - no match, where findschemeinuse is '1'
    ('01', 'F499', '-3'),  # Invalid code - more than 3 chars - no match, where findschemeinuse is '1'
    ('03', 'G041', '-3'),  # Invalid code - more than 3 chars - no match, where findschemeinuse is not '4'
    (None, 'A01', None),  # Where findschemeinuse is not populated, should return None
    ('01', None, None),  # Where submitted icd10code is not populated, should return None
    ('01', None, None),  # Where submitted icd10code is not populated, should return None
    ('01', '!!!!', '-3'),  # testing special characters
    ('01', '@@@@', '-3'),  # testing special characters
    ('01', '^^^^', '-3'),  # testing special characters
    ('01', 'adb,sd''', '-3'),  # testing special characters
    ('01', '~~~~', '-3'),  # testing special characters
    ('01', '++++', '-3'),  # testing special characters
    ('04', '====', '===='),  # testing speciacial characters
    ('01', ',,,,', '-3')  # testing special characters
])
def test_match_clean_icd10code_careactivity(findschemeinuse, icd10code, expected_icd10code):
    record = referral()
    record['CareContacts'][0]['CareActivities'][0]['FindSchemeInUse'] = findschemeinuse
    record['CareContacts'][0]['CareActivities'][0]['CodeFind'] = icd10code
    new_referral = iapt.Referral(record)
    assert expected_icd10code == new_referral.CareContacts[0].CareActivities[0].Validated_FindingCode


@pytest.mark.parametrize("findschemeinuse, icd10code, expected_icd10code", [
    ('04', '83482000', '83482000'),  # Set output to '83482000' where findschemeinuse is '04' and icd10 is '8348200'.
    ('01', 'A0.1', '-3'),  # Removing single non-alphanumeric and matching.
    ('01', 'a01', '-3'),  # Converting to upper and matching.
    ('01', 'a0.1', '-3'),  # All cleaning together and matching.
    ('01', 'A33', 'A33X'),  # Add X and lookup success.
    ('01', 'A33X', 'A33X'),  # Keep X and lookup success.
    ('01', 'A00', 'A009'),  # Add 9 and lookup success. A00 is in ref data, but A009 picked ahead
    ('01', 'A009', 'A009'),  # Keep 9 and lookup success.
    ('01', 'A0.1A', '-3'),  # Removing single non-alphanumeric and matching - With asterisk.
    ('01', 'a01A', '-3'),  # Converting to upper and matching - With asterisk.
    ('01', 'a0.1a', '-3'),  # All cleaning together and matching - With asterisk.
    ('01', 'A33A', 'A33XA'),  # Add X and lookup success - With asterisk.
    ('01', 'A0.1D', '-3'),  # Removing single non-alphanumeric and matching - With dagger.
    ('01', 'a01D', '-3'),  # Converting to upper and matching - With dagger.
    ('01', 'a0.1d', '-3'),  # All cleaning together and matching - With dagger.
    ('01', 'A33D', 'A33XD'),  # Add X and lookup success - With dagger.
    ('01', 'A000', 'A000'),  # 4 characters.
    ('01', 'F10.0', 'F100'),  # 4 characters.
    ('01', 'I7000', 'I7000'),  # More than 4 characters.
    ('01', 'I7000d', 'I7000D'),  # More than 4 characters - with dagger
    ('01', 'ZZ9', '-3'),  # Invalid code no match, where findschemeinuse is '1'
    ('03', 'ZZ9', '-3'),  # Invalid code no match, where findschemeinuse is not '4'
    ('01', 'G041', 'G041'),  # Valid code - more than 3 chars - no match, where findschemeinuse is '1'
    ('01', 'F499', '-3'),  # Invalid code - more than 3 chars - no match, where findschemeinuse is '1'
    ('03', 'G041', '-3'),  # Invalid code - more than 3 chars - no match, where findschemeinuse is not '4'
    (None, 'A01', None),  # Where findschemeinuse is not populated, should return None
    ('01', None, None),  # Where submitted icd10code is not populated, should return None
    ('01', None, None),  # Where submitted icd10code is not populated, should return None
    ('01', '!!!!', '-3'),  # testing special characters
    ('01', '@@@@', '-3'),  # testing special characters
    ('01', '^^^^', '-3'),  # testing special characters
    ('01', 'adb,sd''', '-3'),  # testing special characters
    ('01', '~~~~', '-3'),  # testing special characters
    ('01', '++++', '-3'),  # testing special characters
    ('04', '====', '===='),  # testing speciacial characters
    ('01', ',,,,', '-3')  # testing special characters
])
def test_match_clean_icd10code_longtermconditions(findschemeinuse, icd10code, expected_icd10code):
    record = referral()
    record['LongTermConditions'][0]['FindSchemeInUse'] = findschemeinuse
    record['LongTermConditions'][0]['LongTermCondition'] = icd10code
    new_referral = iapt.Referral(record)
    assert expected_icd10code == new_referral.LongTermConditions[0].Validated_LongTermConditionCode


@pytest.mark.parametrize("gp_practice_code, expected_org_id", [
    ('A81004', '00M'),
    ('A81017', '00K'),
    ('D82001', '06V'),
    ('G82092', '99J'),
    ('bad practice code', None),
    (None, None)
])
def test_org_id_of_ccg_from_gp_practice(gp_practice_code, expected_org_id):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = date(2020, 1, 1)
    record['Patient']['GPPracticeRegistrations'][0]['GMPCodeReg'] = gp_practice_code
    new_referral = iapt.Referral(record)  # creates a table
    assert expected_org_id == new_referral.Patient.GPPracticeRegistrations[0].OrgID_CCG_GP


@pytest.mark.parametrize("rep_period_start_date, patient_post_code, expected_postcode_flag", [
    (date(2015, 5, 1), None, 'N'),
    (date(2015, 5, 1), 'NQ999XX', 'N'),  # not present in gridall file
    (date(2015, 10, 3), 'EH4 9DX', 'N'),  # postcode valid from 20160201 to 20170201
    (date(2016, 10, 3), 'EH4 9DX', 'Y'),  # postcode valid from 20160201 to 20170201
    (date(2018, 10, 3), 'EH4 9DX', 'N'),  # postcode valid from 20160201 to 20170201
    (date(1990, 3, 1), 'CM40LZ', 'N'),  # postcode valid from 19980601 to 19991201
])
def test_valid_postcode_flag(rep_period_start_date, patient_post_code, expected_postcode_flag):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    record['Patient']['Postcode'] = patient_post_code
    new_referral = iapt.Referral(record)  # creates a table
    assert expected_postcode_flag == new_referral.Patient.ValidPostcode_Flag


@pytest.mark.parametrize("rep_period_start_date, patient_post_code, expected_ccg_res", [
    (date(2015, 5, 1), None, None),
    (date(2018, 5, 1), 'LS14JL', '15F'),  # "ccg": [20180501, "15F"]
    (date(2013, 6, 1), 'LS14JL', '03G'),  # "ccg": [20130401, "03G"]
    (date(2005, 6, 1), 'LS14JL', None),  # no ccg before 20060501
    (date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (date(2015, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(2016, 10, 3), 'EH4 9DX', '042'),  # postcode valid from 20160201 to 20170201
    (date(2018, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(1990, 3, 1), 'CM40LZ', None),  # postcode valid from 19980601 to 19991201
])
def test_ccg_res_from_patient_postcode(rep_period_start_date, patient_post_code, expected_ccg_res):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    record['Patient']['Postcode'] = patient_post_code
    new_referral = iapt.Referral(record)  # creates a table
    assert expected_ccg_res == new_referral.Patient.OrgID_CCG_Residence


@pytest.mark.parametrize("care_contact_date, referral_received_date, expected_time_in_days", [
    (date(2020, 1, 1), date(2020, 1, 1), 0),
])
def test_time_referral_to_care_contact(care_contact_date, referral_received_date, expected_time_in_days):
    record = referral()
    record['CareContacts'][0]['CareContDate'] = care_contact_date
    record['ReferralRequestReceivedDate'] = referral_received_date
    new_referral = iapt.Referral(record)
    assert new_referral.CareContacts[0].Time_Referral_to_CareContact == expected_time_in_days


@pytest.mark.parametrize("patient_post_code, expected_postcode_district", [
    ('LS14JL', 'LS1'),
    ('LS1 4JL', 'LS1'),
    ('LS157DJ', 'LS15'),
    ('LS15 7DJ', 'LS15'),
    ('bad postcode', None),
    (None, None)
])
def test_postcode_district_from_patient_postcode(patient_post_code, expected_postcode_district):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = date(2020, 1, 1)
    record['Patient']['Postcode'] = patient_post_code
    new_referral = iapt.Referral(record)  # creates a table
    assert expected_postcode_district == new_referral.Patient.Postcode_District


@pytest.mark.parametrize("patient_post_code, expected_default_postcode", [
    ('LS99 9ZZ', None),
    ('LS10 1LG', None),
    ('ZZ99 3VZ', 'ZZ99 3VZ'),
    (None, None)
])
def test_default_postcode_from_patient_postcode(patient_post_code, expected_default_postcode):
    record = referral()
    record['Patient']['Postcode'] = patient_post_code
    new_referral = iapt.Referral(record)  # creates a table
    assert expected_default_postcode == new_referral.Patient.Postcode_Default


@pytest.mark.parametrize("rep_period_start_date, patient_post_code, expected_lsoa", [
    (date(2015, 5, 1), None, None),
    (date(2015, 5, 1), 'LS14JL', 'E01033015'),  # "lsoa11": [20150501, "E01033015"]
    (date(2014, 5, 1), 'LS14JL', 'E01033008'),  # "lsoa11": [20140201, "E01033008"]
    (date(2013, 5, 1), 'LS14JL', None),  # no lsoa before 20140201
    (date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (date(2015, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201. lsoa valid from 20160501
    (date(2016, 10, 3), 'EH4 9DX', 'S01008864'),  # postcode valid from 20160201 to 20170201.
    (date(2018, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(1990, 3, 1), 'CM40LZ', None),  # postcode valid from 19980601 to 19991201
])
def test_lsoa_from_patient_postcode(rep_period_start_date, patient_post_code, expected_lsoa):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    record['Patient']['Postcode'] = patient_post_code
    new_referral = iapt.Referral(record)  # creates a table
    assert expected_lsoa == new_referral.Patient.LSOA


@pytest.mark.parametrize("lsoa, expected_census_year", [
    (None, None),
    ('E01033015', 2011),
])
def test_census_year(lsoa, expected_census_year):
    record = referral()
    record['Patient']['LSOA'] = lsoa
    new_referral = iapt.Referral(record)  # creates a table
    assert expected_census_year == new_referral.Patient.Census_Year


@pytest.mark.parametrize("imd_decile, imd_quartile, expected_imd_year", [
    (None, None, None),
    ('11', None, 2019),
    (None, '1', 2019),
    ('2', '1', 2019),
])
def test_imd_year(imd_decile, imd_quartile, expected_imd_year):
    record = referral()
    record['Patient']['IndicesOfDeprivationDecile'] = imd_decile
    record['Patient']['IndicesOfDeprivationQuartile'] = imd_quartile
    new_referral = iapt.Referral(record)  # creates a table
    assert expected_imd_year == new_referral.Patient.IMD_YEAR


@pytest.mark.parametrize("rep_period_start_date, patient_post_code, expected_la", [
    (date(2015, 5, 1), None, None),
    (date(2015, 5, 1), 'LS14JL', 'E08000035'),  # "oslaua": [20110201, "E08000035"]
    (date(2006, 6, 1), 'LS14JL', 'DA'),  # "oslaua": [20060501, "DA"]
    (date(2005, 6, 1), 'LS14JL', None),  # no county before 20060501
    (date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (date(2015, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(2016, 10, 3), 'EH4 9DX', 'S12000036'),  # postcode valid from 20160201 to 20170201
    (date(2018, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(1990, 3, 1), 'CM40LZ', None),  # postcode valid from 19980601 to 19991201
])
def test_la_from_patient_postcode(rep_period_start_date, patient_post_code, expected_la):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    record['Patient']['Postcode'] = patient_post_code
    new_referral = iapt.Referral(record)  # creates a table
    assert expected_la == new_referral.Patient.LocalAuthority


@pytest.mark.parametrize("rep_period_start_date, patient_post_code, expected_county", [
    (date(2015, 5, 1), None, None),
    (date(2015, 5, 1), 'LS14JL', 'E99999999'),  # "oscty": [20110201, "E99999999"]
    (date(2006, 6, 1), 'LS14JL', '00'),  # "oscty": [20060501, "00"]]
    (date(2005, 6, 1), 'LS14JL', None),  # no county before 20060501
    (date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (date(2015, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(2016, 10, 3), 'EH4 9DX', 'S99999999'),  # postcode valid from 20160201 to 20170201
    (date(2018, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(1990, 3, 1), 'CM40LZ', None),  # postcode valid from 19980601 to 19991201
])
def test_county_from_patient_postcode(rep_period_start_date, patient_post_code, expected_county):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    record['Patient']['Postcode'] = patient_post_code
    new_referral = iapt.Referral(record)  # creates a table
    assert expected_county == new_referral.Patient.County


@pytest.mark.parametrize("rep_period_start_date, patient_post_code, expected_ward", [
    (date(2015, 5, 1), None, None),
    (date(2015, 5, 1), 'LS14JL', 'E05001420'),  # "osward": [20110201, "E05001420"]
    (date(2006, 6, 1), 'LS14JL', 'GW'),  # "osward": [20060501, "GW"]
    (date(2005, 6, 1), 'LS14JL', None),  # no ward before 20060501
    (date(2015, 5, 1), 'NQ999XX', None),  # not present in gridall file
    (date(2015, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(2016, 10, 3), 'EH4 9DX', 'S13002587'),  # postcode valid from 20160201 to 20170201
    (date(2018, 10, 3), 'EH4 9DX', None),  # postcode valid from 20160201 to 20170201
    (date(1990, 3, 1), 'CM40LZ', None),  # postcode valid from 19980601 to 19991201
])
def test_ward_from_patient_postcode(rep_period_start_date, patient_post_code, expected_ward):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    record['Patient']['Postcode'] = patient_post_code
    new_referral = iapt.Referral(record)  # creates a table
    assert expected_ward == new_referral.Patient.ElectoralWard


@pytest.mark.parametrize("residence_postcode, gp_practice_code, expected_distance", [
    (None, None, None),
    ('LS999ZZ', 'A81001', None),  # invalid postcode
    ('AL100AU', 'A81001', 320),
    ('E10 5NP', 'A81002', 346)
])
def test_gp_distance_from_home(residence_postcode, gp_practice_code, expected_distance):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = date(2020, 1, 1)
    record['Patient']['Postcode'] = residence_postcode
    record['Patient']['GPPracticeRegistrations'][0]['GMPCodeReg'] = gp_practice_code
    new_referral = iapt.Referral(record)  # creates a table
    assert expected_distance == new_referral.Patient.GPPracticeRegistrations[0].DistanceFromHome_GP


@pytest.mark.parametrize("residence_postcode, site_id, expected_distance", [
    (None, None, None),
    ('LS999ZZ', 'A81003', None),  # invalid postcode
    ('AL100AU', 'A81003', 332),
    ('E10 5NP', 'A81004', 342)
])
def test_cont_loc_distance_from_home(residence_postcode, site_id, expected_distance):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = date(2020, 1, 1)
    record['Patient']['Postcode'] = residence_postcode
    record['CareContacts'][0]['SiteIDOfTreat'] = site_id
    new_referral = iapt.Referral(record)  # creates a table
    assert expected_distance == new_referral.CareContacts[0].DistanceFromHome_ContactLocation


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (None, None, None),
    (date(2015, 5, 1), None, None),
    (date(2018, 10, 3), 'SO168HA', None),  # date before  09/2019
    (date(2019, 10, 3), 'LS999ZZ', None),  # invalid postcode
    (date(2019, 1, 1), 'AB555GW', None),  # Active after 05/01/2019
    (date(2019, 10, 3), 'SO168GX', 1),
    (date(2019, 10, 3), 'SO168HA', 2),
    (date(2019, 10, 3), 'SO168DD', 3),
    (date(2019, 10, 3), 'N31NT', 4)
])
def test_indices_of_deprivation_quartile(rep_period_start_date, postcode, expected):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    record['Patient']['Postcode'] = postcode
    new_referral = iapt.Referral(record)
    assert expected == new_referral.Patient.IndicesOfDeprivationQuartile


@pytest.mark.parametrize("rep_period_start_date, postcode, expected", [
    (None, None, None),
    (date(2015, 5, 1), None, None),
    (date(2018, 10, 3), 'SO168HA', None),  # date before  09/2019
    (date(2019, 10, 3), 'LS999ZZ', None),  # invalid postcode
    (date(2019, 1, 1), 'AB555GW', None),  # Active after 05/01/2019
    (date(2019, 10, 3), 'SO168GX', 2),
    (date(2019, 10, 3), 'SO168HA', 4),
    (date(2019, 10, 3), 'SO168DD', 8),
    (date(2019, 10, 3), 'N31PU', 9),
    (date(2019, 10, 3), 'NW100AB', 1)
])
def test_indices_of_deprivation_decile(rep_period_start_date, postcode, expected):
    record = referral()
    record['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    record['Patient']['Postcode'] = postcode
    new_referral = iapt.Referral(record)
    assert expected == new_referral.Patient.IndicesOfDeprivationDecile


@pytest.mark.parametrize("OrgIDComm, expected_commissioning_region", [
    ('27T', 'Y62'),
    # org 27T has a role RO98 and a relationship (role RO209) so should return CommissioningRegion (org code) Y62
    ('32T', 'Y62'),
    # org 32T has a role RO98 and a relationship (role RO209) so should return CommissioningRegion (org code) Y62
    ('08F', 'Y56'),
    # org 08H has a role RO98 and a relationship (role RO261) so should return CommissioningRegion (org code) Y56
    ('08H', 'Y56'),
    # org 08H has a role RO98 and a relationship (role RO210) so should return CommissioningRegion (org code) Y56
    ('08Q', 'Y56'),
    # org 08H has a role RO98 and a relationship (role RO210) so should return CommissioningRegion (org code) Y56
    ('00WCC', None),
    # org 00WCC has a role RO98 but does not have relationship roles RO209 nor RO2010 so should return None
    ('LSP03', None),  # org LSP03 has a relationship role(RO209) but does not have a role RO98 so should return None

])
def test_commissioning_region(OrgIDComm, expected_commissioning_region):
    """ Jira Ticket #: DSP-11351"""

    record = referral()
    record['Header']['ReportingPeriodEndDate'] = date(2020, 12, 12)
    record['OrgIDComm'] = OrgIDComm
    new_referral = iapt.Referral(record)
    assert expected_commissioning_region == new_referral.CommissioningRegion


