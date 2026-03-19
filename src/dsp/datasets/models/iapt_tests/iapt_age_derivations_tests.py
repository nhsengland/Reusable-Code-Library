from datetime import datetime

from dsp.datasets.models import iapt

from dsp.datasets.models.iapt_tests.iapt_helper_tests import referral


def test_at_given_date_child():
    record = referral()
    record['Patient']["PersonBirthDate"] = datetime(2000, 12, 21)
    record['Header']["ReportingPeriodStartDate"] = datetime(2002, 8, 1)
    record['Header']["ReportingPeriodEndDate"] = datetime(2003, 8, 30)
    record["ReferralRequestReceivedDate"] = datetime(2004, 8, 15)
    record["ServDischDate"] = datetime(2005, 7, 1)
    record['CareContacts'][0]["CareContDate"] = datetime(2006, 7, 2)
    record['CodedScoredAssessments'][0]["AssToolCompDate"] = datetime(2007, 7, 3)


    new_patient = iapt.Referral(record)
    assert new_patient.Patient.Age_RP_StartDate == 1
    assert new_patient.Patient.Age_RP_EndDate == 2
    assert new_patient.Age_ReferralRequest_ReceivedDate == 3
    assert new_patient.Age_ServiceDischarge_Date == 4
    assert new_patient.CareContacts[0].Age_CareContact_Date == 5
    assert new_patient.CodedScoredAssessments[0].Age_AssessmentCompletion_Date == 6
    assert new_patient.CareContacts[0].CareActivities[0].CodedScoredAssessments[0].Age_AssessmentCompletion_Date == 5


def test_at_given_date_adult():
    record = referral()
    record['Patient']["PersonBirthDate"] = datetime(1982, 11, 20)
    record['Header']["ReportingPeriodStartDate"] = datetime(2002, 8, 1)
    record['Header']["ReportingPeriodEndDate"] = datetime(2003, 8, 30)
    record["ReferralRequestReceivedDate"] = datetime(2004, 8, 15)
    record["ServDischDate"] = datetime(2005, 7, 1)
    record['CareContacts'][0]["CareContDate"] = datetime(2006, 7, 2)
    record['CodedScoredAssessments'][0]["AssToolCompDate"] = datetime(2007, 7, 3)

    new_patient = iapt.Referral(record)
    assert new_patient.Patient.Age_RP_StartDate == 19
    assert new_patient.Patient.Age_RP_EndDate == 20
    assert new_patient.Age_ReferralRequest_ReceivedDate == 21
    assert new_patient.Age_ServiceDischarge_Date == 22
    assert new_patient.CareContacts[0].Age_CareContact_Date == 23
    assert new_patient.CodedScoredAssessments[0].Age_AssessmentCompletion_Date == 24
    assert new_patient.CareContacts[0].CareActivities[0].CodedScoredAssessments[0].Age_AssessmentCompletion_Date == 23


def test_at_given_date_elderly():
    record = referral()
    record['Patient']["PersonBirthDate"] = datetime(1936, 10, 22)
    record['Header']["ReportingPeriodStartDate"] = datetime(2002, 8, 1)
    record['Header']["ReportingPeriodEndDate"] = datetime(2003, 8, 30)
    record["ReferralRequestReceivedDate"] = datetime(2004, 8, 15)
    record["ServDischDate"] = datetime(2005, 7, 1)
    record['CareContacts'][0]["CareContDate"] = datetime(2006, 7, 2)
    record['CodedScoredAssessments'][0]["AssToolCompDate"] = datetime(2007, 7, 3)

    new_patient = iapt.Referral(record)
    assert new_patient.Patient.Age_RP_StartDate == 65
    assert new_patient.Patient.Age_RP_EndDate == 66
    assert new_patient.Age_ReferralRequest_ReceivedDate == 67
    assert new_patient.Age_ServiceDischarge_Date == 68
    assert new_patient.CareContacts[0].Age_CareContact_Date == 69
    assert new_patient.CodedScoredAssessments[0].Age_AssessmentCompletion_Date == 70
    assert new_patient.CareContacts[0].CareActivities[0].CodedScoredAssessments[0].Age_AssessmentCompletion_Date == 69