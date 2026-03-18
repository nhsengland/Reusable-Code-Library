import datetime
from typing import Dict

import pytest

from dsp.datasets.models.mhsds_v5 import Referral, MasterPatientIndex, ReferralToTreatment, HospitalProviderSpell

# this import is to make pytest fixture available
from dsp.datasets.models.mhsds_v5_tests.mhsds_v5_helper_tests import referral, master_patient_index, \
    referral_to_treatment, care_cluster, hospital_provider_spell

@pytest.mark.parametrize("rep_period_start_date, date_of_birth, expected_age", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2015, 5, 1), datetime.date(2015, 5, 1), 0),
    (datetime.date(2015, 5, 1), datetime.date(1997, 2, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 4, 30), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 2), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 16), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), 17),
    (datetime.date(2017, 2, 28), datetime.date(2016, 2, 29), 0),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 1),
])
def test_age_reporting_date(referral, rep_period_start_date, date_of_birth, expected_age):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['Patient']['PersonBirthDate'] = date_of_birth
    new_referral = Referral(referral)
    assert expected_age == new_referral.Patient.AgeRepPeriodStart


@pytest.mark.parametrize("accomodation_type_date, date_of_birth, expected_age", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2015, 5, 1), datetime.date(1997, 2, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(2015, 5, 1), 0),
    (datetime.date(2015, 5, 1), datetime.date(1997, 4, 30), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 2), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 16), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), 17),
    (datetime.date(2017, 2, 28), datetime.date(2016, 2, 29), 0),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 1),
])
def test_age_accom_status_date(master_patient_index, accomodation_type_date, date_of_birth, expected_age):
    master_patient_index['AccommodationStatuses'][0]['AccommodationTypeDate'] = accomodation_type_date

    master_patient_index['PersonBirthDate'] = date_of_birth
    new_master_patient = MasterPatientIndex(master_patient_index)
    assert expected_age == new_master_patient.AccommodationStatuses[0].AgeAccomTypeDate


@pytest.mark.parametrize("referral_rec_date, date_of_birth, expected_age", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2015, 5, 1), datetime.date(1997, 2, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(2015, 5, 1), 0),
    (datetime.date(2015, 5, 1), datetime.date(1997, 4, 30), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 2), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 16), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), 17),
    (datetime.date(2017, 2, 28), datetime.date(2016, 2, 29), 0),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 1),
])
def test_age_serv_refer_rec_date(referral, referral_rec_date, date_of_birth, expected_age):
    referral['ReferralRequestReceivedDate'] = referral_rec_date
    referral['Patient']['PersonBirthDate'] = date_of_birth
    new_referral = Referral(referral)
    assert expected_age == new_referral.AgeServReferRecDate


@pytest.mark.parametrize("service_discharge_date, date_of_birth, expected_age", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2015, 5, 1), datetime.date(1997, 2, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(2015, 5, 1), 0),
    (datetime.date(2015, 5, 1), datetime.date(1997, 4, 30), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 2), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 16), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), 17),
    (datetime.date(2017, 2, 28), datetime.date(2016, 2, 29), 0),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 1),
])
def test_age_serv_discharge_date(referral, service_discharge_date, date_of_birth, expected_age):
    referral['ServDischDate'] = service_discharge_date
    referral['Patient']['PersonBirthDate'] = date_of_birth
    new_referral = Referral(referral)
    assert expected_age == new_referral.AgeServReferDischDate


@pytest.mark.parametrize("serv_refer_closure_date, date_of_birth, expected_age", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2015, 5, 1), datetime.date(1997, 2, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 4, 30), 18),
    (datetime.date(2015, 5, 1), datetime.date(2015, 5, 1), 0),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 2), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 16), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), 17),
    (datetime.date(2017, 2, 28), datetime.date(2016, 2, 29), 0),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 1),
])
def test_age_serv_refer_closure(referral, serv_refer_closure_date, date_of_birth, expected_age):
    referral['ServiceTypesReferredTo'][0]['ReferClosureDate'] = serv_refer_closure_date
    referral['Patient']['PersonBirthDate'] = date_of_birth
    new_referral = Referral(referral)
    assert expected_age == new_referral.ServiceTypesReferredTo[0].AgeServReferClosure


@pytest.mark.parametrize("serv_refer_rejection_date, date_of_birth, expected_age", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2015, 5, 1), datetime.date(1997, 2, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 4, 30), 18),
    (datetime.date(2015, 5, 1), datetime.date(2015, 5, 1), 0),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 2), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 16), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), 17),
    (datetime.date(2017, 2, 28), datetime.date(2016, 2, 29), 0),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 1),
])
def test_age_serv_refer_rejection(referral, serv_refer_rejection_date, date_of_birth, expected_age):
    referral['ServiceTypesReferredTo'][0]['ReferRejectionDate'] = serv_refer_rejection_date
    referral['Patient']['PersonBirthDate'] = date_of_birth
    new_referral = Referral(referral)
    assert expected_age == new_referral.ServiceTypesReferredTo[0].AgeServReferRejection


@pytest.mark.parametrize("rtt_period_start_date, date_of_birth, expected_age", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2015, 5, 1), datetime.date(1997, 2, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(2015, 5, 1), 0),
    (datetime.date(2015, 5, 1), datetime.date(1997, 4, 30), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 2), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 16), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), 17),
    (datetime.date(2017, 2, 28), datetime.date(2016, 2, 29), 0),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 1),
])
def test_age_refer_treat_start(referral, rtt_period_start_date, date_of_birth, expected_age):
    referral['ReferralsToTreatment'][0]['ReferToTreatPeriodStartDate'] = rtt_period_start_date
    referral['Patient']['PersonBirthDate'] = date_of_birth
    new_referral = Referral(referral)
    assert expected_age == new_referral.ReferralsToTreatment[0].AgeReferTreatStartDate


@pytest.mark.parametrize("rtt_period_end_date, date_of_birth, expected_age", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2015, 5, 1), datetime.date(1997, 2, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(2015, 5, 1), 0),
    (datetime.date(2015, 5, 1), datetime.date(1997, 4, 30), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 2), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 16), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), 17),
    (datetime.date(2017, 2, 28), datetime.date(2016, 2, 29), 0),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 1),
])
def test_age_refer_treat_end(referral, rtt_period_end_date, date_of_birth, expected_age):
    referral['ReferralsToTreatment'][0]['ReferToTreatPeriodEndDate'] = rtt_period_end_date
    referral['Patient']['PersonBirthDate'] = date_of_birth
    new_referral = Referral(referral)
    assert expected_age == new_referral.ReferralsToTreatment[0].AgeReferTreatEndDate


@pytest.mark.parametrize("care_contact_date, date_of_birth, expected_age", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2015, 5, 1), datetime.date(1997, 2, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(2015, 5, 1), 0),
    (datetime.date(2015, 5, 1), datetime.date(1997, 4, 30), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 2), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 16), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), 17),
    (datetime.date(2017, 2, 28), datetime.date(2016, 2, 29), 0),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 1),
])
def test_age_care_contact(referral, care_contact_date, date_of_birth, expected_age):
    referral['CareContacts'][0]['CareContDate'] = care_contact_date
    referral['Patient']['PersonBirthDate'] = date_of_birth
    new_referral = Referral(referral)
    assert expected_age == new_referral.CareContacts[0].AgeCareContDate


@pytest.mark.parametrize("assess_tool_comp_date, date_of_birth, expected_age", [
    (None, None, None),
    (datetime.date(2015, 5, 1), None, None),
    (datetime.date(2015, 5, 1), datetime.date(1997, 2, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(2015, 5, 1), 0),
    (datetime.date(2015, 5, 1), datetime.date(1997, 4, 30), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 2), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 16), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), 17),
    (datetime.date(2017, 2, 28), datetime.date(2016, 2, 29), 0),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 1),
])
def test_age_assess_tool_comp(referral, assess_tool_comp_date, date_of_birth, expected_age):
    referral['CodedScoredAssessmentReferrals'][0]['AssToolCompTimestamp'] = assess_tool_comp_date
    referral['Patient']['PersonBirthDate'] = date_of_birth
    new_referral = Referral(referral)
    assert expected_age == new_referral.CodedScoredAssessmentReferrals[0].AgeAssessToolReferCompDate


@pytest.mark.parametrize("rep_period_end_date, date_of_birth, expected_age", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2015, 5, 1), datetime.date(1997, 2, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(2015, 5, 1), 0),
    (datetime.date(2015, 5, 1), datetime.date(1997, 4, 30), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 2), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 16), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), 17),
    (datetime.date(2017, 2, 28), datetime.date(2016, 2, 29), 0),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 1),
])
def test_age_reporting_end_date(referral, rep_period_end_date, date_of_birth, expected_age):
    referral['Header']['ReportingPeriodEndDate'] = rep_period_end_date
    referral['Patient']['PersonBirthDate'] = date_of_birth
    new_referral = Referral(referral)
    assert expected_age == new_referral.Patient.AgeRepPeriodEnd


@pytest.mark.parametrize("death_date, date_of_birth, expected_age", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2015, 5, 1), datetime.date(1997, 2, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(2015, 5, 1), 0),
    (datetime.date(2015, 5, 1), datetime.date(1997, 4, 30), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 2), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 5, 16), 17),
    (datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), 17),
    (datetime.date(2017, 2, 28), datetime.date(2016, 2, 29), 0),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 1),
])
def test_age_death_date(master_patient_index, death_date, date_of_birth, expected_age):
    master_patient_index['PersDeathDate'] = death_date
    master_patient_index['PersonBirthDate'] = date_of_birth
    new_patient = MasterPatientIndex(master_patient_index)
    assert expected_age == new_patient.AgeDeath


@pytest.mark.parametrize("code_obs, care_contact_date, date_of_birth, expected_age", [
    ('E0...', None, None, None),
    ('E0...', None, datetime.date(2018, 11, 8), None),
    ('E0...', datetime.date(2018, 11, 8), None, None),
    ('E0...', datetime.date(2015, 5, 1), datetime.date(1997, 2, 1), 18),
    ('E0...', datetime.date(2015, 5, 1), datetime.date(2015, 5, 1), 0),
    ('E0...', datetime.date(2015, 5, 1), datetime.date(1997, 4, 30), 18),
    ('E0...', datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    ('E0...', datetime.date(2015, 5, 1), datetime.date(1997, 5, 1), 18),
    ('E0...', datetime.date(2015, 5, 1), datetime.date(1997, 5, 2), 17),
    ('E0...', datetime.date(2015, 5, 1), datetime.date(1997, 5, 16), 17),
    ('E0...', datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), 17),
    ('E0...', datetime.date(2017, 2, 28), datetime.date(2016, 2, 29), 0),
    ('E0...', datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 1),
    (None, datetime.date(2015, 5, 1), datetime.date(1997, 8, 1), None),
])
def test_age_assess_tool_date(referral, code_obs, care_contact_date, date_of_birth, expected_age):
    referral['CareContacts'][0]['CareActivities'][0]['CodeObs'] = code_obs
    referral['CareContacts'][0]['CareContDate'] = care_contact_date
    referral['Patient']['PersonBirthDate'] = date_of_birth
    new_referral = Referral(referral)
    assert expected_age == new_referral.CareContacts[0].CareActivities[0].CodedScoredAssessmentCareActivities[0].AgeAssessToolCont


@pytest.mark.parametrize("end_date, start_date, days_between", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2018, 12, 31), datetime.date(2018, 1, 1), 364),
    (datetime.date(2016, 2, 29), datetime.date(2016, 2, 1), 28),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 366),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 366),
])
def test_days_between(
        referral_to_treatment: Dict, end_date: datetime.date, start_date: datetime.date, days_between: str):

    referral_to_treatment['ReferToTreatPeriodEndDate'] = end_date
    referral_to_treatment['ReferToTreatPeriodStartDate'] = start_date
    new_mhs104rtt = ReferralToTreatment(referral_to_treatment)
    assert days_between == new_mhs104rtt.TimeReferStartAndEndDate


@pytest.mark.parametrize("date_to_check, start_date, end_date, expected_flag", [
    (None, None, None, False),
    (None, datetime.date(2018, 11, 8), None, False),
    (datetime.date(2018, 11, 8), None, None, False),
    (datetime.date(2018, 12, 10), datetime.date(2018, 12, 1), datetime.date(2018, 12, 10), True),
    (datetime.date(2016, 2, 29), datetime.date(2016, 2, 29), datetime.date(2016, 3, 1), True),
    (datetime.date(2017, 3, 1), datetime.date(2017, 3, 2), datetime.date(2017, 3, 10), False),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), datetime.date(2018, 1, 1), True),
])
def test_date_in_between(referral: Dict,
                         date_to_check: datetime.date,
                         start_date: datetime.date,
                         end_date: datetime.date,
                         expected_flag: str):

    referral['Header']['ReportingPeriodStartDate'] = start_date
    referral['Header']['ReportingPeriodEndDate'] = end_date
    referral['Patient']['ClusteringToolAssessments'][0]['CareClusters'][0]['StartDateCareClust'] = date_to_check

    new_referral = Referral(referral)  # type: Referral

    assert expected_flag == new_referral.Patient.ClusteringToolAssessments[0].CareClusters[0].ClusterStartRPFlag


@pytest.mark.parametrize("end_date, start_date, days_between", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2018, 12, 31), datetime.date(2018, 12, 31), 0),
    (datetime.date(2018, 12, 31), datetime.date(2018, 1, 1), 364),
    (datetime.date(2016, 2, 29), datetime.date(2016, 2, 1), 28),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 366),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 366),
])
def test_time_refer_care_contact(
        referral: Dict, end_date: datetime.date, start_date: datetime.date, days_between: int):

    referral['CareContacts'][0]['CareContDate'] = end_date
    referral['ReferralRequestReceivedDate'] = start_date
    new_referral = Referral(referral)
    assert days_between == new_referral.CareContacts[0].TimeReferAndCareContact


@pytest.mark.parametrize("end_date, start_date, days_between", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2018, 12, 31), datetime.date(2018, 12, 31), 0),
    (datetime.date(2018, 12, 31), datetime.date(2018, 1, 1), 364),
    (datetime.date(2016, 2, 29), datetime.date(2016, 2, 1), 28),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 366),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 366),
])
def test_los_discharge_hosp_spell(
        hospital_provider_spell: Dict, end_date: datetime.date, start_date: datetime.date, days_between: int):

    hospital_provider_spell['DischDateHospProvSpell'] = end_date
    hospital_provider_spell['StartDateHospProvSpell'] = start_date
    new_hospital_provider_spell = HospitalProviderSpell(hospital_provider_spell)
    assert days_between == new_hospital_provider_spell.LOSDischHosSpell


@pytest.mark.parametrize("end_date, start_date, days_between", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2018, 12, 31), datetime.date(2018, 12, 31), 0),
    (datetime.date(2018, 12, 31), datetime.date(2018, 1, 1), 364),
    (datetime.date(2016, 2, 29), datetime.date(2016, 2, 1), 28),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 366),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 366),
])
def test_los_hosp_spell_eorp(
        referral: Dict, end_date: datetime.date, start_date: datetime.date, days_between: int):

    referral['Header']['ReportingPeriodEndDate'] = end_date
    referral['HospitalProviderSpells'][0]['StartDateHospProvSpell'] = start_date
    new_referral = Referral(referral)
    assert days_between == new_referral.HospitalProviderSpells[0].LOSHosSpellEoRP


@pytest.mark.parametrize("end_date, start_date, days_between", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2018, 12, 31), datetime.date(2018, 12, 31), None),
    (datetime.date(2018, 12, 31), datetime.date(2018, 1, 1), 364),
    (datetime.date(2016, 2, 29), datetime.date(2016, 2, 1), 28),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 366),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 366),
])
def test_time_est_disch_date(
        hospital_provider_spell: Dict, end_date: datetime.date, start_date: datetime.date, days_between: int):

    hospital_provider_spell['EstimatedDischDateHospProvSpell'] = end_date
    hospital_provider_spell['StartDateHospProvSpell'] = start_date
    new_hospital_provider_spell = HospitalProviderSpell(hospital_provider_spell)
    assert days_between == new_hospital_provider_spell.TimeEstDischDate


@pytest.mark.parametrize("end_date, start_date, days_between", [
    (None, None, None),
    (None, datetime.date(2018, 11, 8), None),
    (datetime.date(2018, 11, 8), None, None),
    (datetime.date(2018, 12, 31), datetime.date(2018, 12, 31), None),
    (datetime.date(2018, 12, 31), datetime.date(2018, 1, 1), 364),
    (datetime.date(2016, 2, 29), datetime.date(2016, 2, 1), 28),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 366),
    (datetime.date(2017, 3, 1), datetime.date(2016, 2, 29), 366),
])
def test_time_plan_disch_date(
        hospital_provider_spell: Dict, end_date: datetime.date, start_date: datetime.date, days_between: int):

    hospital_provider_spell['PlannedDischDateHospProvSpell'] = end_date
    hospital_provider_spell['StartDateHospProvSpell'] = start_date
    new_hospital_provider_spell = HospitalProviderSpell(hospital_provider_spell)
    assert days_between == new_hospital_provider_spell.TimePlanDischDate


@pytest.mark.parametrize("rp_end_date, rp_start_date, home_leave_end_date, home_leave_start_date, expected_days", [
    (None, None, None, None, None),
    (datetime.date(2018, 11, 8), datetime.date(2018, 11, 8), datetime.date(2018, 11, 8),
     datetime.date(2018, 11, 8), 0),
    (datetime.date(2018, 12, 8), datetime.date(2018, 11, 8), datetime.date(2019, 1, 8),
     datetime.date(2018, 12, 8), 31),
    (datetime.date(2018, 12, 8), datetime.date(2018, 11, 8), datetime.date(2019, 1, 8),
     datetime.date(2018, 12, 9), 30),
    (datetime.date(2019, 1, 8), datetime.date(2018, 12, 9), datetime.date(2018, 12, 8),
     datetime.date(2018, 11, 8), -1),
    (datetime.date(2018, 11, 10), datetime.date(2018, 11, 8), datetime.date(2018, 11, 10),
     datetime.date(2018, 11, 9), 1),
    (datetime.date(2018, 11, 10), datetime.date(2018, 11, 8), datetime.date(2018, 11, 10),
     datetime.date(2018, 11, 9), 1),
    (datetime.date(2018, 11, 10), datetime.date(2018, 11, 8), None, datetime.date(2018, 11, 9), 2),
])
def test_home_leave_days_rp(
        referral: Dict, rp_end_date: datetime.date, rp_start_date: datetime.date,
        home_leave_end_date: datetime.date, home_leave_start_date: datetime.date, expected_days: int):

    referral['Header']['ReportingPeriodEndDate'] = rp_end_date
    referral['Header']['ReportingPeriodStartDate'] = rp_start_date
    referral['HospitalProviderSpells'][0]['WardStays'][0]['HomeLeaves'][0]['EndDateHomeLeave'] = home_leave_end_date
    referral['HospitalProviderSpells'][0]['WardStays'][0]['HomeLeaves'][0]['StartDateHomeLeave'] = home_leave_start_date
    new_referral = Referral(referral)
    assert expected_days == new_referral.HospitalProviderSpells[0].WardStays[0].HomeLeaves[0].HomeLeaveDaysEndRP


@pytest.mark.parametrize("rp_end_date, rp_start_date, leave_absence_end_date, leave_absence_start_date, expected_days", [
    (None, None, None, None, None),
    (datetime.date(2018, 11, 8), datetime.date(2018, 11, 8), datetime.date(2018, 11, 8),
     datetime.date(2018, 11, 8), 0),
    (datetime.date(2018, 12, 8), datetime.date(2018, 11, 8), datetime.date(2019, 1, 8),
     datetime.date(2018, 12, 8), 31),
    (datetime.date(2018, 12, 8), datetime.date(2018, 11, 8), datetime.date(2019, 1, 8),
     datetime.date(2018, 12, 9), 30),
    (datetime.date(2019, 1, 8), datetime.date(2018, 12, 9), datetime.date(2018, 12, 8),
     datetime.date(2018, 11, 8), -1),
    (datetime.date(2018, 11, 10), datetime.date(2018, 11, 8), datetime.date(2018, 11, 10),
     datetime.date(2018, 11, 9), 1),
    (datetime.date(2018, 11, 10), datetime.date(2018, 11, 8), datetime.date(2018, 11, 10),
     datetime.date(2018, 11, 9), 1),
    (datetime.date(2018, 11, 10), datetime.date(2018, 11, 8), None, datetime.date(2018, 11, 9), 2),
])
def test_loa_days_rp(
        referral: Dict, rp_end_date: datetime.date, rp_start_date: datetime.date,
        leave_absence_end_date: datetime.date, leave_absence_start_date: datetime.date, expected_days: int):

    referral['Header']['ReportingPeriodEndDate'] = rp_end_date
    referral['Header']['ReportingPeriodStartDate'] = rp_start_date
    referral['HospitalProviderSpells'][0]['WardStays'][0]['LeaveOfAbsences'][0]['EndDateMHLeaveAbs'] \
        = leave_absence_end_date
    referral['HospitalProviderSpells'][0]['WardStays'][0]['LeaveOfAbsences'][0]['StartDateMHLeaveAbs'] \
        = leave_absence_start_date
    new_referral = Referral(referral)
    assert expected_days == new_referral.HospitalProviderSpells[0].WardStays[0].LeaveOfAbsences[0].LOADaysRP


@pytest.mark.parametrize("rp_end_date, rp_start_date, abs_wo_leave_end_date, abs_wo_leave_start_date, expected_days", [
    (None, None, None, None, None),
    (datetime.date(2018, 11, 8), datetime.date(2018, 11, 8), datetime.date(2018, 11, 8),
     datetime.date(2018, 11, 8), 0),
    (datetime.date(2018, 12, 8), datetime.date(2018, 11, 8), datetime.date(2019, 1, 8),
     datetime.date(2018, 12, 8), 31),
    (datetime.date(2018, 12, 8), datetime.date(2018, 11, 8), datetime.date(2019, 1, 8),
     datetime.date(2018, 12, 9), 30),
    (datetime.date(2019, 1, 8), datetime.date(2018, 12, 9), datetime.date(2018, 12, 8),
     datetime.date(2018, 11, 8), -1),
    (datetime.date(2018, 11, 10), datetime.date(2018, 11, 8), datetime.date(2018, 11, 10),
     datetime.date(2018, 11, 9), 1),
    (datetime.date(2018, 11, 10), datetime.date(2018, 11, 8), datetime.date(2018, 11, 10),
     datetime.date(2018, 11, 9), 1),
    (datetime.date(2018, 11, 10), datetime.date(2018, 11, 8), None, datetime.date(2018, 11, 9), 2),
])
def test_awol_days_end_rp(
        referral: Dict, rp_end_date: datetime.date, rp_start_date: datetime.date,
        abs_wo_leave_end_date: datetime.date, abs_wo_leave_start_date: datetime.date, expected_days: int):

    referral['Header']['ReportingPeriodEndDate'] = rp_end_date
    referral['Header']['ReportingPeriodStartDate'] = rp_start_date
    referral['HospitalProviderSpells'][0]['WardStays'][0]['AbsenceWithoutLeaves'][0]['EndDateMHAbsWOLeave'] \
        = abs_wo_leave_end_date
    referral['HospitalProviderSpells'][0]['WardStays'][0]['AbsenceWithoutLeaves'][0]['StartDateMHAbsWOLeave'] \
        = abs_wo_leave_start_date
    new_referral = Referral(referral)
    assert expected_days == new_referral.HospitalProviderSpells[0].WardStays[0].AbsenceWithoutLeaves[0].AWOLDaysEndRP
