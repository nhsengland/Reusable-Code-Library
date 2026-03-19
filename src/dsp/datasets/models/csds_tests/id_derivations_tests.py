import pytest

from dsp.datasets.models import csds
from dsp.datasets.models.csds_tests.csds_helper_tests import mpi, patient


def test_unique_service_request_id():
    service_request_id = 'SRI000001'
    org_id_provider = 'RJ6'

    expected = f"{org_id_provider}{service_request_id}"

    record = mpi()
    record['Referrals'][0]['ServiceRequestID'] = service_request_id
    record['Referrals'][0]['ServiceTypesReferredTo'][0]['ServiceRequestID'] = service_request_id
    record['Referrals'][0]['OtherReasonsForReferral'][0]['ServiceRequestID'] = service_request_id
    record['Referrals'][0]['ReferralsToTreatment'][0]['ServiceRequestID'] = service_request_id
    record['Referrals'][0]['OnwardReferrals'][0]['ServiceRequestID'] = service_request_id
    record['Referrals'][0]['CareContacts'][0]['ServiceRequestID'] = service_request_id
    record['Referrals'][0]['ProvisionalDiagnoses'][0]['ServiceRequestID'] = service_request_id
    record['Referrals'][0]['PrimaryDiagnoses'][0]['ServiceRequestID'] = service_request_id
    record['Referrals'][0]['SecondaryDiagnoses'][0]['ServiceRequestID'] = service_request_id
    record['Referrals'][0]['CodedScoredAssessmentReferrals'][0]['ServiceRequestID'] = service_request_id
    record['Header']['OrgID_Provider'] = org_id_provider
    new_patient = csds.MPI(record)

    assert new_patient.Referrals[0].Unique_ServiceRequestID == expected
    assert new_patient.Referrals[0].ServiceTypesReferredTo[0].Unique_ServiceRequestID == expected
    assert new_patient.Referrals[0].OtherReasonsForReferral[0].Unique_ServiceRequestID == expected
    assert new_patient.Referrals[0].ReferralsToTreatment[0].Unique_ServiceRequestID == expected
    assert new_patient.Referrals[0].OnwardReferrals[0].Unique_ServiceRequestID == expected
    assert new_patient.Referrals[0].CareContacts[0].Unique_ServiceRequestID == expected
    assert new_patient.Referrals[0].ProvisionalDiagnoses[0].Unique_ServiceRequestID == expected
    assert new_patient.Referrals[0].PrimaryDiagnoses[0].Unique_ServiceRequestID == expected
    assert new_patient.Referrals[0].SecondaryDiagnoses[0].Unique_ServiceRequestID == expected
    assert new_patient.Referrals[0].CodedScoredAssessmentReferrals[0].Unique_ServiceRequestID == expected


def test_unique_care_activity_id():
    care_activity_id = 'CA000001'
    org_id_provider = 'RJ6'

    expected = f"{org_id_provider}{care_activity_id}"

    record = mpi()
    record['Referrals'][0]['CareContacts'][0]['CareActivities'][0]["CareActivityID"] = care_activity_id
    record['Referrals'][0]['CareContacts'][0]['CareActivities'][0]["BreastfeedingStatuses"][0]["CareActivityID"] = care_activity_id
    record['Referrals'][0]['CareContacts'][0]['CareActivities'][0]["Observations"][0]["CareActivityID"] = care_activity_id
    record['Referrals'][0]['CareContacts'][0]['CareActivities'][0]["CodedScoredAssessmentContacts"][0]["CareActivityID"] = care_activity_id
    record['Header']['OrgID_Provider'] = org_id_provider

    new_patient = csds.MPI(record)

    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Unique_CareActivityID == expected
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].BreastfeedingStatuses[0].Unique_CareActivityID == expected
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Observations[0].Unique_CareActivityID == expected
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].CodedScoredAssessmentContacts[0].Unique_CareActivityID == expected


def test_unique_care_contact_id():
    care_contact_id = 'CC000001'
    org_id_provider = 'RJ6'

    expected = f"{org_id_provider}{care_contact_id}"

    record = mpi()
    record['Referrals'][0]['CareContacts'][0]["CareContactID"] = care_contact_id
    record['Referrals'][0]['CareContacts'][0]["CareActivities"][0]["CareContactID"] = care_contact_id
    record['Header']['OrgID_Provider'] = org_id_provider

    new_patient = csds.MPI(record)

    assert new_patient.Referrals[0].CareContacts[0].Unique_CareContactID == expected
    assert new_patient.Referrals[0].CareContacts[0].CareActivities[0].Unique_CareContactID == expected


def test_unique_care_plan_id():
    care_plan_id = 'CP000001'
    org_id_provider = 'RJ6'

    expected = f"{org_id_provider}{care_plan_id}"

    record = mpi()
    record['CarePlanTypes'][0]['PlanID'] = care_plan_id
    record['CarePlanTypes'][0]['CarePlanAgreements'][0]['PlanID'] = care_plan_id
    record['Header']['OrgID_Provider'] = org_id_provider

    new_patient = csds.MPI(record)

    assert new_patient.CarePlanTypes[0].Unique_PlanID == expected
    assert new_patient.CarePlanTypes[0].CarePlanAgreements[0].Unique_PlanID == expected


@pytest.mark.parametrize("team_id_local, expected_unique_team_id_local", [
    ('CP000001', 'RJ6CP000001'),
    (None, None)
])
def test_unique_care_professional_team_id(team_id_local, expected_unique_team_id_local):
    record = patient()
    record['Referrals'][0]['CareContacts'][0]['TeamID_Local'] = team_id_local
    record['Referrals'][0]['ServiceTypesReferredTo'][0]['TeamID_Local'] = team_id_local
    record['Header']['OrgID_Provider'] = "RJ6"
    new_patient = csds.MPI(record)  # creates a table
    assert expected_unique_team_id_local == new_patient.Referrals[0].CareContacts[0].Unique_TeamID_Local
    assert expected_unique_team_id_local == new_patient.Referrals[0].ServiceTypesReferredTo[0].Unique_TeamID_Local


@pytest.mark.parametrize("nhs_number, expected_valid_nhs_number_flag", [
    ('9686270272', 'Y'),
    (None, 'N')
])
def test_valid_nhs_number_flag(nhs_number, expected_valid_nhs_number_flag):
    record = patient()
    record['NHSNumber'] = nhs_number
    new_patient = csds.MPI(record)  # creates a table
    assert expected_valid_nhs_number_flag == new_patient.ValidNHSNumber_Flag


def test_record_number():
    uni_sub_id = 45
    row_number = 6

    expected = 456

    record = mpi()
    record['Header']['UniqueSubmissionID'] = uni_sub_id
    record['RowNumber'] = row_number

    new_patient = csds.MPI(record)

    assert new_patient.RecordNumber == expected
