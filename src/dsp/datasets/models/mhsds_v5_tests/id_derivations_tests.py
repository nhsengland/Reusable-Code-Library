import datetime

import pytest

from dsp.datasets.models.mhsds_v5 import Referral

# this import is to make pytest fixture available
from dsp.datasets.models.mhsds_v5_tests.mhsds_v5_helper_tests import referral


def test_submission_id(referral):
    referral['META']['EVENT_ID'] = '1234:567'
    new_referral = Referral(referral)
    assert 1234 == new_referral.Header.UniqSubmissionID


@pytest.mark.parametrize("file_uploaded_time, expected", [
    (datetime.date(1900, 4, 1), 1),
    (datetime.date(2015, 4, 1), 1381),
    (datetime.date(2015, 5, 1), 1382),
    (datetime.date(2015, 4, 1), 1381),
    (datetime.date(2015, 5, 1), 1382),
])
def test_uniq_month_id(referral, file_uploaded_time, expected):
    referral['Header']['ReportingPeriodStartDate'] = file_uploaded_time
    new_referral = Referral(referral)
    assert expected == new_referral.UniqMonthID


@pytest.mark.parametrize("org_id_provider, expected", [
    ('RAL', 'RAL'),
    ('', ''),
    (' ', ' '),
    (None, None)
])
def test_org_id_prov(referral, org_id_provider, expected):
    referral['Header']['OrgIDProvider'] = org_id_provider
    new_referral = Referral(referral)
    assert expected == new_referral.Header.OrgIDProvider


@pytest.mark.parametrize("org_id_provider, care_prof_id, expected", [
    ('RAL', 'CPL00000000000000002', 'RALCPL00000000000000002'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)
])
def test_uniq_care_prof_local_id(referral, org_id_provider, care_prof_id, expected):
    referral['Header']['OrgIDProvider'] = org_id_provider
    referral['Patient']['MentalHealthCareCoordinators'][0]['CareProfLocalId'] = care_prof_id
    referral['CareContacts'][0]['CareActivities'][0]['CareProfLocalId'] = care_prof_id
    referral['IndirectActivities'][0]['CareProfLocalId'] = care_prof_id
    referral['Patient']['MentalHealthActLegalStatusClassificationAssignmentPeriods'][0]['ResponsibleClinicianAssignmentPeriods'][0][
        'CareProfLocalId'] = care_prof_id
    referral['HospitalProviderSpells'][0]['AssignedCareProfessionals'][0]['CareProfLocalId'] = care_prof_id
    referral['CodedScoredAssessmentReferrals'][0]['CareProfLocalId'] = care_prof_id
    referral['Patient']['CPACareEpisodes'][0]['CPAReviews'][0]['CareProfLocalId'] = care_prof_id
    new_referral = Referral(referral)
    uniq_care_prof_local_id_values = [
        new_referral.Patient.MentalHealthCareCoordinators[0].UniqCareProfLocalID,
        new_referral.CareContacts[0].CareActivities[0].UniqCareProfLocalID,
        new_referral.IndirectActivities[0].UniqCareProfLocalID,
        new_referral.IndirectActivities[0].UniqCareProfLocalID,
        new_referral.Patient.MentalHealthActLegalStatusClassificationAssignmentPeriods[
            0].ResponsibleClinicianAssignmentPeriods[0].UniqCareProfLocalID,
        new_referral.HospitalProviderSpells[0].AssignedCareProfessionals[0].UniqCareProfLocalID,
        new_referral.CodedScoredAssessmentReferrals[0].UniqCareProfLocalID,
        new_referral.Patient.CPACareEpisodes[0].CPAReviews[0].UniqCareProfLocalID
    ]
    for uniq_prof_id in uniq_care_prof_local_id_values:
        assert expected == uniq_prof_id


@pytest.mark.parametrize("org_id_provider, care_plan_id, expected", [
    ('RAL', 'CPI00000000000000003', 'RALCPI00000000000000003'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)
])
def test_uniq_care_plan_id(referral, org_id_provider, care_plan_id, expected):
    referral['Header']['OrgIDProvider'] = org_id_provider
    referral['Patient']['CarePlanTypes'][0]['CarePlanAgreements'][0]['CarePlanID'] = care_plan_id
    referral['Patient']['CarePlanTypes'][0]['CarePlanID'] = care_plan_id
    new_referral = Referral(referral)
    uniq_care_plan_id_values = [
        new_referral.Patient.CarePlanTypes[0].CarePlanAgreements[0].UniqCarePlanID,
        new_referral.Patient.CarePlanTypes[0].UniqCarePlanID
    ]
    for uniq_plan_id in uniq_care_plan_id_values:
        assert expected == uniq_plan_id


@pytest.mark.parametrize("org_id_provider, service_req_id, expected", [
    ('RAL', 'SRI00000000000000001', 'RALSRI00000000000000001'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)
])
def test_uniq_service_req_id(referral, org_id_provider, service_req_id, expected):
    referral['Header']['OrgIDProvider'] = org_id_provider
    referral['ServiceRequestId'] = service_req_id
    referral['HospitalProviderSpells'][0]['ServiceRequestId'] = service_req_id
    referral['ProvisionalDiagnoses'][0]['ServiceRequestId'] = service_req_id
    referral['SecondaryDiagnoses'][0]['ServiceRequestId'] = service_req_id
    referral['CodedScoredAssessmentReferrals'][0]['ServiceRequestId'] = service_req_id
    referral['ServiceTypesReferredTo'][0]['ServiceRequestId'] = service_req_id
    referral['OtherReasonsForReferral'][0]['ServiceRequestId'] = service_req_id
    referral['ReferralsToTreatment'][0]['ServiceRequestId'] = service_req_id
    referral['MedicationPrescriptions'][0]['ServiceRequestId'] = service_req_id
    referral['OnwardReferrals'][0]['ServiceRequestId'] = service_req_id
    referral['CareContacts'][0]['ServiceRequestId'] = service_req_id
    new_referral = Referral(referral)
    uniq_care_service_req_id_values = [
        new_referral.HospitalProviderSpells[0].WardStays[0].SubstanceMisuses[0].UniqServReqID,
        new_referral.HospitalProviderSpells[0].WardStays[0].Assaults[0].UniqServReqID,
        new_referral.HospitalProviderSpells[0].WardStays[0].SelfHarms[0].UniqServReqID,
        new_referral.HospitalProviderSpells[0].WardStays[0].TrialLeaves[0].UniqServReqID,
        new_referral.HospitalProviderSpells[0].WardStays[0].HomeLeaves[0].UniqServReqID,
        new_referral.HospitalProviderSpells[0].WardStays[0].LeaveOfAbsences[0].UniqServReqID,
        new_referral.HospitalProviderSpells[0].WardStays[0].UniqServReqID,
        new_referral.HospitalProviderSpells[0].AssignedCareProfessionals[0].UniqServReqID,
        new_referral.HospitalProviderSpells[0].DelayedDischarges[0].UniqServReqID,
        new_referral.HospitalProviderSpells[0].HospitalProviderSpellCommissionersAssignmentPeriods[0].UniqServReqID,
        new_referral.HospitalProviderSpells[0].UniqServReqID,
        new_referral.ProvisionalDiagnoses[0].UniqServReqID,
        new_referral.SecondaryDiagnoses[0].UniqServReqID,
        new_referral.CodedScoredAssessmentReferrals[0].UniqServReqID,
        new_referral.ServiceTypesReferredTo[0].UniqServReqID,
        new_referral.OtherReasonsForReferral[0].UniqServReqID,
        new_referral.ReferralsToTreatment[0].UniqServReqID,
        new_referral.MedicationPrescriptions[0].UniqServReqID,
        new_referral.OnwardReferrals[0].UniqServReqID,
        new_referral.CareContacts[0].OtherAttendances[0].UniqServReqID,
        new_referral.CareContacts[0].CareActivities[0].CodedScoredAssessmentCareActivities[0].UniqServReqID,
        new_referral.CareContacts[0].CareActivities[0].UniqServReqID,
        new_referral.CareContacts[0].UniqServReqID,
        new_referral.UniqServReqID,
    ]
    for uniq_service_id in uniq_care_service_req_id_values:
        assert expected == uniq_service_id


@pytest.mark.parametrize("org_id_provider, care_prof_team_id, expected", [
    ('RAL', 'CPT00000000000000010', 'RALCPT00000000000000010'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)
])
def test_uniq_care_prof_team_id(referral, org_id_provider, care_prof_team_id, expected):
    referral['Header']['OrgIDProvider'] = org_id_provider
    referral['ServiceTypesReferredTo'][0]['CareProfTeamLocalId'] = care_prof_team_id
    referral['CareContacts'][0]['CareProfTeamLocalId'] = care_prof_team_id
    new_referral = Referral(referral)
    uniq_care_prof_team_id_values = [
        new_referral.ServiceTypesReferredTo[0].UniqCareProfTeamID,
        new_referral.CareContacts[0].UniqCareProfTeamID
    ]
    for uniq_care_prof_team_id in uniq_care_prof_team_id_values:
        assert expected == uniq_care_prof_team_id


@pytest.mark.parametrize("org_id_provider, care_contact_id, expected", [
    ('RAL', 'CCI00000000000000010', 'RALCCI00000000000000010'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)
])
def test_uniq_care_contact_id(referral, org_id_provider, care_contact_id, expected):
    referral['Header']['OrgIDProvider'] = org_id_provider
    referral['CareContacts'][0]['CareContactId'] = care_contact_id
    referral['CareContacts'][0]['CareActivities'][0]['CareContactId'] = care_contact_id
    referral['CareContacts'][0]['OtherAttendances'][0]['CareContactId'] = care_contact_id
    new_referral = Referral(referral)
    uniq_care_contact_id_values = [
        new_referral.CareContacts[0].UniqCareContID,
        new_referral.CareContacts[0].CareActivities[0].UniqCareContID,
        new_referral.CareContacts[0].CareActivities[0].CodedScoredAssessmentCareActivities[0].UniqCareContID,
        new_referral.CareContacts[0].OtherAttendances[0].UniqCareContID,
    ]
    for uniq_care_contact_id in uniq_care_contact_id_values:
        assert expected == uniq_care_contact_id


@pytest.mark.parametrize("org_id_provider, care_act_id, expected", [
    ('RAL', 'CAI00000000000000010', 'RALCAI00000000000000010'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)
])
def test_uniq_care_act_id(referral, org_id_provider, care_act_id, expected):
    referral['Header']['OrgIDProvider'] = org_id_provider
    referral['CareContacts'][0]['CareActivities'][0]['CareActId'] = care_act_id
    referral['CareContacts'][0]['CareActivities'][0]['CodedScoredAssessmentCareActivities'][0]['CareActId'] = care_act_id
    new_referral = Referral(referral)
    uniq_care_act_id_values = [
        new_referral.CareContacts[0].CareActivities[0].UniqCareActID,
        new_referral.CareContacts[0].CareActivities[0].CodedScoredAssessmentCareActivities[0].UniqCareActID,

    ]
    for uniq_care_act_id in uniq_care_act_id_values:
        assert expected == uniq_care_act_id


@pytest.mark.parametrize("org_id_provider, act_episode_id, expected", [
    ('RAL', 'MHA00000000000000010', 'RALMHA00000000000000010'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)
])
def test_uniq_mh_act_id(referral, org_id_provider, act_episode_id, expected):
    referral['Header']['OrgIDProvider'] = org_id_provider
    referral['Patient']['MentalHealthActLegalStatusClassificationAssignmentPeriods'][0]['MHActLegalStatusClassPeriodId'] = act_episode_id
    referral['Patient']['MentalHealthActLegalStatusClassificationAssignmentPeriods'][0]['CommunityTreatmentOrderRecalls'][0][
        'MHActLegalStatusClassPeriodId'] = act_episode_id
    referral['Patient']['MentalHealthActLegalStatusClassificationAssignmentPeriods'][0]['CommunityTreatmentOrders'][0][
        'MHActLegalStatusClassPeriodId'] = act_episode_id
    referral['Patient']['MentalHealthActLegalStatusClassificationAssignmentPeriods'][0]['ConditionalDischarges'][0][
        'MHActLegalStatusClassPeriodId'] = act_episode_id
    referral['Patient']['MentalHealthActLegalStatusClassificationAssignmentPeriods'][0]['ResponsibleClinicianAssignmentPeriods'][0][
        'MHActLegalStatusClassPeriodId'] = act_episode_id

    new_referral = Referral(referral)
    uniq_act_episode_id_values = [
        new_referral.Patient.MentalHealthActLegalStatusClassificationAssignmentPeriods[
            0].CommunityTreatmentOrderRecalls[0].UniqMHActEpisodeID,
        new_referral.Patient.MentalHealthActLegalStatusClassificationAssignmentPeriods[
            0].CommunityTreatmentOrders[0].UniqMHActEpisodeID,
        new_referral.Patient.MentalHealthActLegalStatusClassificationAssignmentPeriods[
            0].ConditionalDischarges[0].UniqMHActEpisodeID,
        new_referral.Patient.MentalHealthActLegalStatusClassificationAssignmentPeriods[
            0].ResponsibleClinicianAssignmentPeriods[0].UniqMHActEpisodeID,
        new_referral.Patient.MentalHealthActLegalStatusClassificationAssignmentPeriods[0].UniqMHActEpisodeID,
    ]
    for uniq_act_episode_id in uniq_act_episode_id_values:
        assert expected == uniq_act_episode_id


@pytest.mark.parametrize("org_id_provider, ward_stay_id, expected", [
    ('RAL', 'WSI00000000000000010', 'RALWSI00000000000000010'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)
])
def test_uniq_ward_stay_id(referral, org_id_provider, ward_stay_id, expected):
    referral['Header']['OrgIDProvider'] = org_id_provider
    referral['HospitalProviderSpells'][0]['WardStays'][0]['WardStayId'] = ward_stay_id
    referral['HospitalProviderSpells'][0]['WardStays'][0]['AbsenceWithoutLeaves'][0]['WardStayId'] = ward_stay_id
    referral['HospitalProviderSpells'][0]['WardStays'][0]['Assaults'][0]['WardStayId'] = ward_stay_id
    referral['HospitalProviderSpells'][0]['WardStays'][0]['HomeLeaves'][0]['WardStayId'] = ward_stay_id
    referral['HospitalProviderSpells'][0]['WardStays'][0]['LeaveOfAbsences'][0]['WardStayId'] = ward_stay_id
    referral['HospitalProviderSpells'][0]['WardStays'][0]['SelfHarms'][0]['WardStayId'] = ward_stay_id
    referral['HospitalProviderSpells'][0]['WardStays'][0]['SubstanceMisuses'][0]['WardStayId'] = ward_stay_id
    referral['HospitalProviderSpells'][0]['WardStays'][0]['TrialLeaves'][0]['WardStayId'] = ward_stay_id
    new_referral = Referral(referral)
    uniq_ward_stay_id_values = [
        new_referral.HospitalProviderSpells[0].WardStays[0].UniqWardStayID,
        new_referral.HospitalProviderSpells[0].WardStays[0].AbsenceWithoutLeaves[0].UniqWardStayID,
        new_referral.HospitalProviderSpells[0].WardStays[0].Assaults[0].UniqWardStayID,
        new_referral.HospitalProviderSpells[0].WardStays[0].HomeLeaves[0].UniqWardStayID,
        new_referral.HospitalProviderSpells[0].WardStays[0].LeaveOfAbsences[0].UniqWardStayID,
        new_referral.HospitalProviderSpells[0].WardStays[0].SelfHarms[0].UniqWardStayID,
        new_referral.HospitalProviderSpells[0].WardStays[0].SubstanceMisuses[0].UniqWardStayID,
        new_referral.HospitalProviderSpells[0].WardStays[0].TrialLeaves[0].UniqWardStayID
    ]
    for uniq_ward_stay_id in uniq_ward_stay_id_values:
        assert expected == uniq_ward_stay_id


@pytest.mark.parametrize("org_id_provider, care_episode_id, expected", [
    ('RAL', 'CEI00000000000000010', 'RALCEI00000000000000010'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)
])
def test_uniq_cpa_episode_id(referral, org_id_provider, care_episode_id, expected):
    referral['Header']['OrgIDProvider'] = org_id_provider
    referral['Patient']['CPACareEpisodes'][0]['CPAEpisodeId'] = care_episode_id
    referral['Patient']['CPACareEpisodes'][0]['CPAReviews'][0]['CPAEpisodeId'] = care_episode_id
    new_referral = Referral(referral)
    uniq_cpa_episode_id_values = [
        new_referral.Patient.CPACareEpisodes[0].UniqCPAEpisodeID,
        new_referral.Patient.CPACareEpisodes[0].CPAReviews[0].UniqCPAEpisodeID,

    ]
    for uniq_cpa_episode_id in uniq_cpa_episode_id_values:
        assert expected == uniq_cpa_episode_id


@pytest.mark.parametrize("org_id_provider, cluster_id, expected", [
    ('RAL', 'CTI00000000000000010', 'RALCTI00000000000000010'),
    ('', '', ''),
    (' ', ' ', '  '),
    (None, None, None)
])
def test_uniq_cluster_id(referral, org_id_provider, cluster_id, expected):
    referral['Header']['OrgIDProvider'] = org_id_provider
    referral['Patient']['ClusteringToolAssessments'][0]['ClustId'] = cluster_id
    referral['Patient']['ClusteringToolAssessments'][0]['CareClusters'][0]['ClustId'] = cluster_id
    referral['Patient']['ClusteringToolAssessments'][0]['ClusterAssesses'][0]['ClustId'] = cluster_id
    new_referral = Referral(referral)
    uniq_cluster_id_values = [
        new_referral.Patient.ClusteringToolAssessments[0].UniqClustID,
        new_referral.Patient.ClusteringToolAssessments[0].CareClusters[0].UniqClustID,
        new_referral.Patient.ClusteringToolAssessments[0].ClusterAssesses[0].UniqClustID,
    ]
    for uniq_cluster_id in uniq_cluster_id_values:
        assert expected == uniq_cluster_id


@pytest.mark.parametrize("event_id, row_num, expected", [
    ('1234:567', 11, '1234000000011'),
    ('1234567899', 111111, '1234567899000111111')
])
def test_uniq_table_id(referral, event_id, row_num, expected):
    referral['META']['EVENT_ID'] = event_id
    referral["Header"]["RowNumber"] = row_num
    referral["HospitalProviderSpells"][0]["RowNumber"] = row_num
    referral["HospitalProviderSpells"][0]["WardStays"][0]["RowNumber"] = row_num
    referral["HospitalProviderSpells"][0]["WardStays"][0]["SubstanceMisuses"][0]["RowNumber"] = row_num
    referral["HospitalProviderSpells"][0]["WardStays"][0]["Assaults"][0]["RowNumber"] = row_num
    referral["HospitalProviderSpells"][0]["WardStays"][0]["SelfHarms"][0]["RowNumber"] = row_num
    referral["HospitalProviderSpells"][0]["WardStays"][0]["TrialLeaves"][0]["RowNumber"] = row_num
    referral["HospitalProviderSpells"][0]["WardStays"][0]["HomeLeaves"][0]["RowNumber"] = row_num
    referral["HospitalProviderSpells"][0]["WardStays"][0]["LeaveOfAbsences"][0]["RowNumber"] = row_num
    referral["HospitalProviderSpells"][0]["WardStays"][0]["AbsenceWithoutLeaves"][0]["RowNumber"] = row_num
    referral["HospitalProviderSpells"][0]["AssignedCareProfessionals"][0]["RowNumber"] = row_num
    referral["HospitalProviderSpells"][0]["DelayedDischarges"][0]["RowNumber"] = row_num
    referral["HospitalProviderSpells"][0]["HospitalProviderSpellCommissionersAssignmentPeriods"][0]["RowNumber"] = row_num
    referral["ProvisionalDiagnoses"][0]["RowNumber"] = row_num
    referral["PrimaryDiagnoses"][0]["RowNumber"] = row_num
    referral["SecondaryDiagnoses"][0]["RowNumber"] = row_num
    referral["CodedScoredAssessmentReferrals"][0]["RowNumber"] = row_num
    referral["ServiceTypesReferredTo"][0]["RowNumber"] = row_num
    referral["MedicationPrescriptions"][0]["RowNumber"] = row_num
    referral["OtherReasonsForReferral"][0]["RowNumber"] = row_num
    referral["ReferralsToTreatment"][0]["RowNumber"] = row_num
    referral["OnwardReferrals"][0]["RowNumber"] = row_num
    referral["DischargePlanAgreements"][0]["RowNumber"] = row_num
    referral["IndirectActivities"][0]["RowNumber"] = row_num
    referral["CareContacts"][0]["RowNumber"] = row_num
    referral["CareContacts"][0]["CareActivities"][0]["RowNumber"] = row_num
    referral["CareContacts"][0]["CareActivities"][0]["CodedScoredAssessmentCareActivities"][0]["RowNumber"] = row_num
    referral["CareContacts"][0]["OtherAttendances"][0]["RowNumber"] = row_num
    referral["Patient"]["RowNumber"] = row_num
    referral["Patient"]["FiveForensicPathways"][0]["RowNumber"] = row_num
    referral["Patient"]["ClusteringToolAssessments"][0]["RowNumber"] = row_num
    referral["Patient"]["ClusteringToolAssessments"][0]["ClusterAssesses"][0]["RowNumber"] = row_num
    referral["Patient"]["EmploymentStatuses"][0]["RowNumber"] = row_num
    referral["Patient"]["AccommodationStatuses"][0]["RowNumber"] = row_num
    referral["Patient"]["GPs"][0]["RowNumber"] = row_num
    referral["Patient"]["PatientIndicators"][0]["RowNumber"] = row_num
    referral["Patient"]["MentalHealthCareCoordinators"][0]["RowNumber"] = row_num
    referral["Patient"]["DisabilityTypes"][0]["RowNumber"] = row_num
    referral["Patient"]["AssistiveTechnologiesToSupportDisabilityTypes"][0]["RowNumber"] = row_num
    referral["Patient"]["SocialAndPersonalCircumstances"][0]["RowNumber"] = row_num
    referral["Patient"]["CarePlanTypes"][0]["RowNumber"] = row_num
    referral["Patient"]["CarePlanTypes"][0]["CarePlanAgreements"][0]["RowNumber"] = row_num
    referral["Patient"]["MedicalHistoryPreviousDiagnoses"][0]["RowNumber"] = row_num
    referral["Patient"]["OverseasVisitorChargingCategories"][0]["RowNumber"] = row_num
    referral["Patient"]["CPACareEpisodes"][0]["RowNumber"] = row_num
    referral["Patient"]["CPACareEpisodes"][0]["CPAReviews"][0]["RowNumber"] = row_num
    referral["Patient"]["MentalHealthActLegalStatusClassificationAssignmentPeriods"][0]["RowNumber"] = row_num
    referral["Patient"]["MentalHealthActLegalStatusClassificationAssignmentPeriods"][0]["ResponsibleClinicianAssignmentPeriods"][0]["RowNumber"] = row_num
    referral["Patient"]["MentalHealthActLegalStatusClassificationAssignmentPeriods"][0]["ConditionalDischarges"][0]["RowNumber"] = row_num
    referral["Patient"]["MentalHealthActLegalStatusClassificationAssignmentPeriods"][0]["CommunityTreatmentOrders"][0]["RowNumber"] = row_num
    referral["Patient"]["MentalHealthActLegalStatusClassificationAssignmentPeriods"][0]["CommunityTreatmentOrderRecalls"][0]["RowNumber"] = row_num
    new_referral = Referral(referral)

    assert new_referral.Header.MHS000UniqID == int(expected)
    assert new_referral.HospitalProviderSpells[0].MHS501UniqID == int(expected)
    assert new_referral.HospitalProviderSpells[0].WardStays[0].MHS502UniqID == int(expected)
    assert new_referral.HospitalProviderSpells[0].WardStays[0].SubstanceMisuses[0].MHS513UniqID == int(expected)
    assert new_referral.HospitalProviderSpells[0].WardStays[0].Assaults[0].MHS506UniqID == int(expected)
    assert new_referral.HospitalProviderSpells[0].WardStays[0].SelfHarms[0].MHS507UniqID == int(expected)
    assert new_referral.HospitalProviderSpells[0].WardStays[0].TrialLeaves[0].MHS514UniqID == int(expected)
    assert new_referral.HospitalProviderSpells[0].WardStays[0].HomeLeaves[0].MHS509UniqID == int(expected)
    assert new_referral.HospitalProviderSpells[0].WardStays[0].LeaveOfAbsences[0].MHS510UniqID == int(expected)
    assert new_referral.HospitalProviderSpells[0].WardStays[0].AbsenceWithoutLeaves[0].MHS511UniqID == int(expected)
    assert new_referral.HospitalProviderSpells[0].AssignedCareProfessionals[0].MHS503UniqID == int(expected)
    assert new_referral.HospitalProviderSpells[0].DelayedDischarges[0].MHS504UniqID == int(expected)
    assert new_referral.HospitalProviderSpells[0].HospitalProviderSpellCommissionersAssignmentPeriods[
        0].MHS512UniqID == int(expected)
    assert new_referral.ProvisionalDiagnoses[0].MHS603UniqID == int(expected)
    assert new_referral.PrimaryDiagnoses[0].MHS604UniqID == int(expected)
    assert new_referral.SecondaryDiagnoses[0].MHS605UniqID == int(expected)
    assert new_referral.CodedScoredAssessmentReferrals[0].MHS606UniqID == int(expected)
    assert new_referral.ServiceTypesReferredTo[0].MHS102UniqID == int(expected)
    assert new_referral.MedicationPrescriptions[0].MHS107UniqID == int(expected)
    assert new_referral.OtherReasonsForReferral[0].MHS103UniqID == int(expected)
    assert new_referral.ReferralsToTreatment[0].MHS104UniqID == int(expected)
    assert new_referral.OnwardReferrals[0].MHS105UniqID == int(expected)
    assert new_referral.DischargePlanAgreements[0].MHS106UniqID == int(expected)
    assert new_referral.IndirectActivities[0].MHS204UniqID == int(expected)
    assert new_referral.CareContacts[0].MHS201UniqID == int(expected)
    assert new_referral.CareContacts[0].CareActivities[0].MHS202UniqID == int(expected)
    assert new_referral.CareContacts[0].CareActivities[0].CodedScoredAssessmentCareActivities[0].MHS607UniqID == int(
        expected)
    assert new_referral.CareContacts[0].OtherAttendances[0].MHS203UniqID == int(expected)
    assert new_referral.Patient.MHS001UniqID == int(expected)
    assert new_referral.Patient.FiveForensicPathways[0].MHS804UniqID == int(expected)
    assert new_referral.Patient.ClusteringToolAssessments[0].MHS801UniqID == int(expected)
    assert new_referral.Patient.ClusteringToolAssessments[0].ClusterAssesses[0].MHS802UniqID == int(expected)
    assert new_referral.Patient.EmploymentStatuses[0].MHS004UniqID == int(expected)
    assert new_referral.Patient.AccommodationStatuses[0].MHS003UniqID == int(expected)
    assert new_referral.Patient.GPs[0].MHS002UniqID == int(expected)
    assert new_referral.Patient.PatientIndicators[0].MHS005UniqID == int(expected)
    assert new_referral.Patient.MentalHealthCareCoordinators[0].MHS006UniqID == int(expected)
    assert new_referral.Patient.OverseasVisitorChargingCategories[0].MHS012UniqID == int(expected)
    assert new_referral.Patient.DisabilityTypes[0].MHS007UniqID == int(expected)
    assert new_referral.Patient.AssistiveTechnologiesToSupportDisabilityTypes[0].MHS010UniqID == int(expected)
    assert new_referral.Patient.SocialAndPersonalCircumstances[0].MHS011UniqID == int(expected)
    assert new_referral.Patient.CarePlanTypes[0].MHS008UniqID == int(expected)
    assert new_referral.Patient.CarePlanTypes[0].CarePlanAgreements[0].MHS009UniqID == int(expected)
    assert new_referral.Patient.MedicalHistoryPreviousDiagnoses[0].MHS601UniqID == int(expected)
    assert new_referral.Patient.CPACareEpisodes[0].MHS701UniqID == int(expected)
    assert new_referral.Patient.CPACareEpisodes[0].CPAReviews[0].MHS702UniqID == int(expected)
    assert new_referral.Patient.MentalHealthActLegalStatusClassificationAssignmentPeriods[0].MHS401UniqID == int(
        expected)
    assert new_referral.Patient.MentalHealthActLegalStatusClassificationAssignmentPeriods[0].ResponsibleClinicianAssignmentPeriods[
        0].MHS402UniqID == int(expected)
    assert new_referral.Patient.MentalHealthActLegalStatusClassificationAssignmentPeriods[0].ConditionalDischarges[0].MHS403UniqID == int(
        expected)
    assert new_referral.Patient.MentalHealthActLegalStatusClassificationAssignmentPeriods[0].CommunityTreatmentOrders[0].MHS404UniqID == int(
        expected)
    assert new_referral.Patient.MentalHealthActLegalStatusClassificationAssignmentPeriods[0].CommunityTreatmentOrderRecalls[
        0].MHS405UniqID == int(expected)


@pytest.mark.parametrize("event_id, row_num, expected", [
    ('1234:567', 11, 1234000000011)
])
def test_record_number(referral, event_id, row_num, expected):
    referral['META']['EVENT_ID'] = event_id
    referral['Patient']['RowNumber'] = row_num
    new_referral = Referral(referral)
    assert expected == new_referral.Patient.RecordNumber


@pytest.mark.parametrize("rep_period_start_date, gp_practice_code, expected", [
    (datetime.date(2020, 1, 1), 'A81004', '00M'),
    (datetime.date(2020, 1, 1), 'bad practice code', None),
    (datetime.date(2020, 1, 1), None, None),
    (datetime.date(2023, 3, 31), 'A81017', '16C'),
    (datetime.date(2023, 4, 1), 'A81017', None),
])
def test_ccg_from_gp_practice(referral, rep_period_start_date, gp_practice_code, expected):
    referral['Header']['ReportingPeriodStartDate'] = rep_period_start_date
    referral['Patient']['GPs'][0]['GMPCodeReg'] = gp_practice_code
    new_referral = Referral(referral)  # creates a table
    assert expected == new_referral.Patient.GPs[0].OrgIDCCGGPPractice
