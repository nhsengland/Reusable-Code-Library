from typing import Dict, Type, Any
import pytest
from datetime import date

from dsp.datasets.models.mhsds_v5 import (
    GroupSession, MentalHealthDropInContact,
    AnonymousSelfAssessment, StaffDetails
)
from dsp.common.structured_model import DSPStructuredModel

__all__ = [
    'referral_derivations_dict',
    # Only use dict 2 for overlapping derivations tests
    'referral_derivations_dict_2',
    'group_session_derivations_dict',
    'mental_health_drop_in_contact_derivations_dict',
    'anonymous_self_assessment_derivations_dict',
    'staff_details_derivations_dict'
]

community_treatment_order_recall_derivations_dict = {
    'UniqMHActEpisodeID': 'B0BMHI00000000000000001'
}
community_treatment_order_derivations_dict = {
    'UniqMHActEpisodeID': 'B0BMHI00000000000000001'
}
conditional_discharge_derivations_dict = {
    'UniqMHActEpisodeID': 'B0BMHI00000000000000001'
}
self_harm_derivations_dict = {
    'UniqHospProvSpellID': 'B0BHSN00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
police_assistance_request_derivations_dict = {
    'UniqHospProvSpellID': 'B0BHSN00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
other_in_attendance_derivations_dict = {
    'UniqCareContID': 'B0BCPI00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
care_plan_agreement_derivations_dict = {
    'UniqCarePlanID': 'B0BCPI00000000000000001'
}
mental_health_responsible_clinician_assignment_period_derivations_dict = {
    'UniqCareProfLocalID': 'B0BCPI00000000000000001',
    'UniqMHActEpisodeID': 'B0BMHI00000000000000001'
}
restrictive_intervention_type_derivations_dict = {
    'UniqRestrictiveIntIncID': 'B0BRII00000000000000001',
    'UniqRestrictiveIntTypeID': 'B0BRITI0000000000000001',
    'UniqHospProvSpellID': 'B0BHPI00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
restrictive_intervention_incident_derivations_dict = {
    'UniqRestrictiveIntIncID': 'B0BRII00000000000000001',
    'UniqHospProvSpellID': 'B0BHPI00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001',
    'RestrictiveInterventionTypes': [
        restrictive_intervention_type_derivations_dict
    ]
}
meta_derivations_dict = {}
absence_without_leave_derivations_dict = {
    'UniqHospProvSpellID': 'B0BHSN00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
hospital_provider_spell_commissioner_assignment_period_derivations_dict = {
    'UniqHospProvSpellID': 'B0BHPI00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
substance_misuse_derivations_dict = {
    'UniqHospProvSpellID': 'B0BHSN00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
trial_leave_derivations_dict = {
    'UniqHospProvSpellID': 'B0BHSN00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
specialised_mental_health_exceptional_package_of_care_derivations_dict = {
    'UniqHospProvSpellID': 'B0BHSN00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
medical_history_previous_diagnosis_derivations_dict = {
    'MapSnomedCTPrevDiagCode': None,
    'MapSnomedCTPrevDiagTerm': None,
    'MasterSnomedCTPrevDiagCode': None,
    'MasterSnomedCTPrevDiagTerm': None,
    'MapICD10PrevDiagCode': None,
    'MapICD10PrevDiagDesc': None,
    'MasterICD10PrevDiagCode': None,
    'MasterICD10PrevDiagDesc': None
}
provisional_diagnosis_derivations_dict = {
    'MapSnomedCTProvDiagCode': None,
    'MapSnomedCTProvDiagTerm': None,
    'MasterSnomedCTProvDiagCode': None,
    'MasterSnomedCTProvDiagTerm': None,
    'MapICD10ProvDiagCode': None,
    'MapICD10ProvDiagDesc': None,
    'MasterICD10ProvDiagCode': None,
    'MasterICD10ProvDiagDesc': None,
    'UniqServReqID': 'B0BSRI00000000000000001'
}
primary_diagnosis_derivations_dict = {
    'MapSnomedCTPrimDiagCode': None,
    'MapSnomedCTPrimDiagTerm': None,
    'MasterSnomedCTPrimDiagCode': None,
    'MapICD10PrimDiagCode': None,
    'MapICD10PrimDiagDesc': None,
    'MasterICD10PrimDiagCode': None,
    'MasterICD10PrimDiagDesc': None,
    'UniqServReqID': 'B0BSRI00000000000000001'
}
secondary_diagnosis_derivations_dict = {
    'MapSnomedCTSecDiagCode': None,
    'MapSnomedCSecDiagTerm': None,
    'MasterSnomedCTSecDiagCode': None,
    'MasterSnomedCTSecDiagTerm': None,
    'MapICD10SecDiagCode': None,
    'MapICD10SecDiagDesc': None,
    'MasterICD10SecDiagCode': None,
    'MasterICD10SecDiagDesc': None,
    'UniqServReqID': 'B0BSRI00000000000000001'
}
coded_scored_assessment_referral_derivations_dict = {
    'UniqCareProfLocalID': 'B0BCPL00000000000000001',
    'AgeAssessToolReferCompDate': 41,
    'UniqServReqID': 'B0BSRI00000000000000001'
}
coded_scored_assessment_care_activity_dict = {
    'AgeAssessToolCont': 34,
    'UniqCareContID': 'B0BCCI00000000000000001',
    'UniqCareActID': 'B0BCAI00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
care_program_approach_review_derivations_dict = {
    'UniqCareProfLocalID': 'B0BCPL00000000000000001',
    'UniqCPAEpisodeID': 'B0BCEI00000000000000001'
}
cluster_assess_derivations_dict = {
    'UniqClustID': 'B0BCTI00000000000000001'
}
care_cluster_derivations_dict = {
    'InactTimeCC': date(2018, 5, 5),
    'UniqClustID': 'B0BCTI00000000000000001'
}
five_forensic_pathways_derivations_dict = {}
cluster_tool_derivations_dict = {
    'UniqClustID': 'B0BCTI00000000000000001',
    'CareClusters': [
        care_cluster_derivations_dict
    ],
    'ClusterAssesses': [
        cluster_assess_derivations_dict
    ]
}
care_program_approach_care_episode_derivations_dict = {
    'UniqCPAEpisodeID': 'B0BCEI00000000000000001',
    'CPAReviews': [
        care_program_approach_review_derivations_dict
    ]
}
mental_health_act_legal_status_classification_assignment_period_derivations_dict = {
    'InactTimeMHAPeriod': date(2018, 5, 5),
    'NHSDLegalStatus': '01',
    'UniqMHActEpisodeID': 'B0BLSI00000000000000001',
    'ConditionalDischarges': [
        conditional_discharge_derivations_dict
    ],
    'ResponsibleClinicianAssignmentPeriods': [
        mental_health_responsible_clinician_assignment_period_derivations_dict
    ],
    'CommunityTreatmentOrders': [
        community_treatment_order_derivations_dict
    ],
    'CommunityTreatmentOrderRecalls': [
        community_treatment_order_recall_derivations_dict
    ]
}
care_activity_derivations_dict = {
    'MapSnomedCTFindingCode': None,
    'MapSnomedCTFindingTerm': None,
    'MasterSnomedCTFindingCode': None,
    'MasterSnomedCTFindingTerm': None,
    'MapICD10FindingCode': None,
    'MapICD10FindingDesc': None,
    'MasterICD10FindingCode': None,
    'MasterICD10FindingDesc': None,
    'MapSnomedCTObsCode': None,
    'MapSnomedCTObsTerm': None,
    'MasterSnomedCTObsCode': None,
    'MasterSnomedCTObsTerm': None,
    'UniqCareProfLocalID': 'B0BCPL00000000000000001',
    'UniqCareActID': 'B0BCAI00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001',
    'CodedScoredAssessmentCareActivities': [
        coded_scored_assessment_care_activity_dict
    ],
    'UniqCareContID': 'B0BCCI00000000000000001'
}
care_contact_derivations_dict = {
    'AgeCareContDate': 34,
    'ContLocDistanceHome': None,
    'UniqCareProfTeamID': 'B0BCPI00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001',
    'CareActivities': [
        care_activity_derivations_dict
    ],
    'OtherAttendances': [
        other_in_attendance_derivations_dict
    ]
}
care_plan_type_derivations_dict = {
    'CarePlanAgreements': [
        care_plan_agreement_derivations_dict
    ],
    'UniqCarePlanID': 'B0BCPI00000000000000001'
}
accommodation_status_derivations_dict = {
    'AgeAccomTypeDate': 34
}
assault_derivations_dict = {
    'UniqHospProvSpellID': 'B0BHSN00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
assigned_care_professional_derivations_dict = {
    'UniqCareProfLocalID': 'B0BCPI00000000000000001',
    'UniqHospProvSpellID': 'B0BHPI00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
assistive_technology_to_support_disability_type_derivations_dict = {}
delayed_discharge_derivations_dict = {
    'UniqHospProvSpellID': 'B0BHPI00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
disability_type_derivations_dict = {}
discharge_plan_agreement_derivations_dict = {
    'UniqServReqID': 'B0BSRI00000000000000001'
}
employment_status_derivations_dict = {}
gp_derivations_dict = {
    'GPDistanceHome': 266,
    'OrgIDCCGGPPractice': '07X',
    'OrgIDSubICBLocGP': '07X',
    'OrgIDICBGPPractice': 'Q71'
}
header_derivations_dict = {}
home_leave_derivations_dict = {
    'UniqHospProvSpellID': 'B0BHSN00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
indirect_activity_derivations_dict = {
    'MapSnomedCTFindingCode': None,
    'MapSnomedCTFindingTerm': None,
    'MasterSnomedCTFindingCode': None,
    'MasterSnomedCTFindingTerm': None,
    'MapICD10FindingCode': None,
    'MapICD10FindingDesc': None,
    'MasterICD10FindingCode': None,
    'MasterICD10FindingDesc': None,
    'UniqCareProfLocalID': 'B0BCPI00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
leave_of_absence_derivations_dict = {
    'LOADaysRP': 0,
    'UniqHospProvSpellID': 'B0BHSN00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001'
}
ward_stay_derivations_dict = {
    'UniqHospProvSpellID': 'B0BHSN00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001',
    'LeaveOfAbsences': [
        leave_of_absence_derivations_dict
    ],
    'PoliceAssistanceRequests': [
        police_assistance_request_derivations_dict
    ],
    'Assaults': [
        assault_derivations_dict
    ],
    'SelfHarms': [
        self_harm_derivations_dict
    ],
    'HomeLeaves': [
        home_leave_derivations_dict
    ],
    'AbsenceWithoutLeaves': [
        absence_without_leave_derivations_dict
    ],
    'SubstanceMisuses': [
        substance_misuse_derivations_dict
    ],
    'TrialLeaves': [
        trial_leave_derivations_dict
    ],
    'HospitalBedTypeName': 'Acute adult mental health care',
    'WardSayBedTypesLkup': 4,
    'InactTimeWS': date(2018, 5, 5),
    'WardLocDistanceHome': 272
}
hospital_provider_spell_derivations_dict = {
    'MHS501UniqID': 1066000000004,
    'UniqServReqID': 'B0BSRI00000000000000001',
    'UniqHospProvSpellID': 'B0BHPI00000000000000001',
    'InactTimeHPS': date(2018, 5, 5),
    'RestrictiveInterventionIncidents': [
        restrictive_intervention_incident_derivations_dict
    ],
    'WardStays': [
        ward_stay_derivations_dict
    ],
    'AssignedCareProfessionals': [
        assigned_care_professional_derivations_dict
    ],
    'DelayedDischarges': [
        delayed_discharge_derivations_dict
    ],
    'HospitalProviderSpellCommissionersAssignmentPeriods': [
        hospital_provider_spell_commissioner_assignment_period_derivations_dict
    ],
    'SpecialisedMentalHealthExceptionalPackageOfCares': [
        specialised_mental_health_exceptional_package_of_care_derivations_dict
    ]
}
medication_prescription_derivations_dict = {
    'MHS107UniqID': 1066000000006,
    'UniqServReqID': 'B0BSRI00000000000000001'
}
medication_prescription_derivations_dict_2 = {
    'MHS107UniqID': 1066000000007
}
mental_health_care_coordinator_derivations_dict = {
    'InactTimeCC': date(2018, 5, 5),
    'UniqCareProfLocalID': 'B0BCPI00000000000000001'
}
mental_health_currency_model_derivations_dict = {}
onward_referral_derivations_dict = {
    'UniqServReqID': 'B0BSRI00000000000000001'
}
other_reason_for_referral_derivations_dict = {
    'UniqServReqID': 'B0BSRI00000000000000001'
}
overseas_visitor_charging_category_derivations_dict = {}
patient_indicators_derivations_dict = {}
referral_to_treatment_derivations_dict = {
    'AgeReferTreatStartDate': 34,
    'AgeReferTreatEndDate': 34,
    'UniqServReqID': 'B0BSRI00000000000000001'
}
service_type_referred_to_derivations_dict = {
    'InactTimeST': date(2018, 5, 5),
    'AgeServReferRejection': 34,
    'UniqCareProfTeamID': 'B0BCPI00000000000000001',
    'UniqServReqID': 'B0BSRI00000000000000001',
}
service_type_referred_to_derivations_dict_2 = {
    'AgeServReferClosure': 34
}
social_and_personal_circumstances_derivations_dict = {}
master_patient_index_derivations_dict = {
    'GPs': [
        gp_derivations_dict
    ],
    'AccommodationStatuses': [
        accommodation_status_derivations_dict
    ],
    'MentalHealthCareCoordinators': [
        mental_health_care_coordinator_derivations_dict
    ],
    'ClusteringToolAssessments': [
        cluster_tool_derivations_dict
    ],
    'CPACareEpisodes': [
        care_program_approach_care_episode_derivations_dict
    ],
    'CarePlanTypes': [
        care_plan_type_derivations_dict
    ],
    'MentalHealthActLegalStatusClassificationAssignmentPeriods': [
        mental_health_act_legal_status_classification_assignment_period_derivations_dict
    ],
    'EthnicityHigher': 'White',
    'EthnicityLow': 'British',
    'LADistrictAuth': 'E08000035',
    'LSOA2011': 'E01032501',
    'AgeDeath': 34,
    'AgeRepPeriodStart': 33,
    'OrgIDCCGRes': '03C',
    'OrgIDSubICBLocResidence': '03C',
    'OrgIDICBRes': None
}
referral_derivations_dict = {
    'MHS101UniqID': 1066000000001,
    'UniqMonthID': 1414,
    'Patient': master_patient_index_derivations_dict,
    'FirstAttendedContactInRPDate': date(2018, 5, 4),
    'FirstContactEverDate': None,
    'InactTimeRef': date(2018, 5, 5),
    'AgeServReferRecDate': 34,
    'UniqServReqID': 'B0BSRI00000000000000001',
    'HospitalProviderSpells': [
        hospital_provider_spell_derivations_dict
    ],
    'CountOfAttendedCareContacts': None,
    'MedicationPrescriptions': [
        medication_prescription_derivations_dict,
        medication_prescription_derivations_dict_2
    ],
    'CareContacts': [
        care_contact_derivations_dict
    ],
    'IndirectActivities': [
        indirect_activity_derivations_dict
    ],
    'CodedScoredAssessmentReferrals': [
        coded_scored_assessment_referral_derivations_dict
    ],
    'ServiceTypesReferredTo': [
        service_type_referred_to_derivations_dict,
        service_type_referred_to_derivations_dict_2
    ],
    'ReferralsToTreatment': [
        referral_to_treatment_derivations_dict
    ],
    'OtherReasonsForReferral': [
        other_reason_for_referral_derivations_dict
    ],
    'OnwardReferrals': [
        onward_referral_derivations_dict
    ],
    'DischargePlanAgreements': [
        discharge_plan_agreement_derivations_dict
    ],
    'ProvisionalDiagnoses': [
        provisional_diagnosis_derivations_dict
    ],
    'PrimaryDiagnoses': [
        primary_diagnosis_derivations_dict
    ],
    'SecondaryDiagnoses': [
        secondary_diagnosis_derivations_dict
    ]
}
referral_derivations_dict_2 = {
    'AgeServReferDischDate': 34,
    'Patient': master_patient_index_derivations_dict,

}
group_session_derivations_dict = {
    'MHS301UniqID': 1066000000000,
    'UniqCareProfLocalID': 'B0BCPI00000000000000001',
    'UniqMonthID': 1414
}
mental_health_drop_in_contact_derivations_dict = {
    'AgeRepPeriodEnd': 34,
    'AgeRepPeriodStart': 33,
    'MHS302UniqID': 1066000000001,
    'NHSDEthnicity': 'A',
    'UniqCareProfLocalID': "B0BCPI00000000000000001",
    'UniqMonthID': 1414,
    'UniqMHDropInContactId': "B0BDCI00000000000000001"
}
anonymous_self_assessment_derivations_dict = {
    'MHS608UniqID': 1066000000000,
    'UniqMonthID': 1414
}
staff_details_derivations_dict = {
    'MHS901UniqID': 1066000000003,
    'UniqCareProfLocalID': 'B0BCPI00000000000000001',
    'UniqMonthID': 1414
}


# TODO: Add all models to test once all derivations have been added to the above dictionaries.
@pytest.mark.parametrize(['values', 'model'], [
    (group_session_derivations_dict, GroupSession),
    (mental_health_drop_in_contact_derivations_dict, MentalHealthDropInContact),
    (anonymous_self_assessment_derivations_dict, AnonymousSelfAssessment),
    (staff_details_derivations_dict, StaffDetails),
])
def test_contains_all_derived_values(values: Dict[str, Any], model: Type[DSPStructuredModel]):
    given_fields = values.keys()
    required_fields = model.get_field_names(DSPStructuredModel.derived_fields_only)
    assert set(given_fields) == set(required_fields),\
        'Required {} but got {}'.format(required_fields, given_fields)
