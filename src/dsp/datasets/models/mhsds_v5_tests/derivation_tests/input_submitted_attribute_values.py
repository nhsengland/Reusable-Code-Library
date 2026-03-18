from datetime import datetime, date
from typing import Dict, Any, Type

import pytest

from dsp.dam import current_record_version
from dsp.datasets.models.mhsds_v5 import (
    AbsenceWithoutLeave,
    AccommodationStatus,
    AnonymousSelfAssessment,
    Assault,
    AssignedCareProfessional,
    AssistiveTechnologyToSupportDisabilityType,
    CareActivity,
    CareCluster,
    CareContact,
    CarePlanAgreement,
    CarePlanType,
    CareProgramApproachCareEpisode,
    CareProgramApproachReview,
    ClusterAssess,
    ClusterTool,
    CodedScoredAssessmentCareActivity,
    CodedScoredAssessmentReferral,
    CommunityTreatmentOrder,
    CommunityTreatmentOrderRecall,
    ConditionalDischarge,
    DelayedDischarge,
    DisabilityType,
    DischargePlanAgreement,
    EmploymentStatus,
    FiveForensicPathways,
    GP,
    GroupSession,
    Header,
    HomeLeave,
    HospitalProviderSpell,
    HospitalProviderSpellCommissionerAssignmentPeriod,
    IndirectActivity,
    LeaveOfAbsence,
    MasterPatientIndex,
    MedicalHistoryPreviousDiagnosis,
    MedicationPrescription,
    MentalHealthActLegalStatusClassificationAssignmentPeriod,
    MentalHealthCareCoordinator,
    MentalHealthCurrencyModel,
    MentalHealthDropInContact,
    MentalHealthResponsibleClinicianAssignmentPeriod,
    OnwardReferral,
    OtherInAttendance,
    OtherReasonForReferral,
    OverseasVisitorChargingCategory,
    PatientIndicators,
    PoliceAssistanceRequest,
    PrimaryDiagnosis,
    ProvisionalDiagnosis,
    Referral,
    ReferralToTreatment,
    RestrictiveInterventionIncident,
    RestrictiveInterventionType,
    SecondaryDiagnosis,
    SelfHarm,
    ServiceTypeReferredTo,
    SocialAndPersonalCircumstances,
    SpecialisedMentalHealthExceptionalPackageOfCare,
    StaffDetails,
    SubstanceMisuse,
    TrialLeave,
    WardStay
)
from dsp.common.structured_model import DSPStructuredModel, META
from dsp.shared.constants import DS

__all__ = [
    'meta_dict',
    'referral_dict',
    # Only use dict 2 for overlapping derivations tests
    'referral_dict_2',
    'group_session_dict',
    'mental_health_drop_in_contact_dict',
    'anonymous_self_assessment_dict',
    'staff_details_dict'
]

meta_dict = {
    'DATASET_VERSION': 5,
    'EVENT_ID': '1066:5',
    'EVENT_RECEIVED_TS': datetime(2021, 1, 1),
    'RECORD_INDEX': 0,
    'RECORD_VERSION': current_record_version(DS.MHSDS_V5)
}

#  MHS511AbsenceWithoutLeave
absence_without_leave_dict = {
    'EndDateMHAbsWOLeave': date(2018, 5, 24),
    'EndTimeMHAbsWOLeave': datetime(2018, 5, 24, 5, 30, 10),
    'MHAbsWOLeaveEndReason': '01',
    'RowNumber': 1,
    'StartDateMHAbsWOLeave': date(2018, 5, 24),
    'StartTimeMHAbsWOLeave': datetime(2018, 5, 24, 2, 30, 10),
    'WardStayId': 'WSI00000000000000001',
}

#  MHS512HospSpellCommAssPer
hospital_provider_spell_commissioner_assignment_period_dict = {
    'EndDateOrgCodeComm': date(2018, 5, 28),
    'HospProvSpellID': 'HSN00000000000000001',
    'OrgIDComm': '06Q',
    'RowNumber': 1,
    'StartDateOrgCodeComm': date(2018, 5, 22)
}

#  MHS513SubstanceMisuse
substance_misuse_dict = {
    'RowNumber': 1,
    'SubstanceMisuse': date(2018, 5, 24),
    'WardStayId': 'HSN00000000000000001'
}

#  MHS514TrialLeave
trial_leave_dict = {
    'EndDateMHTrialLeave': date(2018, 5, 24),
    'EndTimeMHTrialLeave': datetime(2018, 5, 24, 15, 57, 00),
    'RowNumber': 1,
    'StartDateMHTrialLeave': date(2018, 5, 24),
    'StartTimeMHTrialLeave': datetime(2018, 5, 24, 5, 3, 57),
    'WardStayId': 'WSI00000000000000001'
}

#  MHS517SMHExceptionalPackOfCare
specialised_mental_health_exceptional_package_of_care_dict = {
    'EndDateSMHEPC': date(2018, 5, 25),
    'HospProvSpellID': 'HSN00000000000000001',
    'OrgIDComm': '08Q',
    'RowNumber': 1,
    'SMHEPCCharge': "222.00",
    'StartDateSMHEPC': date(2018, 5, 25)
}

#  MHS601MedHistPrevDiag
medical_history_previous_diagnosis_dict = {
    'CodedDiagTimestamp': "1986-07-09T08:22:37+01:00",
    'DiagSchemeInUse': '06',
    'LocalPatientId': 'LPI00000000000000001',
    'PrevDiag': '277891000',
    'RowNumber': 1
}

#  MHS603ProvDiag
provisional_diagnosis_dict = {
    'CodedProvDiagTimestamp': "1981-10-28T06:06:24+01:00",
    'DiagSchemeInUse': '06',
    'ProvDiag': '47745005',
    'RowNumber': 1,
    'ServiceRequestId': 'SRI00000000000000001'
}

#  MHS604PrimDiag
primary_diagnosis_dict = {
    'CodedDiagTimestamp': "1981-10-28T06:06:24+00:00",
    'DiagSchemeInUse': '06',
    'PrimDiag': '45411003',
    'RowNumber': 1,
    'ServiceRequestId': 'SRI00000000000000001'
}

#  MHS605SecDiag
secondary_diagnosis_dict = {
    'CodedDiagTimestamp': "1981-10-28T06:06:24+00:00",
    'DiagSchemeInUse': '06',
    'RowNumber': 1,
    'SecDiag': '422929003',
    'ServiceRequestId': 'SRI00000000000000001'
}

#  MHS606CodedScoreAssessmentRefer
coded_scored_assessment_referral_dict = {
    'CareActId': 'CAI00000000000000001',
    'AssToolCompTimestamp': "2025-10-28T06:06:24+00:00",
    'CareProfLocalId': 'CPL00000000000000001',
    'CodedAssToolType': '218023008',
    'PersScore': '12345',
    'RowNumber': 1,
    'ServiceRequestId': 'SRI00000000000000001'
}

#  MHS607CodedScoreAssessmentAct
coded_scored_assessment_care_activity_dict = {
    'CareActId': 'CAI00000000000000001',
    'CodedAssToolType': '242129005',
    'PersScore': '1010',
    'RowNumber': 1
}

#  MHS702CPAReview
care_program_approach_review_dict = {
    'CPAEpisodeId': 'CEI00000000000000001',
    'CPAReviewDate': date(2018, 5, 20),
    'CareProfLocalId': 'CPL00000000000000001',
    'RowNumber': 1
}

#  MHS802ClusterAssess
cluster_assess_dict = {
    'ClustId': 'CTI00000000000000001',
    'CodedAssToolType': '396747005',
    'PersScore': 12345,
    'RowNumber': 1
}

#  MHS803CareCluster
care_cluster_dict = {
    'AMHCareClustCodeFin': '01',
    'CAMHNeedsBasedGroupingCode': '10',
    'ClustId': 'CTI00000000000000001',
    'EndDateCareClust': None,
    'EndTimeCareClust': datetime(2018, 5, 28, 16, 18, 42),
    'FLDCareClustCodeFin': 'ghve',
    'FMHCareClustCodeFin': 'hres',
    'LDCareClustCodeFin': 'dkep',
    'RowNumber': 1,
    'StartDateCareClust': date(2018, 5, 4),
    'StartTimeCareClust': datetime(2018, 5, 4, 7, 47, 40)
}

#  MHS804FiveForensicPathways
five_forensic_pathways_dict = {
    'FFPAssDate': date(2018, 5, 4),
    'FFPAssReason': '10',
    'FFPCode': '1',
    'LocalPatientId': 'LPI00000000000000001',
    'RowNumber': 1
}

#  MHS801ClusterTool
cluster_tool_dict = {
    'AMHCareClustCodeInit': '01',
    'AssToolCompDate': date(2018, 5, 4),
    'AssToolCompTime': datetime(2018, 5, 4, 7, 47, 40),
    'CareClusters': [
        care_cluster_dict
    ],
    'ClustCat': '01',
    'ClustId': 'CTI00000000000000001',
    'ClustToolAssReason': '10',
    'ClusterAssesses': [
        cluster_assess_dict
    ],
    'FLDCareClustInit': 'deth',
    'LDCareClustInit': 'gjrn',
    'LocalPatientId': 'LPI00000000000000001',
    'MHCareClusterSuperClass': 'A',
    'RowNumber': 1
}

#  MHS701CPACareEpisode
care_program_approach_care_episode_dict = {
    'CPAEpisodeId': 'CEI00000000000000001',
    'CPAReviews': [
        care_program_approach_review_dict
    ],
    'EndDateCPA': date(2018, 5, 4),
    'LocalPatientId': 'LPI00000000000000001',
    'RowNumber': 1,
    'StartDateCPA': date(2018, 5, 4)
}

#  MHS506Assault
assault_dict = {
    'DateAssault': date(2018, 5, 4),
    'RowNumber': 1,
    'WardStayId': 'WSI00000000000000001'
}

#  MHS509HomeLeave
home_leave_dict = {
    'EndDateHomeLeave': date(2018, 5, 4),
    'EndTimeHomeLeave': datetime(2018, 5, 4, 7, 47, 40),
    'RowNumber': 1,
    'StartDateHomeLeave': date(2018, 5, 4),
    'StartTimeHomeLeave': datetime(2018, 5, 4, 7, 47, 40),
    'WardStayId': 'WSI00000000000000001'
}

#  MHS510LeaveOfAbsence
leave_of_absence_dict = {
    'EndDateMHLeaveAbs': date(2018, 5, 4),
    'EndTimeMHLeaveAbs': datetime(2018, 5, 4, 7, 47, 40),
    'EscortedLeaveIndicator': 'Y',
    'MHLeaveAbsEndReason': '01',
    'RowNumber': 1,
    'StartDateMHLeaveAbs': date(2018, 5, 4),
    'StartTimeMHLeaveAbs': datetime(2018, 5, 4, 7, 47, 40),
    'WardStayId': 'WSI00000000000000001'
}

#  MHS516PoliceAssistanceRequest
police_assistance_request_dict = {
    'PoliceAssistArrDate': date(2018, 5, 4),
    'PoliceAssistArrTime': datetime(2018, 5, 4, 7, 47, 40),
    'PoliceAssistReqDate': date(2018, 5, 4),
    'PoliceAssistReqTime': datetime(2018, 5, 4, 7, 47, 40),
    'PoliceRestraintForceUsedInd': 'Y',
    'RowNumber': 1,
    'WardStayId': 'WSI00000000000000001'
}

#  MHS507SelfHarm
self_harm_dict = {
    'DateSelfHarm': date(2018, 5, 4),
    'RowNumber': 1,
    'WardStayId': 'WSI00000000000000001'
}

#  MHS502WardStay
ward_stay_dict = {
    'AbsenceWithoutLeaves': [
        absence_without_leave_dict
    ],
    'Assaults': [
        assault_dict
    ],
    'EndDateMHTrialLeave': datetime(2018, 5, 28, 7, 47, 40),
    'EndDateWardStay': None,
    'EndTimeWardStay': datetime(2018, 5, 4, 7, 47, 40),
    'HomeLeaves': [
        home_leave_dict
    ],
    'HospProvSpellID': 'HSN00000000000000001',
    'HospitalBedTypeMH': '10',
    'IntendClinCareIntenCodeMH': '51',
    'LeaveOfAbsences': [
        leave_of_absence_dict
    ],
    'LockedWardInd': 'Y',
    'PoliceAssistanceRequests': [
        police_assistance_request_dict
    ],
    'RowNumber': 1,
    'SelfHarms': [
        self_harm_dict
    ],
    'SiteIDOfTreat': 'RMYMJ',
    'SpecialisedMHServiceCode': 'boomTATAboom',
    'StartDateWardStay': date(2018, 5, 4),
    'StartTimeWardStay': datetime(2018, 5, 4, 7, 47, 40),
    'SubstanceMisuses': [
        substance_misuse_dict
    ],
    'TrialLeaves': [
        trial_leave_dict
    ],
    'WardAge': '10',
    'WardCode': 'BOB',
    'WardSecLevel': '0',
    'WardSexTypeCode': '1',
    'WardStayId': 'WSI00000000000000001',
    'WardType': '01'
}

#  MHS503AssignedCareProf
assigned_care_professional_dict = {
    'CareProfLocalId': 'CPI00000000000000001',
    'EndDateAssCareProf': date(2018, 5, 4),
    'HospProvSpellID': 'HPI00000000000000001',
    'RowNumber': 1,
    'StartDateAssCareProf': date(2018, 5, 4),
    'TreatFuncCodeMH': '319'
}

#  MHS515RestrictiveInterventType
restrictive_intervention_type_dict = {
    'EndDateRestrictiveIntType': date(2018, 5, 4),
    'EndTimeRestrictiveIntType': datetime(2018, 5, 4, 7, 47, 40),
    'RestraintInjuryCarePers': 'Y',
    'RestraintInjuryOtherPers': 'Y',
    'RestraintInjuryPatient': 'Y',
    'RestrictiveIntIncID': 'RII00000000000000001',
    'RestrictiveIntType': 'RIT00000000000000001',
    'RestrictiveIntTypeID': 'RITI0000000000000001',
    'RowNumber': 1,
    'StartDateRestrictiveIntType': date(2018, 5, 4),
    'StartTimeRestrictiveIntType': datetime(2018, 5, 4, 7, 47, 40)
}

#  MHS505RestrictiveInterventInc
restrictive_intervention_incident_dict = {
    'EndDateRestrictiveIntInc': date(2018, 5, 4),
    'EndTimeRestrictiveIntInc': datetime(2018, 5, 4, 7, 47, 40),
    'HospProvSpellID': 'HPI00000000000000001',
    'RestrictiveIntIncID': 'RII00000000000000001',
    'RestrictiveIntPIReviewHeldCarePers': 'Y',
    'RestrictiveIntPIReviewHeldPat': 'Y',
    'RestrictiveIntPIReviewNotHeldReasPat': '1',
    'RestrictiveInterventionTypes': [
        restrictive_intervention_type_dict
    ],
    'RestrictiveIntReason': 42,
    'RowNumber': 1,
    'StartDateRestrictiveIntInc': date(2018, 5, 4),
    'StartTimeRestrictiveIntInc': datetime(2018, 5, 4, 7, 47, 40)
}

#  MHS504DelayedDischarge
delayed_discharge_dict = {
    'AttribToIndic': '04',
    'DelayDischReason': 'A2',
    'EndDateDelayDisch': date(2018, 5, 4),
    'HospProvSpellID': 'HPI00000000000000001',
    'OrgIDRespLADelayDisch': 'BOB',
    'RowNumber': 1,
    'StartDateDelayDisch': date(2018, 5, 4)
}

#  MHS501HospProvSpell
hospital_provider_spell_dict = {
    'AssignedCareProfessionals': [
        assigned_care_professional_dict
    ],
    'DecidedToAdmitDate': date(2018, 5, 4),
    'DecidedToAdmitTime': datetime(2018, 5, 4, 7, 47, 40),
    'DelayedDischarges': [
        delayed_discharge_dict
    ],
    'DestOfDischHospProvSpell': '19',
    'DischDateHospProvSpell': None,
    'DischTimeHospProvSpell': datetime(2018, 5, 4, 7, 47, 40),
    'EstimatedDischDateHospProvSpell': date(2018, 5, 4),
    'HospProvSpellID': 'HPI00000000000000001',
    'HospitalProviderSpellCommissionersAssignmentPeriods': [
        hospital_provider_spell_commissioner_assignment_period_dict
    ],
    'MethAdmMHHospProvSpell': '11',
    'MethOfDischMHHospProvSpell': '1',
    'PlannedDestDisch': '19',
    'PlannedDischDateHospProvSpell': date(2018, 5, 4),
    'PostcodeDischDestHospProvSpell': 'LS16 6EH',
    'PostcodeMainVisitor': 'LS16 6EH',
    'RestrictiveInterventionIncidents': [
        restrictive_intervention_incident_dict
    ],
    'RowNumber': 4,
    'ServiceRequestId': 'SRI00000000000000001',
    'SourceAdmMHHospProvSpell': '19',
    'SpecialisedMentalHealthExceptionalPackageOfCares': [
        specialised_mental_health_exceptional_package_of_care_dict
    ],
    'StartDateHospProvSpell': date(2018, 5, 4),
    'StartTimeHospProvSpell': datetime(2018, 5, 4, 7, 47, 40),
    'TransformingCareCategory': '1',
    'TransformingCareInd': 'Y',
    'WardStays': [
        ward_stay_dict
    ]
}

#  MHS405CommTreatOrderRecall
community_treatment_order_recall_dict = {
    'EndDateCommTreatOrdRecall': date(2018, 5, 4),
    'EndTimeCommTreatOrdRecall': datetime(2018, 5, 4, 7, 47, 40),
    'MHActLegalStatusClassPeriodId': 'MHI00000000000000001',
    'RowNumber': 1,
    'StartDateCommTreatOrdRecall': date(2018, 5, 4),
    'StartTimeCommTreatOrdRecall': datetime(2018, 5, 4, 7, 47, 40)
}

#  MHS404CommTreatOrder
community_treatment_order_dict = {
    'CommTreatOrdEndReason': '01',
    'EndDateCommTreatOrd': date(2018, 5, 4),
    'ExpiryDateCommTreatOrd': date(2018, 5, 4),
    'MHActLegalStatusClassPeriodId': 'MHI00000000000000001',
    'RowNumber': 1,
    'StartDateCommTreatOrd': date(2018, 5, 4)
}

#  MHS403ConditionalDischarge
conditional_discharge_dict = {
    'AbsDischResp': '01',
    'CondDischEndReason': '01',
    'EndDateMHCondDisch': date(2018, 5, 4),
    'MHActLegalStatusClassPeriodId': 'MHI00000000000000001',
    'RowNumber': 1,
    'StartDateMHCondDisch': date(2018, 5, 4)
}

#  MHS402RespClinicianAssignPeriod
mental_health_responsible_clinician_assignment_period_dict = {
    'CareProfLocalId': 'CPI00000000000000001',
    'EndDateRespClinAssign': date(2018, 5, 4),
    'MHActLegalStatusClassPeriodId': 'MHI00000000000000001',
    'RowNumber': 1,
    'StartDateRespClinAssign': date(2018, 5, 4)
}

#  MHS401MHActPeriod
mental_health_act_legal_status_classification_assignment_period_dict = {
    'CommunityTreatmentOrderRecalls': [
        community_treatment_order_recall_dict
    ],
    'CommunityTreatmentOrders': [
        community_treatment_order_dict
    ],
    'ConditionalDischarges': [
        conditional_discharge_dict
    ],
    'EndDateMHActLegalStatusClass': None,
    'EndTimeMHActLegalStatusClass': datetime(2018, 5, 4, 7, 47, 40),
    'ExpiryDateMHActLegalStatusClass': date(2018, 5, 4),
    'ExpiryTimeMHActLegalStatusClass': datetime(2018, 5, 4, 7, 47, 40),
    'LegalStatusClassPeriodEndReason': '01',
    'LegalStatusClassPeriodStartReason': '01',
    'LegalStatusCode': '01',
    'LocalPatientId': 'LPI00000000000000001',
    'MHActLegalStatusClassPeriodId': 'LSI00000000000000001',
    'MentalCat': 'A',
    'ResponsibleClinicianAssignmentPeriods': [
        mental_health_responsible_clinician_assignment_period_dict
    ],
    'RowNumber': 1,
    'StartDateMHActLegalStatusClass': date(2018, 5, 4),
    'StartTimeMHActLegalStatusClass': datetime(2018, 5, 4, 7, 47, 40)
}

#  MHS202CareActivity
care_activity_dict = {
    'CareActId': 'CAI00000000000000001',
    'CareContactId': 'CCI00000000000000001',
    'CareProfLocalId': 'CPL00000000000000001',
    'ClinContactDurOfCareAct': 5248,
    'CodeFind': 'ghen',
    'CodeObs': 568489,
    'CodeProcAndProcStatus': 450925000,
    'CodedScoredAssessmentCareActivities': [
        coded_scored_assessment_referral_dict
    ],
    'FindSchemeInUse': '01',
    'ObsValue': '02548',
    'RowNumber': 1,
    'UnitMeasure': 'plums'
}

#  MHS203OtherAttend
other_in_attendance_dict = {
    'CareContactId': 'CPI00000000000000001',
    'OtherPersonInAttend': '01',
    'ReasonPatientNoIMCA': '01',
    'ReasonPatientNoIMHA': '01',
    'RowNumber': 1
}

#  MHS201CareContact
care_contact_dict = {
    'ActLocTypeCode': 'A03',
    'AdminCatCode': '01',
    'AttendOrDNACode': '5',
    'CareActivities': [
        care_activity_dict
    ],
    'CareContCancelDate': date(2018, 5, 4),
    'CareContCancelReas': '01',
    'CareContDate': date(2018, 5, 4),
    'CareContPatientTherMode': '1',
    'CareContSubj': '01',
    'CareContTime': datetime(2018, 5, 4, 7, 47, 40),
    'CareContactId': 'CCI00000000000000001',
    'CareProfTeamLocalId': 'CPI00000000000000001',
    'ClinContDurOfCareCont': 5485,
    'ComPeriMHPartAssessOfferInd': 'Y',
    'ConsMechanismMH': '01',
    'ConsType': '01',
    'EarliestClinAppDate': date(2018, 5, 4),
    'EarliestReasonOfferDate': date(2018, 5, 4),
    'OrgIDComm': 'OB1',
    'OtherAttendances': [
        other_in_attendance_dict
    ],
    'PlaceOfSafetyInd': 'Y',
    'PlannedCareContIndicator': 'Y',
    'ReasonableAdjustmentMade': 'Y',
    'RowNumber': 1,
    'ServiceRequestId': 'SRI00000000000000001',
    'SiteIDOfTreat': 'DOODEDOO',
    'SpecialisedMHServiceCode': 'LALALALA'
}

#  MHS009CarePlanAgreement
care_plan_agreement_dict = {
    'CarePlanContentAgreedBy': '10',
    'CarePlanContentAgreedDate': date(2018, 5, 4),
    'CarePlanContentAgreedTime': datetime(2018, 5, 4, 7, 47, 40),  # time(7, 47, 40),
    'CarePlanID': 'CPI00000000000000001',
    'FamilyCarePlanIndicator': 'Y',
    'NoFamilyCarePlanReason': '01',
    'RowNumber': 1
}

#  MHS008CarePlanType
care_plan_type_dict = {
    'CarePlanAgreements': [
        care_plan_agreement_dict
    ],
    'CarePlanCreatDate': date(2018, 5, 4),
    'CarePlanCreationTime': datetime(2018, 5, 4, 7, 47, 40),
    'CarePlanID': 'CPI00000000000000001',
    'CarePlanImplementDate': date(2018, 5, 4),
    'CarePlanLastUpdateDate': date(2018, 5, 4),
    'CarePlanLastUpdateTime': datetime(2018, 5, 4, 7, 47, 40),
    'CarePlanTypeMH': '10',
    'LocalPatientId': 'LPI00000000000000001',
    'RowNumber': 1
}

#  MHS003AccommStatus
accommodation_status_dict = {
    'AccommodationType': '01',
    'AccommodationTypeDate': date(2018, 5, 4),
    'AccommodationTypeEndDate': date(2018, 5, 4),
    'AccommodationTypeStartDate': date(2018, 5, 4),
    'LocalPatientId': 'LPI00000000000000001',
    'RowNumber': 1,
    'SCHPlacementType': '1',
    'SettledAccommodationInd': 'Y'
}

#  MHS010AssTechToSupportDisTyp
assistive_technology_to_support_disability_type_dict = {
    'AssistiveTechnologyFinding': 227800007,
    'AssistiveTechnologyPrescTimestamp': "1981-10-28T06:06:24+00:00",
    'LocalPatientId': 'LPI00000000000000001',
    'RowNumber': 1
}

#  MHS007DisabilityType
disability_type_dict = {
    'DisabCode': '01',
    'DisabImpacPercep': '01',
    'LocalPatientId': 'LPI00000000000000001',
    'RowNumber': 1
}

#  MHS004EmpStatus
employment_status_dict = {
    'EmployStatus': '01',
    'EmployStatusEndDate': date(2018, 5, 4),
    'EmployStatusRecDate': date(2018, 5, 4),
    'EmployStatusStartDate': date(2018, 5, 4),
    'LocalPatientId': 'LPI00000000000000001',
    'RowNumber': 1,
    'WeekHoursWorked': '01'
}

#  MHS002GP
gp_dict = {
    'EndDateGMPRegistration': date(2018, 5, 4),
    'GMPCodeReg': 'F85678',
    'LocalPatientId': 'LPI00000000000000001',
    'RowNumber': 1,
    'StartDateGMPRegistration': date(2018, 5, 4)
}

#  MHS006MHCareCoord
mental_health_care_coordinator_dict = {
    'CareProfLocalId': 'CPI00000000000000001',
    'CareProfServOrTeamTypeAssoc': 'A01',
    'EndDateAssCareCoord': None,
    'LocalPatientId': 'LPI00000000000000001',
    'RowNumber': 1,
    'StartDateAssCareCoord': date(2018, 5, 4)
}

#  MHS013MHCurrencyModel
mental_health_currency_model_dict = {
    'EndDateMHResourceGroup': date(2018, 5, 4),
    'LocalPatientId': 'LPI00000000000000001',
    'MHResourceGroupType': '19362000',
    'RowNumber': 1,
    'StartDateMHResourceGroup': date(2018, 5, 4)
}

#  MHS012OverseasVisitorChargCat
overseas_visitor_charging_category_dict = {
    'LocalPatientId': 'LPI00000000000000001',
    'OvsVisChCat': 'A',
    'OvsVisChCatAppDate': date(2018, 5, 4),
    'OvsVisChCatEndDate': date(2018, 5, 4),
    'RowNumber': 1
}

#  MHS005PatInd
patient_indicators_dict = {
    'CPP': '1',
    'ConstSuperReqDueToDis': 'Y',
    'EducationalAssessOutcome': '01',
    'EmerPsychDate': date(2018, 5, 4),
    'ExBAFIndicator': '02',
    'LACLegalStatus': '01',
    'LACStatus': '01',
    'LocalPatientId': 'LPI00000000000000001',
    'ManPsychDate': date(2018, 5, 4),
    'OffenceHistory': '1',
    'ParentalResp': 'Y',
    'ProPsychDate': date(2018, 5, 4),
    'PsychPrescDate': date(2018, 5, 4),
    'PsychTreatDate': date(2018, 5, 4),
    'ReasonableAdjustmentInd': 'Y',
    'RowNumber': 1,
    'YoungCarer': 'Y'
}

#  MHS011SocPerCircumstances
social_and_personal_circumstances_dict = {
    'LocalPatientId': 'LPI00000000000000001',
    'RowNumber': 1,
    'SocPerCircumstance': 256749004,
    'SocPerCircumstanceRecTimestamp': "1981-10-28T06:06:24+00:00"
}

#  MHS000Header
header_dict = {
    'DatSetVer': 5,
    'DateTimeDatSetCreate': datetime(2018, 5, 4, 1, 1, 1),
    'OrgIDProvider': 'B0B',
    'OrgIDSubmit': 'B8N',
    'PrimSystemInUse': 'GP5000',
    'ReportingPeriodEndDate': date(2018, 5, 4),
    'ReportingPeriodStartDate': date(2018, 1, 1),
    'RowNumber': 1
}

#  MHS001MPI
master_patient_index_dict = {
    'AccommodationStatuses': [
        accommodation_status_dict
    ],
    'AssistiveTechnologiesToSupportDisabilityTypes': [
        assistive_technology_to_support_disability_type_dict
    ],
    'CPACareEpisodes': [
        care_program_approach_care_episode_dict
    ],
    'CarePlanTypes': [
        care_plan_type_dict
    ],
    'ClusteringToolAssessments': [
        cluster_tool_dict
    ],
    'DisabilityTypes': [
        disability_type_dict
    ],
    'EmploymentStatuses': [
        employment_status_dict
    ],
    'EthnicCategory': 'A',
    'EthnicCategory2021': 'A',
    'FiveForensicPathways': [
        five_forensic_pathways_dict
    ],
    'GPs': [
        gp_dict
    ],
    'Gender': '1',
    'GenderIDCode': '1',
    'GenderSameAtBirth': 'Y',
    'LanguageCodePreferred': 'q1',
    'LocalPatientId': 'LPI00000000000000001',
    'MaritalStatus': 'S',
    'MedicalHistoryPreviousDiagnoses': [
        medical_history_previous_diagnosis_dict
    ],
    'MentalHealthActLegalStatusClassificationAssignmentPeriods': [
        mental_health_act_legal_status_classification_assignment_period_dict
    ],
    'MentalHealthCareCoordinators': [
        mental_health_care_coordinator_dict
    ],
    'MentalHealthCurrencyModels': [
        mental_health_currency_model_dict
    ],
    'NHSNumber': '4587578542',
    'NHSNumberStatus': '01',
    'OrgIDEduEstab': 'B0B',
    'OrgIDLocalPatientId': 'SAL1',
    'OverseasVisitorChargingCategories': [
        overseas_visitor_charging_category_dict
    ],
    'PatientIndicators': [
        patient_indicators_dict
    ],
    'PersDeathDate': date(2018, 5, 4),
    'PersonBirthDate': date(1984, 4, 4),
    'Postcode': 'LS16 6EH',
    'RowNumber': 1,
    'SocialAndPersonalCircumstances': [
        social_and_personal_circumstances_dict
    ]
}

#  MHS106DischargePlanAgreement
discharge_plan_agreement_dict = {
    'DischPlanContentAgreedBy': '10',
    'DischPlanContentAgreedDate': date(2018, 5, 4),
    'DischPlanContentAgreedTime': datetime(2018, 5, 4, 7, 47, 40),
    'RowNumber': 1,
    'ServiceRequestId': 'SRI00000000000000001'
}

#  MHS204IndirectActivity
indirect_activity_dict = {
    'CareProfLocalId': 'CPI00000000000000001',
    'CareProfTeamLocalId': 'CPTI0000000000000001',
    'CodeFind': '35838000',
    'CodeIndActProcAndProcStatus': ' Bigeye Cigarfish',
    'DurationIndirectAct': 5874,
    'FindSchemeInUse': '04',
    'IndirectActDate': date(2018, 5, 4),
    'IndirectActTime': datetime(2018, 5, 4, 7, 47, 40),
    'OrgIDComm': 'B0B',
    'RowNumber': 1,
    'ServiceRequestId': 'SRI00000000000000001'
}

#  MHS107MedicationPrescription
medication_prescription_dict = {
    'PrescriptionDate': date(2018, 5, 4),
    'PrescriptionID': 'GHTRN558',
    'PrescriptionTime': datetime(2018, 5, 4, 7, 47, 40),
    'RowNumber': 6,
    'ServiceRequestId': 'SRI00000000000000001'
}

#  MHS107MedicationPrescription
medication_prescription_dict_2 = {
    'PrescriptionDate': date(2018, 5, 4),
    'PrescriptionID': 'KMMHL558',
    'PrescriptionTime': datetime(2018, 5, 4, 7, 47, 40),
    'RowNumber': 7,
    'ServiceRequestId': 'SRI00000000000000001'
}

#  MHS105OnwardReferral
onward_referral_dict = {
    'CodeRefProcAndProcStatus': '56664008',
    'DecisionToReferDate': date(2018, 5, 4),
    'DecisionToReferTime': datetime(2018, 5, 4, 7, 47, 40),
    'OATReason': '01',
    'OnwardReferDate': date(2018, 5, 4),
    'OnwardReferReason': '01',
    'OnwardReferTime': datetime(2018, 5, 4, 7, 47, 40),
    'OrgIDReceiving': 'BOB',
    'RowNumber': 1,
    'ServiceRequestId': 'SRI00000000000000001'
}

#  MHS103OtherReasonReferral
other_reason_for_referral_dict = {
    'OtherReasonReferMH': '01',
    'RowNumber': 1,
    'ServiceRequestId': 'SRI00000000000000001'
}

#  MHS104RTT
referral_to_treatment_dict = {
    'OrgIDPatPathIdIssuer': 'HAM',
    'PatPathId': 'FHFREB595',
    'ReferToTreatPeriodEndDate': date(2018, 5, 4),
    'ReferToTreatPeriodStartDate': date(2018, 5, 4),
    'ReferToTreatPeriodStatus': '01',
    'RowNumber': 1,
    'ServiceRequestId': 'SRI00000000000000001',
    'WaitTimeMeasureType': '02'
}

#  MHS102ServiceTypeReferredTo
service_type_referred_to_dict = {
    'CareProfTeamLocalId': 'CPI00000000000000001',
    'ReferClosReason': '01',
    'ReferClosureDate': None,
    'ReferClosureTime': datetime(2018, 5, 4, 7, 47, 40),
    'ReferRejectReason': '01',
    'ReferRejectionDate': date(2018, 5, 4),
    'ReferRejectionTime': datetime(2018, 5, 4, 7, 47, 40),
    'RowNumber': 1,
    'ServTeamTypeRefToMH': 'A03',
    'ServiceRequestId': 'SRI00000000000000001'
}
service_type_referred_to_dict_2 = {
    'ReferClosureDate': date(2018, 5, 4),
}

#  MHS101Referral
referral_dict = {
    'CareContacts': [
        care_contact_dict
    ],
    'ClinRespPriorityType': '1',
    'CodedScoredAssessmentReferrals': [
        coded_scored_assessment_referral_dict
    ],
    'DecisionToTreatDate': date(2018, 5, 4),
    'DecisionToTreatTime': datetime(2018, 5, 4, 7, 47, 40),
    'DischPlanCreationDate': date(2018, 5, 4),
    'DischPlanCreationTime': datetime(2018, 5, 4, 7, 47, 40),
    'DischPlanLastUpdatedDate': date(2018, 5, 4),
    'DischPlanLastUpdatedTime': datetime(2018, 5, 4, 7, 47, 40),
    'DischargePlanAgreements': [
        discharge_plan_agreement_dict
    ],
    'Header': header_dict,
    'HospitalProviderSpells': [
        hospital_provider_spell_dict
    ],
    'IndirectActivities': [
        indirect_activity_dict
    ],
    'LocalPatientId': 'LPI00000000000000001',
    'META': meta_dict,
    'MedicationPrescriptions': [
        medication_prescription_dict, medication_prescription_dict_2
    ],
    'NHSServAgreeLineNum': '5895475589',
    'OnwardReferrals': [
        onward_referral_dict
    ],
    'OrgIDComm': 'B0B',
    'OrgIDReferring': 'ZELDA',
    'OtherReasonsForReferral': [
        other_reason_for_referral_dict
    ],
    'Patient': master_patient_index_dict,
    'PrimReasonReferralMH': '01',
    'PrimaryDiagnoses': [
        primary_diagnosis_dict
    ],
    'ProvisionalDiagnoses': [
        provisional_diagnosis_dict
    ],
    'ReasonOAT': '10',
    'ReferralRequestReceivedDate': date(2018, 5, 4),
    'ReferralRequestReceivedTime': datetime(2018, 5, 4, 7, 47, 40),
    'ReferralsToTreatment': [
        referral_to_treatment_dict
    ],
    'ReferringCareProfessionalStaffGroup': 'A01',
    'RowNumber': 1,
    'SecondaryDiagnoses': [
        secondary_diagnosis_dict
    ],
    'ServDischDate': None,
    'ServDischTime': datetime(2018, 5, 4, 7, 47, 40),
    'ServiceRequestId': 'SRI00000000000000001',
    'ServiceTypesReferredTo': [
        service_type_referred_to_dict,
        service_type_referred_to_dict_2
    ],
    'SourceOfReferralMH': 'A1',
    'SpecialisedMHServiceCode': 'DJFSEJSJ45782'
}
referral_dict_2 = {
    'Header': header_dict,
    'META': meta_dict,
    'ServDischDate': datetime(2018, 5, 4),
    'Patient': master_patient_index_dict
}

#  MHS301GroupSession
group_session_dict = {
    'ActLocTypeCode': 'A01',
    'CareProfLocalId': 'CPI00000000000000001',
    'ClinContDurOfGroupSess': 5874,
    'GroupSessDate': date(2018, 5, 4),
    'GroupSessId': 'SJKFDJRE74C',
    'GroupSessType': '01',
    'Header': header_dict,
    'META': meta_dict,
    'NHSServAgreeLineNum': '545168',
    'NumberOfGroupSessParticip': 100,
    'OrgIDComm': 'DAISY',
    'RowNumber': 1,
    'ServTeamTypeRefToMH': 'A01',
    'SiteIDOfTreat': '13TAE'
}

#  MHS302MHDropInContact
mental_health_drop_in_contact_dict = {
    'CareContactDateMHDropInContact': date(2018, 5, 4),
    'CareProfLocalId': 'CPI00000000000000001',
    'ConsMechanismMH': '01',
    'EndTimeDropInContact': datetime(2018, 5, 4, 7, 47, 40),
    'EthnicCategory': 'A',
    'EthnicCategory2021': 'A',
    'GenderIDCode': '1',
    'GenderSameAtBirth': 'Y',
    'Header': header_dict,
    'LocalPatientId': 'LPI00000000000000001',
    'META': meta_dict,
    'MHDropInContactId': 'DCI00000000000000001',
    'MHDropInContactOutcome': '01',
    'MHDropInContactServiceType': 'A17',
    'NHSNumber': 4587455895,
    'OrgIDReceiving': 'SALLY',
    'PersonBirthDate': date(1984, 5, 1),
    'RowNumber': 1,
    'StartTimeDropInContact': datetime(2018, 5, 4, 7, 47, 40)
}

#  MHS608AnonSelfAssess
anonymous_self_assessment_dict = {
    'ActLocTypeCode': 'A01',
    'AssToolCompTimestamp': "1981-10-28T06:06:24+00:00",
    'CodedAssToolType': '4739002',
    'Header': header_dict,
    'META': meta_dict,
    'OrgIDComm': 'B0B',
    'PersScore': '45P',
    'RowNumber': 1
}

#  MHS901StaffDetails
staff_details_dict = {
    'CareProfJobRoleCode': 'FD45',
    'CareProfLocalId': 'CPI00000000000000001',
    'CareProfStaffGpMH': '01',
    'Header': header_dict,
    'META': {
        'DATASET_VERSION': 5,
        'EVENT_ID': '1066:5',
        'EVENT_RECEIVED_TS': datetime(2021, 1, 1),
        'RECORD_INDEX': 3,
        'RECORD_VERSION': current_record_version(DS.MHSDS_V5)
    },
    'MainSpecCodeMH': '600',
    'OccCode': 'GH5',
    'ProfRegBodyCode': '01',
    'ProfRegEntryId': 'SHEK58EJNT4',
    'RowNumber': 8
}


@pytest.mark.parametrize(['values', 'model'], [
    (meta_dict, META),
    (absence_without_leave_dict, AbsenceWithoutLeave),
    (hospital_provider_spell_commissioner_assignment_period_dict,
     HospitalProviderSpellCommissionerAssignmentPeriod),
    (substance_misuse_dict, SubstanceMisuse),
    (trial_leave_dict, TrialLeave),
    (specialised_mental_health_exceptional_package_of_care_dict,
     SpecialisedMentalHealthExceptionalPackageOfCare),
    (medical_history_previous_diagnosis_dict, MedicalHistoryPreviousDiagnosis),
    (provisional_diagnosis_dict, ProvisionalDiagnosis),
    (primary_diagnosis_dict, PrimaryDiagnosis),
    (secondary_diagnosis_dict, SecondaryDiagnosis),
    (coded_scored_assessment_referral_dict, CodedScoredAssessmentReferral),
    (coded_scored_assessment_care_activity_dict, CodedScoredAssessmentCareActivity),
    (care_program_approach_review_dict, CareProgramApproachReview),
    (cluster_assess_dict, ClusterAssess),
    (care_cluster_dict, CareCluster),
    (five_forensic_pathways_dict, FiveForensicPathways),
    (cluster_tool_dict, ClusterTool),
    (care_program_approach_care_episode_dict, CareProgramApproachCareEpisode),
    (ward_stay_dict, WardStay),
    (hospital_provider_spell_dict, HospitalProviderSpell),
    (mental_health_act_legal_status_classification_assignment_period_dict,
     MentalHealthActLegalStatusClassificationAssignmentPeriod),
    (care_activity_dict, CareActivity),
    (care_contact_dict, CareContact),
    (care_plan_type_dict, CarePlanType),
    (master_patient_index_dict, MasterPatientIndex),
    (referral_dict, Referral),
    (group_session_dict, GroupSession),
    (mental_health_drop_in_contact_dict, MentalHealthDropInContact),
    (anonymous_self_assessment_dict, AnonymousSelfAssessment),
    (staff_details_dict, StaffDetails),
    (accommodation_status_dict, AccommodationStatus),
    (assault_dict, Assault),
    (assigned_care_professional_dict, AssignedCareProfessional),
    (assistive_technology_to_support_disability_type_dict,
     AssistiveTechnologyToSupportDisabilityType),
    (care_plan_agreement_dict, CarePlanAgreement),
    (community_treatment_order_dict, CommunityTreatmentOrder),
    (community_treatment_order_recall_dict, CommunityTreatmentOrderRecall),
    (conditional_discharge_dict, ConditionalDischarge),
    (delayed_discharge_dict, DelayedDischarge),
    (disability_type_dict, DisabilityType),
    (discharge_plan_agreement_dict, DischargePlanAgreement),
    (employment_status_dict, EmploymentStatus),
    (gp_dict, GP),
    (header_dict, Header),
    (home_leave_dict, HomeLeave),
    (indirect_activity_dict, IndirectActivity),
    (leave_of_absence_dict, LeaveOfAbsence),
    (medication_prescription_dict, MedicationPrescription),
    (mental_health_care_coordinator_dict, MentalHealthCareCoordinator),
    (mental_health_currency_model_dict, MentalHealthCurrencyModel),
    (mental_health_responsible_clinician_assignment_period_dict,
     MentalHealthResponsibleClinicianAssignmentPeriod),
    (onward_referral_dict, OnwardReferral),
    (other_in_attendance_dict, OtherInAttendance),
    (other_reason_for_referral_dict, OtherReasonForReferral),
    (overseas_visitor_charging_category_dict, OverseasVisitorChargingCategory),
    (patient_indicators_dict, PatientIndicators),
    (police_assistance_request_dict, PoliceAssistanceRequest),
    (referral_to_treatment_dict, ReferralToTreatment),
    (restrictive_intervention_incident_dict, RestrictiveInterventionIncident),
    (restrictive_intervention_type_dict, RestrictiveInterventionType),
    (self_harm_dict, SelfHarm),
    (service_type_referred_to_dict, ServiceTypeReferredTo),
    (social_and_personal_circumstances_dict, SocialAndPersonalCircumstances)
])
def test_contains_all_submitted_values(values: Dict[str, Any], model: Type[DSPStructuredModel]):
    given_fields = values.keys()
    required_fields = model.get_field_names(DSPStructuredModel.submitted_fields_only)
    assert set(given_fields) == set(required_fields), "Required {} but got {}".format(
        required_fields, given_fields)
