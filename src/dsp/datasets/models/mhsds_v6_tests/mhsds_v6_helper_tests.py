import copy
from datetime import datetime, date
from typing import Dict, Any, Type
from decimal import Decimal

import pytest

from dsp.dam import current_record_version
from dsp.datasets.models.mhsds_v6 import (
AbsenceWithoutLeave,
AccommodationStatus,
AnonymousSelfAssessment,
Assault,
AssignedCareProfessional,
AssistiveTechnologyToSupportDisabilityType,
CareActivity,
CareContact,
CarePlanAgreement,
CarePlanType,
CareProgramApproachCareEpisode,
CareProgramApproachReview,
CodedScoredAssessmentCareActivity,
CodedScoredAssessmentReferral,
CommunityTreatmentOrder,
CommunityTreatmentOrderRecall,
ConditionalDischarge,
DisabilityType,
DischargePlanAgreement,
EmploymentStatus,
GroupSession,
Header,
HomeLeave,
HospitalProviderSpell,
HospitalProviderSpellCommissionerAssignmentPeriod,
IndirectActivity,
LeaveOfAbsence,
MasterPatientIndex,
MedicalHistoryPreviousDiagnosis,
MentalHealthActLegalStatusClassificationAssignmentPeriod,
MentalHealthCareCoordinator,
MentalHealthDropInContact,
MentalHealthResponsibleClinicianAssignmentPeriod,
OnwardReferral,
OtherInAttendance,
OtherReasonForReferral,
OverseasVisitorChargingCategory,
PatientIndicators,
PoliceAssistanceRequest,
PrimaryDiagnosis,
Referral,
ReferralToTreatment,
RestrictiveInterventionIncident,
RestrictiveInterventionType,
SecondaryDiagnosis,
SelfHarm,
SocialAndPersonalCircumstances,
SpecialisedMentalHealthExceptionalPackageOfCare,
StaffDetails,
SubstanceMisuse,
TrialLeave,
WardStay,
get_all_models
)
from dsp.common.structured_model import DSPStructuredModel, META
from dsp.shared.constants import DS

meta_dict = {
    'DATASET_VERSION': 6,
    'EVENT_ID': '1234:5',
    'EVENT_RECEIVED_TS': datetime(2021,1,1),
    'RECORD_INDEX': 0,
    'RECORD_VERSION': current_record_version(DS.MHSDS_V6)
}

#  MHS511AbsenceWithoutLeave
absence_without_leave_dict = {
    'EndDateMHAbsWOLeave': date(2018, 5, 24),
    'EndTimeMHAbsWOLeave': datetime(2018, 5, 24, 5, 30,10),
    'MHAbsWOLeaveEndReason': '01',
    'RowNumber': 1,
    'StartDateMHAbsWOLeave': date(2018,5,24),
    'StartTimeMHAbsWOLeave': datetime(2018, 5, 24, 2, 30,10),
    'WardStayID': 'WSI00000000000000001',
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
    'WardStayID': 'HSN00000000000000001'
}

#  MHS514TrialLeave
trial_leave_dict = {
    'EndDateMHTrialLeave': date(2018, 5, 24),
    'EndTimeMHTrialLeave': datetime(2018, 5, 24, 15, 57, 00),
    'RowNumber': 1,
    'StartDateMHTrialLeave': date(2018, 5, 24),
    'StartTimeMHTrialLeave': datetime(2018, 5, 24, 5, 3, 57),
    'WardStayID': 'WSI00000000000000001'
}

#  MHS517SMHExceptionalPackOfCare
specialised_mental_health_exceptional_package_of_care_dict = {
    'EndDateSMHEPC': date(2018, 5, 25),
    'HospProvSpellID': 'HSN00000000000000001',
    'OrgIDComm': '08Q',
    'RowNumber': 1,
    'SMHEPCCharge': Decimal(222.00),
    'StartDateSMHEPC': date(2018, 5, 25)
}

#  MHS601MedHistPrevDiag
medical_history_previous_diagnosis_dict = {
    'CodedDiagTimestamp': datetime(1986,7,9,8,22,37),
    'DiagSchemeInUse': '06',
    'LocalPatientID': 'LPI00000000000000001',
    'PrevDiag': '277891000',
    'RowNumber': 1
}

#  MHS604PrimDiag
primary_diagnosis_dict = {
    'CodedDiagTimestamp': datetime(1981,10,28,6,6,24),
    'DiagSchemeInUse': '06',
    'PrimDiag': '45411003',
    'RowNumber': 1,
    'ServiceRequestID': 'SRI00000000000000001'
}

#  MHS605SecDiag
secondary_diagnosis_dict = {
    'CodedDiagTimestamp': datetime(1981,10,28,6,6,24),
    'DiagSchemeInUse': '06',
    'RowNumber': 1,
    'SecDiag': '422929003',
    'ServiceRequestID': 'SRI00000000000000001'
}

#  MHS606CodedScoreAssessmentRefer
coded_scored_assessment_referral_dict = {
    'AssToolCompTimestamp': datetime(1981,10,28,6,6,24),
    'CareProfLocalID': 'CPL00000000000000001',
    'CodedAssToolType': '218023008',
    'PersScore': '12345',
    'RowNumber': 1,
    'ServiceRequestID': 'SRI00000000000000001'
}

#  MHS607CodedScoreAssessmentAct
coded_scored_assessment_care_activity_dict = {
    'CareActID': 'CAI00000000000000001',
    'CodedAssToolType': '242129005',
    'PersScore': '1010',
    'RowNumber': 1
}

#  MHS702CPAReview
care_program_approach_review_dict = {
    'CPAEpisodeID': 'CEI00000000000000001',
    'CPAReviewDate': date(2018, 5, 20),
    'CareProfLocalID': 'CPL00000000000000001',
    'RowNumber': 1
}

#  MHS701CPACareEpisode
care_program_approach_care_episode_dict = {
    'CPAEpisodeID': 'CEI00000000000000001',
    'CPAReviews': [
        care_program_approach_review_dict
    ],
    'EndDateCPA': date(2018, 5, 4),
    'LocalPatientID': 'LPI00000000000000001',
    'RowNumber': 1,
    'StartDateCPA': date(2018, 5, 4)
}

#  MHS506Assault
assault_dict = {
    'DateAssault': date(2018, 5, 4),
    'RowNumber': 1,
    'WardStayID': 'WSI00000000000000001'
}

#  MHS509HomeLeave
home_leave_dict = {
    'EndDateHomeLeave': date(2018, 5, 4),
    'EndTimeHomeLeave': datetime(2018, 5, 4, 7, 47, 40),
    'RowNumber': 1,
    'StartDateHomeLeave': date(2018, 5, 4),
    'StartTimeHomeLeave': datetime(2018, 5, 4, 7, 47, 40),
    'WardStayID': 'WSI00000000000000001'
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
    'WardStayID': 'WSI00000000000000001'
}

#  MHS516PoliceAssistanceRequest
police_assistance_request_dict = {
    'PoliceAssistArrDate': date(2018, 5, 4),
    'PoliceAssistArrTime': datetime(2018, 5, 4, 7, 47, 40),
    'PoliceAssistReqDate': date(2018, 5, 4),
    'PoliceAssistReqTime': datetime(2018, 5, 4, 7, 47, 40),
    'PoliceRestraintForceUsedInd': 'Y',
    'RowNumber': 1,
    'WardStayID': 'WSI00000000000000001'
}

#  MHS507SelfHarm
self_harm_dict = {
    'DateSelfHarm': date(2018, 5, 4),
    'RowNumber': 1,
    'WardStayID': 'WSI00000000000000001'
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
    'EndDateWardStay': date(2018, 5, 4),
    'EndTimeWardStay': datetime(2018, 5, 4, 7, 47, 40),
    'HomeLeaves': [
        home_leave_dict
    ],
    'HospProvSpellID': 'HSN00000000000000001',
    'LeaveOfAbsences': [
        leave_of_absence_dict
    ],
    'PoliceAssistanceRequests': [
        police_assistance_request_dict
    ],
    'RowNumber': 1,
    'SelfHarms': [
        self_harm_dict
    ],
    'SpecialisedMHServiceCode': 'boomTATAboom',
    'StartDateWardStay': date(2018, 5, 4),
    'StartTimeWardStay': datetime(2018, 5, 4, 7, 47, 40),
    'SubstanceMisuses': [
        substance_misuse_dict
    ],
    'TrialLeaves': [
        trial_leave_dict
    ],
    'WardCode': 'BOB',
    'WardStayID': 'WSI00000000000000001',
    'MHAdmittedPatientClass' : '203'
}

#  MHS503AssignedCareProf
assigned_care_professional_dict = {
    'CareProfLocalID': 'CPI00000000000000001',
    'EndDateAssCareProf': date(2018, 5, 4),
    'HospProvSpellID': 'HPI00000000000000001',
    'RowNumber': 1,
    'StartDateAssCareProf': date(2018, 5, 4),
    'TreatFuncCodeMH': '319'
}


#  MHS515RestrictiveInterventType
restrictive_intervention_type_dict = {
    "EndDateRestrictiveIntType": date(2018, 5, 4),
    "EndTimeRestrictiveIntType": datetime(2018, 5, 4, 7, 47, 40),
    "RestraintInjuryCarePers": "N",
    "RestraintInjuryOtherPers": "N",
    "RestraintInjuryPatient": "N",
    "RestrictiveIntIncID": "RII00000000000000001",
    "RestrictiveIntType": "01",
    "RestrictiveIntTypeID": "RIT00000000000000001",
    "RowNumber": 1,
    "StartDateRestrictiveIntType": date(2018, 5, 4),
    "StartTimeRestrictiveIntType": datetime(2018, 5, 4, 7, 47, 40),
    "UniqHospProvSpellID": "RA9HSN000000001",
    "UniqServReqID": "RA9SRI00000000000000001",
    "UniqWardStayID": "[RA9WSI00000000000000001]"
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
    'RestrictiveInterventionTypes': [restrictive_intervention_type_dict],
    'RestrictiveIntReason': 42,
    'RowNumber': 1,
    'StartDateRestrictiveIntInc': date(2018, 5, 4),
    'StartTimeRestrictiveIntInc': datetime(2018, 5, 4, 7, 47, 40)
}


#  MHS405CommTreatOrderRecall
community_treatment_order_recall_dict = {
    'EndDateCommTreatOrdRecall': date(2018, 5, 4),
    'EndTimeCommTreatOrdRecall': datetime(2018, 5, 4, 7, 47, 40),
    'MHActLegalStatusClassPeriodID': 'MHI00000000000000001',
    'RowNumber': 1,
    'StartDateCommTreatOrdRecall': date(2018, 5, 4),
    'StartTimeCommTreatOrdRecall': datetime(2018, 5, 4, 7, 47, 40)
}

#  MHS404CommTreatOrder
community_treatment_order_dict = {
    'CommTreatOrdEndReason': '01',
    'EndDateCommTreatOrd': date(2018, 5, 4),
    'ExpiryDateCommTreatOrd': date(2018, 5, 4),
    'MHActLegalStatusClassPeriodID': 'MHI00000000000000001',
    'RowNumber': 1,
    'StartDateCommTreatOrd': date(2018, 5, 4)
}

#  MHS403ConditionalDischarge
conditional_discharge_dict = {
    'AbsDischResp': '01',
    'CondDischEndReason': '01',
    'EndDateMHCondDisch': date(2018, 5, 4),
    'MHActLegalStatusClassPeriodID': 'MHI00000000000000001',
    'RowNumber': 1,
    'StartDateMHCondDisch': date(2018, 5, 4)
}

#  MHS402RespClinicianAssignPeriod
mental_health_responsible_clinician_assignment_period_dict = {
    'CareProfLocalID': 'CPI00000000000000001',
    'EndDateRespClinAssign': date(2018, 5, 4),
    'MHActLegalStatusClassPeriodID': 'MHI00000000000000001',
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
    'EndDateMHActLegalStatusClass': date(2018, 5, 4),
    'EndTimeMHActLegalStatusClass': datetime(2018, 5, 4, 7, 47, 40),
    'ExpiryDateMHActLegalStatusClass': date(2018, 5, 4),
    'ExpiryTimeMHActLegalStatusClass': datetime(2018, 5, 4, 7, 47, 40),
    'LegalStatusClassPeriodEndReason': '01',
    'LegalStatusClassPeriodStartReason': '01',
    'LegalStatusCode': '01',
    'LocalPatientID': 'LPI00000000000000001',
    'MHActLegalStatusClassPeriodID': 'LSI00000000000000001',
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
    'CareActID': 'CAI00000000000000001',
    'CareContactID': 'CCI00000000000000001',
    'ClinContactDurOfCareAct': 5248,
    'CodedScoredAssessmentCareActivities': [
        coded_scored_assessment_referral_dict
    ],
    'FindSchemeInUse': '01',
    'ObsValue': '02548',
    'RowNumber': 1,
    'Procedure': '',
    'Finding': '',
    'StaffActivities': '',
    'UnitofMeasurement_UCUM': '',
    'Observation': ''
}

#  MHS203OtherAttend
other_in_attendance_dict = {
    'CareContactID': 'CPI00000000000000001',
    'OtherPersonInAttend': '01',
    'RowNumber': 1
}

#  MHS201CareContact
care_contact_dict = {
    'ActLocTypeCode': 'A03',
    'AdminCatCode': '01',
    'CareActivities': [
        care_activity_dict
    ],
    'CareContCancelDate': date(2018, 5, 4),
    'CareContCancelReas': '01',
    'CareContDate': date(2018, 5, 4),
    'CareContPatientTherMode': '1',
    'CareContSubj': '01',
    'CareContTime': datetime(2018, 5, 4, 7, 47, 40),
    'CareContactID': 'CCI00000000000000001',
    'ClinContDurOfCareCont': 5485,
    'ComPeriMHPartAssessOfferInd': 'Y',
    'ConsMechanismMH': '01',
    'ConsType': '01',
    'EarliestClinAppDate': date(2018, 5, 4),
    'EarliestReasonOfferDate': date(2018, 5, 4),
    'OrgIDComm': 'OB1',
    'OtherAttendances': [
        other_in_attendance_dict
    ] ,
    'PlaceOfSafetyInd': 'Y',
    'PlannedCareContIndicator': 'Y',
    'ReasonableAdjustmentMade': 'Y',
    'RowNumber': 1,
    'ServiceRequestID': 'SRI00000000000000001',
    'SiteIDOfTreat': 'DOODEDOO',
    'SpecialisedMHServiceCode': 'LALALALA',
    'LanguageCodeTreat': '',
    'ReasonPatientNoIMCA': '',
    'InterpreterPresentInd': '',
    'OtherCareProfTeamLocalID': '',
    'AttendStatus': '',
    'ReasonPatientNoIMHA': ''
}

#  MHS009CarePlanAgreement
care_plan_agreement_dict = {
    'CarePlanContentAgreedBy': '10',
    'CarePlanContentAgreedDate':  date(2018, 5, 4),
    'CarePlanContentAgreedTime': datetime(2018, 5, 4, 7, 47, 40),#time(7, 47, 40),
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
    'LocalPatientID': 'LPI00000000000000001',
    'RowNumber': 1
}

#  MHS003AccommStatus
accommodation_status_dict = {
    'AccommodationType': '01',
    'AccommodationTypeDate': date(2018, 5, 4),
    'AccommodationTypeEndDate': date(2018, 5, 4),
    'AccommodationTypeStartDate': date(2018, 5, 4),
    'LocalPatientID': 'LPI00000000000000001',
    'RowNumber': 1,
    'SCHPlacementType': '1',
    'SettledAccommodationInd': 'Y'
}

#  MHS010AssTechToSupportDisTyp
assistive_technology_to_support_disability_type_dict = {
    'AssistiveTechnologyFinding': 227800007,
    'AssistiveTechnologyPrescTimestamp': datetime(1981,10,28,6,6,24),
    'LocalPatientID': 'LPI00000000000000001',
    'RowNumber': 1
}

#  MHS007DisabilityType
disability_type_dict = {
    'DisabCode': '01',
    'DisabImpacPercep': '01',
    'LocalPatientID': 'LPI00000000000000001',
    'RowNumber': 1
}

#  MHS004EmpStatus
employment_status_dict = {
    'EmployStatus': '01',
    'EmployStatusEndDate': date(2018, 5, 4),
    'EmployStatusRecDate': date(2018, 5, 4),
    'EmployStatusStartDate': date(2018, 5, 4),
    'LocalPatientID': 'LPI00000000000000001',
    'RowNumber': 1,
    'WeekHoursWorked': '01',
    'PatPrimEmpContTypeMH': ''
}

#  MHS002GP
gp_dict = {
    'EndDateGMPRegistration': date(2018, 5, 4),
    'GMPCodeReg': 'B8THRE',
    'LocalPatientID': 'LPI00000000000000001',
    'RowNumber': 1,
    'StartDateGMPRegistration': date(2018, 5, 4)
}

#  MHS006MHCareCoord
mental_health_care_coordinator_dict = {
    'CareProfLocalID': 'CPI00000000000000001',
    'CareProfServOrTeamTypeAssoc': 'A01',
    'EndDateAssCareCoord': date(2018, 5, 4),
    'LocalPatientID': 'LPI00000000000000001',
    'RowNumber': 1,
    'StartDateAssCareCoord': date(2018, 5, 4)
}

#  MHS013MHCurrencyModel
mental_health_currency_model_dict = {
    'EndDateMHResourceGroup': date(2018, 5, 4),
    'LocalPatientID': 'LPI00000000000000001',
    'MHResourceGroupType': '19362000',
    'RowNumber': 1,
    'StartDateMHResourceGroup': date(2018, 5, 4)
}

#  MHS012OverseasVisitorChargCat
overseas_visitor_charging_category_dict = {
    'LocalPatientID': 'LPI00000000000000001',
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
    'LocalPatientID': 'LPI00000000000000001',
    'ManPsychDate': date(2018, 5, 4),
    'OffenceHistory': '1',
    'ParentalResp': 'Y',
    'ProPsychDate': date(2018, 5, 4),
    'PsychPrescDate': date(2018, 5, 4),
    'PsychTreatDate': date(2018, 5, 4),
    'ReasonableAdjustmentInd': 'Y',
    'RowNumber': 1,
    'YoungCarer': 'Y',
    'IMCARequired':'',
    'IMHARequired':'',
    'AutismStatus':'',
    'IMCAAssigned':'',
    'IMHAAssigned':'',
    'LDStatus':''
}

#  MHS011SocPerCircumstances
social_and_personal_circumstances_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'RowNumber': 1,
    'SocPerCircumstance': 256749004,
    'SocPerCircumstanceRecTimestamp': datetime(1981,10,28,6,6,24)
}

#  MHS000Header
header_dict = {
    'DatSetVer': 5,
    'DateTimeDatSetCreate': datetime(2018, 5, 4, 1, 1, 1),
    'OrgIDProvider': 'B0B',
    'OrgIDSubmit': 'B8N',
    'PrimSystemInUse': 'GP5000',
    'ReportingPeriodEndDate': date(2018, 5, 1),
    'ReportingPeriodStartDate': date(2018, 5, 1),
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
    'DisabilityTypes': [
        disability_type_dict
    ],
    'EmploymentStatuses': [
        employment_status_dict
    ],
    'EthnicCategory': 'A',
    'EthnicCategory2021': 'A',
    'Gender': '1',
    'GenderIDCode': '1',
    'GenderSameAtBirth': 'Y',
    'LanguageCodePreferred': 'q1',
    'LocalPatientID': 'LPI00000000000000001',
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
    'NHSNumber': 4587578542,
    'NHSNumberStatus': '01',
    'OrgIDEduEstab': 'B0B',
    'OrgIDLocalPatientID': 'SAL1',
    'OverseasVisitorChargingCategories': [
        overseas_visitor_charging_category_dict
    ],
    'PatientIndicators': [
        patient_indicators_dict
    ],
    'PersDeathDate': date(2018, 5, 4),
    'PersonBirthDate': date(2018, 5, 4),
    'Postcode': 'LS16 6EH',
    'RowNumber': 1,
    'SocialAndPersonalCircumstances': [
        social_and_personal_circumstances_dict
    ],
    'GPPracticeRegistrations': '',
    'eMED3FitNotes': ''
}

#  MHS106DischargePlanAgreement
discharge_plan_agreement_dict = {
    'DischPlanContentAgreedBy': '10',
    'DischPlanContentAgreedDate': date(2018, 5, 4),
    'DischPlanContentAgreedTime': datetime(2018, 5, 4, 7, 47, 40),
    'RowNumber': 1,
    'ServiceRequestID': 'SRI00000000000000001'
}

#  MHS204IndirectActivity
indirect_activity_dict = {
    'CareProfLocalID': 'CPI00000000000000001',
    'DurationIndirectAct': 5874,
    'FindSchemeInUse': '04',
    'IndirectActDate': date(2018, 5, 4),
    'IndirectActTime': datetime(2018, 5, 4, 7, 47, 40),
    'OrgIDComm': 'B0B',
    'RowNumber': 1,
    'ServiceRequestID': 'SRI00000000000000001',
    'Finding':'',
    'IndActProcedure':'',
    'OtherCareProfTeamLocalID':'',
    'IndActPersCons':''
}

#  MHS105OnwardReferral
onward_referral_dict = {
    'DecisionToReferDate': date(2018, 5, 4),
    'DecisionToReferTime': datetime(2018, 5, 4, 7, 47, 40),
    'OATReason': '01',
    'OnwardReferDate': date(2018, 5, 4),
    'OnwardReferReason': '01',
    'OnwardReferTime': datetime(2018, 5, 4, 7, 47, 40),
    'OrgIDReceiving': 'BOB',
    'RowNumber': 1,
    'ServiceRequestID': 'SRI00000000000000001',
    'ReferralProc': ''
}

#  MHS103OtherReasonReferral
other_reason_for_referral_dict = {
    'OtherReasonReferMH': '01',
    'RowNumber': 1,
    'ServiceRequestID': 'SRI00000000000000001'
}

#  MHS104RTT
referral_to_treatment_dict = {
    'OrgIDPatPathIDIssuer': 'HAM',
    'PatPathID': 'FHFREB595',
    'ReferToTreatPeriodEndDate': date(2018, 5, 4),
    'ReferToTreatPeriodStartDate': date(2018, 5, 4),
    'ReferToTreatPeriodStatus': '01',
    'RowNumber': 1,
    'ServiceRequestID': 'SRI00000000000000001',
    'WaitTimeMeasureType': '02'
}

#  MHS102ServiceTypeReferredTo
service_type_referred_to_dict = {
    'CareProfTeamLocalID': 'CPI00000000000000001',
    'ReferClosReason': '01',
    'ReferClosureDate': date(2018, 5, 4),
    'ReferClosureTime': datetime(2018, 5, 4, 7, 47, 40),
    'ReferRejectReason': '01',
    'ReferRejectionDate': date(2018, 5, 4),
    'ReferRejectionTime': datetime(2018, 5, 4, 7, 47, 40),
    'RowNumber': 1,
    'ServTeamTypeRefToMH': 'A03',
    'ServiceRequestID': 'SRI00000000000000001'
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
    'IndirectActivities': [
        indirect_activity_dict
    ],
    'LocalPatientID': 'LPI00000000000000001',
    'META': meta_dict,
    'OnwardReferrals': [
        onward_referral_dict
    ],
    'OrgIDComm': 'B0B',
    'OtherReasonsForReferral': [
        other_reason_for_referral_dict
    ],
    'Patient': master_patient_index_dict,
    'PrimReasonReferralMH': '01',
    'PrimaryDiagnoses': [
        primary_diagnosis_dict
    ],
    'ReasonOAT': '10',
    'ReferralRequestReceivedDate': date(2018, 5, 4),
    'ReferralRequestReceivedTime': datetime(2018, 5, 4, 7, 47, 40),
    'ReferralsToTreatment': [
        referral_to_treatment_dict
    ],
    'RowNumber': 1,
    'SecondaryDiagnoses': [
        secondary_diagnosis_dict
    ],
    'ServDischDate': date(2018, 5, 4),
    'ServDischTime': datetime(2018, 5, 4, 7, 47, 40),
    'ServiceRequestID': 'SRI00000000000000001',
    'SourceOfReferralMH': 'A1',
    'SpecialisedMHServiceCode': 'DJFSEJSJ45782',
    'OtherServiceType':'',
    'PatSelfDirectedDigitalInterventions':'',
    'CareProfTeamLocalID':'',
    'OrgIDReferringOrg':'',
    'NHSServAgreeLineID':'',
    'ReferRejectionTime':'',
    'PresentingComplaints':'',
    'ReferClosReason':'',
    'HospitalProviderSpells':'',
    'ReferRejectReason':'',
    'ReferringCareProfessionalType':'',
    'ReferRejectionDate':''
}

#  MHS301GroupSession
group_session_dict = {
    'ActLocTypeCode': 'A01',
    'CareProfLocalID': 'CPI00000000000000001',
    'ClinContDurOfGroupSess': 5874,
    'GroupSessDate': date(2018, 5, 4),
    'GroupSessID': 'SJKFDJRE74C',
    'GroupSessType': '01',
    'Header': header_dict,
    'META': meta_dict,
    'NumberOfGroupSessParticip': 100,
    'OrgIDComm': 'DAISY',
    'RowNumber': 1,
    'ServTeamTypeRefToMH': 'A01',
    'SiteIDOfTreat': 'DONALD',
    'NHSServAgreeLineID':''
}

#  MHS302MHDropInContact
mental_health_drop_in_contact_dict = {
    'CareContactDateMHDropInContact': date(2018, 5, 4),
    'CareProfLocalID': 'CPI00000000000000001',
    'ConsMechanismMH': '01',
    'EndTimeDropInContact': datetime(2018, 5, 4, 7, 47, 40),
    'EthnicCategory': 'A',
    'EthnicCategory2021': 'A',
    'GenderIDCode': '1',
    'GenderSameAtBirth': 'Y',
    'Header': header_dict,
    'LocalPatientID': 'LPI00000000000000001',
    'META': meta_dict,
    'MHDropInContactID': 'DCI00000000000000001',
    'MHDropInContactOutcome': '01',
    'MHDropInContactServiceType': 'A17',
    'NHSNumber': 4587455895,
    'OrgIDReceiving': 'SALLY',
    'PersonBirthDate': date(2018, 5, 4),
    'RowNumber': 1,
    'StartTimeDropInContact': datetime(2018, 5, 4, 7, 47, 40),
    'OrgIDComm':'',
}

#  MHS608AnonSelfAssess
anonymous_self_assessment_dict = {
    'ActLocTypeCode': 'A01',
    'AssToolCompTimestamp': datetime(1981,10,28,6,6,24),
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
    'CareProfLocalID': 'CPI00000000000000001',
    'CareProfStaffGpMH': '01',
    'Header': header_dict,
    'META': meta_dict,
    'MainSpecCodeMH': '600',
    'OccCode': 'GH5',
    'ProfRegBodyCode': '01',
    'ProfRegEntryID': 'SHEK58EJNT4',
    'RowNumber': 1,
    'OrgIDCareProfLocalID':''
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

@pytest.fixture(scope='function')
def referral():
    yield copy.deepcopy(referral_dict)


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
    (primary_diagnosis_dict, PrimaryDiagnosis),
    (secondary_diagnosis_dict, SecondaryDiagnosis),
    (coded_scored_assessment_referral_dict, CodedScoredAssessmentReferral),
    (coded_scored_assessment_care_activity_dict, CodedScoredAssessmentCareActivity),
    (care_program_approach_review_dict, CareProgramApproachReview),
    (care_program_approach_care_episode_dict, CareProgramApproachCareEpisode),
    (ward_stay_dict, WardStay),
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
    (disability_type_dict, DisabilityType),
    (discharge_plan_agreement_dict, DischargePlanAgreement),
    (employment_status_dict, EmploymentStatus),
    (header_dict, Header),
    (home_leave_dict, HomeLeave),
    (indirect_activity_dict, IndirectActivity),
    (leave_of_absence_dict, LeaveOfAbsence),
    (mental_health_care_coordinator_dict, MentalHealthCareCoordinator),
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
    (social_and_personal_circumstances_dict, SocialAndPersonalCircumstances)
])
def test_contains_all_submitted_values(values: Dict[str, Any], model: Type[DSPStructuredModel]):
    given_fields = values.keys()
    required_fields = model.get_field_names(DSPStructuredModel.submitted_fields_only)
    assert set(given_fields) == set(required_fields), "Required {} but got {}".format(required_fields, given_fields)


@pytest.fixture(scope='function')
def referral():
    yield copy.deepcopy(referral_dict)


@pytest.fixture(scope='function')
def master_patient_index():
    yield copy.deepcopy(master_patient_index_dict)


@pytest.fixture(scope='function')
def ward_stay():
    yield copy.deepcopy(ward_stay_dict)


@pytest.fixture(scope='function')
def referral_to_treatment():
    yield copy.deepcopy(referral_to_treatment_dict)


@pytest.fixture(scope='function')
def anonymous_self_assessment():
    yield copy.deepcopy(anonymous_self_assessment_dict)


@pytest.fixture(scope='function')
def group_session():
    yield copy.deepcopy(group_session_dict)


@pytest.fixture(scope='function')
def mental_health_act_legal_status_classification_assignment_periods():
    yield copy.deepcopy(mental_health_act_legal_status_classification_assignment_period_dict)


def group_session_copy():
    return copy.deepcopy(group_session_dict)


def header_copy():
    return copy.deepcopy(header_dict)
