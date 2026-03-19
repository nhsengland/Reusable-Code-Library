import copy
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Any, Type

import pytest

from dsp.dam import current_record_version
from dsp.common.structured_model import DSPStructuredModel, META
from dsp.shared.constants import DS
from dsp.datasets.models.csds_v1_6 import *

onward_referral_dict = {
    'ServiceRequestID': '902123456789',
    'Onward_ReferralDate': date(2015, 5, 27),
    'Onward_ReferralReason': '03',
    'OrgID_Receiving': 'R1D',
    'RowNumber': 1,
}

coded_score_assessment_contact_dict = {
    'CareActivityID': 'CA000000001',
    'SNOMED_ID': '12356848',
    'Score': '35',
    'RowNumber': 1,
}

breast_feeding_status_dict = {
    'CareActivityID': 'CA000000001',
    'BreastFeedingStatus': '01',
    'RowNumber': 1,
}

observation_dict = {
    'CareActivityID': 'CA000000001',
    'Weight': '65',
    'Height': '1.7',
    'Length': '170',
    'RowNumber': 1,
}

care_activity_dict = {
    'CareActivityID': 'CA000000001',
    'CareContactID': 'CC00000001',
    'Activity_Type': '02',
    'CareProfessionalID_Local': 'CPL00000000000000001',
    'CareActivity_Duration': 1,
    'Procedure_Scheme': '06',
    'CodedProcedure': 'LALALA',
    'Finding_Scheme': '03',
    'CodedFinding': 'Qyu..',
    'Observation_Scheme': '03',
    'CodedObservation': '413737006',
    'ObservationValue': 'L4YI3CW5GW',
    'UC_UnitOfMeasurement': 'KS2NY3NN',
    'BreastfeedingStatuses': [
        breast_feeding_status_dict
    ],
    'Observations': [
        observation_dict
    ],
    'CodedScoredAssessmentContacts': [
        coded_score_assessment_contact_dict
    ],
    'RowNumber': 1,
}

care_contact_dict = {
    'CareContactID': 'CC00000001',
    'ServiceRequestID': '902123456789',
    'TeamID_Local': 'PINK',
    'Contact_Date': date(2015, 5, 19),
    'Contact_Time': datetime(1970, 1, 1, 20, 9, 10),
    'OrgID_Commissioner': '11H',
    'AdminCategory': '01',
    'CareContact_Duration': 8578,
    'Consultation_Type': '01',
    'CareContact_Subject': '01',
    'ConsMechanism': '01',
    'Activity_LocationType': 'B01',
    'Treatment_OrgSiteID': 'A1234',
    'GroupTherapyIndicator': 'n',
    'AttendanceStatus': '5',
    'ReasonableOffer_Date': date(2007, 7, 13),
    'ClinicallyAppropriate_Date': None,
    'Contact_CancellationDate': None,
    'Contact_CancellationReason': '',
    'ReplacementAppointment_OfferedDate': None,
    'ReplacementAppointment_BookedDate': None,
    'CareActivities': [
        care_activity_dict
    ],
    'RowNumber': 1,
}

secondary_diagnosis_dict = {
    'ServiceRequestID': '902123456789',
    'Diagnosis_Scheme': '06',
    'SecondaryDiagnosis': 'secondrydiagnosis',
    'SecondaryDiagnosis_Date': date(2020, 1, 1),
    'RowNumber': 1,
}

primary_diagnosis_dict = {
    'ServiceRequestID': '902123456789',
    'Diagnosis_Scheme': '06',
    'PrimaryDiagnosis': 'primarydiagnosis',
    'PrimaryDiagnosis_Date': date(2020, 1, 1),
    'RowNumber': 1,
}

provisional_diagnosis_dict = {
    'ServiceRequestID': '902123456789',
    'Diagnosis_Scheme': '06',
    'ProvisionalDiagnosis': 'provisionaldiagnosis',
    'ProvisionalDiagnosis_Date': date(2020, 1, 1),
    'RowNumber': 1,
}

coded_score_dassessment_referral_dict = {
    'ServiceRequestID': '902123456789',
    'SNOMED_ID': '12356848',
    'Score': '6.6',
    'AssessmentCompletion_Date': date(2015, 5, 7),
    'RowNumber': 1,
}

referral_to_treatmet_dict = {
    'ServiceRequestID': '902123456789',
    'BookingReference': '123456',
    'PatientPathwayID': '5555',
    'PatientPathwayID_OrgID': 'blah',
    'WaitingTime_MeasurementType': '02',
    'RTT_StartDate': date(2020, 1, 1),
    'RTT_StartTime': datetime(2020, 1, 1, 2, 23, 11),
    'RTT_EndDate': date(2020, 1, 31),
    'RTT_EndTime': datetime(2020, 1, 31, 2, 23, 11),
    'RTT_Status': '10',
    'RowNumber': 1,
}

referral_to_treatmet_dict_2 = {
    'ServiceRequestID': '902123456789',
    'BookingReference': '123456',
    'PatientPathwayID': '5555',
    'PatientPathwayID_OrgID': 'blah',
    'WaitingTime_MeasurementType': '02',
    'RTT_StartDate': date(2020, 1, 1),
    'RTT_StartTime': datetime(2020, 1, 1, 2, 23, 11),
    'RTT_EndDate': date(2020, 1, 31),
    'RTT_EndTime': datetime(2020, 1, 31, 2, 23, 11),
    'RTT_Status': '10',
    'UniqueID_CYP104': Decimal(123),
    'RecordStartDate': date(2020, 1, 1),
    'RecordEndDate': date(2020, 1, 31),
    'RowNumber': 1,
}

referral_to_treatmet_dict_3 = {
    'ServiceRequestID': '902123456790',
    'BookingReference': '123456',
    'PatientPathwayID': '5555',
    'PatientPathwayID_OrgID': 'blah',
    'WaitingTime_MeasurementType': '02',
    'RTT_StartDate': date(2020, 1, 1),
    'RTT_StartTime': datetime(2020, 1, 1, 2, 23, 11),
    'RTT_EndDate': date(2020, 1, 31),
    'RTT_EndTime': datetime(2020, 1, 31, 2, 23, 11),
    'RTT_Status': '10',
    'UniqueID_CYP104': Decimal(123),
    'RecordStartDate': date(2020, 1, 1),
    'RecordEndDate': date(2020, 1, 31),
    'RowNumber': 1,
}

referral_to_treatmet_dict_4 = {
    'ServiceRequestID': '902123456790',
    'BookingReference': '123456',
    'PatientPathwayID': '5555',
    'PatientPathwayID_OrgID': 'blah',
    'WaitingTime_MeasurementType': '02',
    'RTT_StartDate': date(2020, 1, 1),
    'RTT_StartTime': datetime(2020, 1, 1, 2, 23, 11),
    'RTT_EndDate': date(2020, 1, 31),
    'RTT_EndTime': datetime(2020, 1, 31, 2, 23, 11),
    'RTT_Status': '10',
    'UniqueID_CYP104': Decimal(123),
    'RecordStartDate': date(2020, 1, 1),
    'RecordEndDate': date(2020, 1, 31),
    'RowNumber': 1,
}

other_reasons_for_referral_dict = {
    'ServiceRequestID': '902123456789',
    'ReferralReason_Other': '051',
    'RowNumber': 1,
}

service_type_referred_to_dict = {
    'ServiceRequestID': '902123456789',
    'TeamID_Local': 'PINK',
    'TeamType': '02',
    'Referral_ClosureDate': date(2019, 1, 1),
    'Referral_RejectionDate': None,
    'Referral_ClosureReason': 'DEYD',
    'Referral_RejectionReason': '02',
    'RowNumber': 1,
}

referral_dict = {
    'ServiceRequestID': '902123456789',
    'LocalPatientID': 'LPI00000000000000001',
    'OrgID_Commissioner': '08H',
    'ReferralRequest_ReceivedDate': date(2001, 7, 13),
    'ReferralRequest_ReceivedTime': datetime(2001, 7, 13, 5, 1),
    'ServiceLineAgreement': 'YUP',
    'SourceOfReferral': 'A3',
    'OrgID_Referring': 'AYO',
    'Referring_StaffGroup': 'C',
    'Priority_Type': 'A',
    'PrimaryReferralReason': 'SICK',
    'Discharge_Date': date(2015, 5, 17),
    'Discharge_LetterIssuedDate': date(2015, 5, 17),
    'ServiceTypesReferredTo': [
        service_type_referred_to_dict
    ],
    'OtherReasonsForReferral': [
        other_reasons_for_referral_dict
    ],
    'ReferralsToTreatment': [
        referral_to_treatmet_dict,
        referral_to_treatmet_dict_2
    ],
    'OnwardReferrals': [
        onward_referral_dict
    ],
    'CareContacts': [
        care_contact_dict
    ],
    'ProvisionalDiagnoses': [
        provisional_diagnosis_dict
    ],
    'PrimaryDiagnoses': [
        primary_diagnosis_dict
    ],
    'SecondaryDiagnoses': [
        secondary_diagnosis_dict
    ],
    'CodedScoredAssessmentReferrals': [
        coded_score_dassessment_referral_dict
    ],
    'RowNumber': 1,
}

referral_dict_2 = {
    'ServiceRequestID': '902123456790',
    'LocalPatientID': 'LPI00000000000000001',
    'OrgID_Commissioner': '07H',
    'ReferralRequest_ReceivedDate': date(2001, 7, 13),
    'ReferralRequest_ReceivedTime': datetime(2001, 7, 13, 5, 1),
    'ServiceLineAgreement': 'YUP',
    'SourceOfReferral': 'A3',
    'OrgID_Referring': 'AYO',
    'Referring_StaffGroup': 'C',
    'Priority_Type': 'A',
    'PrimaryReferralReason': 'SICK',
    'Discharge_Date': date(2015, 5, 17),
    'Discharge_LetterIssuedDate': date(2015, 5, 17),
    'ServiceTypesReferredTo': [
        service_type_referred_to_dict
    ],
    'OtherReasonsForReferral': [
        other_reasons_for_referral_dict
    ],
    'ReferralsToTreatment': [
        referral_to_treatmet_dict_3,
        referral_to_treatmet_dict_4
    ],
    'OnwardReferrals': [
        onward_referral_dict
    ],
    'CareContacts': [
        care_contact_dict
    ],
    'ProvisionalDiagnoses': [
        provisional_diagnosis_dict
    ],
    'PrimaryDiagnoses': [
        primary_diagnosis_dict
    ],
    'SecondaryDiagnoses': [
        secondary_diagnosis_dict
    ],
    'CodedScoredAssessmentReferrals': [
        coded_score_dassessment_referral_dict
    ],
    'RowNumber': 1,
}

gp_practice_registration_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'OrgID_GP': 'A81017',
    'GPRegistration_StartDate': date(1985, 1, 31),
    'GPRegistration_EndDate': None,
    'RowNumber': 1,
}

accomodation_type_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'AccommStatus': 'STUFF',
    'AccommStatus_Date': date(2020, 1, 1),
    'RowNumber': 1,
}

care_plan_agreement_dict = {
    'PlanID': 'AnID',
    'Plan_AgreedBy': '10',
    'Plan_AgreedDate': date(2020, 1, 1),
    'Plan_AgreedTime': datetime(2020, 1, 1, 2, 23, 11),
    'RowNumber': 1
}

care_plan_type_dict = {
    'PlanID': 'anid',
    'LocalPatientID': 'LPI00000000000000001',
    'Plan_Type': '02',
    'Plan_CreatedDate': date(2020, 1, 1),
    'Plan_CreatedTime': datetime(2020, 1, 1, 2, 23, 11),
    'Plan_LastUpdateDate': date(2020, 1, 1),
    'Plan_LastUpdateTime': datetime(2020, 1, 1, 2, 23, 11),
    'Plan_ImplementDate': date(2020, 1, 2),
    'CarePlanAgreements': [
        care_plan_agreement_dict
    ],
    'RowNumber': 1,
}

social_and_personal_circumstances_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'SNOMED_ID': '12356848',
    'Circumstance_RecordedDate': date(2020, 1, 1),
    'RowNumber': 1,
}

employment_status_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'EmploymentStatus': '01',
    'EmploymentStatus_RecordedDate': date(2020, 1, 1),
    'WeeklyHoursWorked': '01',
    'RowNumber': 1,
}

overseas_charging_categories_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'OvsVisChCat': 'stuff',
    'OvsVisChCatAppDate': date(2020, 1, 1),
    'OvsVisChCatEndDate': date(2020, 1, 1),
    'RowNumber': 1
}

special_educational_need_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'EducationNeed_Type': '01',
    'RowNumber': 1,
}

safeguarding_vulderability_factor_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'SafeguardingFactors_Type': '03',
    'RowNumber': 1,
}

child_protection_plan_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'CPP_Reason': '01',
    'CPP_StartDate': date(2020, 1, 1),
    'CPP_EndDate': date(2020, 1, 31),
    'RowNumber': 1,
}

ass_tech_to_support_disability_type_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'SNOMED_ID': '12356848',
    'SNOMED_Date': date(2020, 1, 1),
    'RowNumber': 1,
}

coded_immunisation_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'Immunisation_Date': date(2020, 1, 1),
    'Procedure_Scheme': '06',
    'ImmunisationProcedure': 'jabsnstabs',
    'OrgID_ImmunisationResponsible': 'AE456',
    'RowNumber': 1,
}

immunisation_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'Immunisation_Date': date(2020, 1, 1),
    'ChildhoodImmunisation_Type': '030',
    'OrgID_ImmunisationResponsible': 'BOB12',
    'RowNumber': 1,
}

medical_history_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'Diagnosis_Scheme': '06',
    'PreviousDiagnosis': 'sick',
    'Diagnosis_Date': date(2020, 1, 1),
    'RowNumber': 1,
}

disability_type_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'Disability': '01',
    'DisabilityImpactPerception': '01',
    'RowNumber': 1,
}

newborn_hearing_screening_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'Result_HearingScreening': '01',
    'ServiceRequest_Date': date(2020, 1, 1),
    'Procedure_Date': date(2020, 1, 1),
    'Result_HearingAudiology': '01',
    'RowNumber': 1,
}

blood_spot_results_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'CardCompletion_Date': date(2020, 1, 1),
    'TestResult_ReceivedDate': date(2020, 1, 1),
    'Result_Phenylketonuria': '01',
    'Result_SickleCellDisease': '01',
    'Result_CysticFibrosis': '01',
    'Result_CongenitalHypothyroidism': '01',
    'Result_MCADD': '01',
    'Result_Homocystinuria': '01',
    'Result_MapleSyrupUrineDisease': '01',
    'Result_GlutaricAciduriaTypeOne': '01',
    'Result_IsovalericAciduria': '01',
    'RowNumber': 1,
}

infant_physical_examination_dict = {
    'LocalPatientID': 'LPI00000000000000001',
    'Examination_Date': date(2020, 1, 1),
    'Result_Hips': '01',
    'Result_Heart': '01',
    'Result_Eyes': '01',
    'Result_Testes': '01',
    'RowNumber': 1,
}

meta_dict = {
    'EVENT_ID': '1:1',
    'RECORD_INDEX': 0,
    'EVENT_RECEIVED_TS': datetime(2020, 1, 1, 10, 47, 39),
    'DATASET_VERSION': '2',
    'RECORD_VERSION': current_record_version(DS.CSDS),
}

header_dict = {
    'Version': '2.0',
    'OrgID_Provider': 'RJ6',
    'OrgID_Submitter': 'RJ6',
    'RP_StartDate': date(2020, 1, 1),
    'RP_EndDate': date(2020, 1, 31),
    'FileCreation_DateTime': datetime(2020, 1, 1, 10, 47, 39),
    'PrimarySystem': 'VISION',
    'RowNumber': 1
}

mpi_dict = {
    'META': meta_dict,
    'Header': header_dict,
    'LocalPatientID': 'LPI00000000000000001',
    'OrgID_LocalPatientID': 'sup',
    'OrgID_EducationEstablishment': 'tuktuk',
    'NHSNumber': '8245410578',
    'NHSNumber_Status': '03',
    'DateOfBirth': date(1981, 1, 1),
    'Postcode': 'LS14JL',
    'Gender': '2',
    'EthnicCategory': 'B',
    'EthnicCategory2021': 'B',
    'LanguagePreferred': 'q3',
    'Relationship_MainCarer': 'BPM',
    'HVFAV_Date': date(2020, 1, 1),
    'ChildLookedAfter_Indicator': 'N',
    'Safeguarding_Indicator': 'N',
    'Disability_SupervisionAndCare_Indicator': 'N',
    'EducationAssessment': '01',
    'DiscussedPreferredDeathLocation_Indicator': 'Y',
    'RiskOfUnexpectedDeath_Indicator': 'N',
    'DeathLocationPreferred_Type': '21',
    'DateOfDeath': None,
    'DeathLocationActual_Type': None,
    'NotAtPreferredLocation_Reason': None,
    'NHSNumber_Mother': '3176807597',
    'NHSNumberStatus_Mother': '03',
    'GPPracticeRegistrations': [
        gp_practice_registration_dict
    ],
    'AccommodationTypes': [
        accomodation_type_dict
    ],
    'CarePlanTypes': [
        care_plan_type_dict
    ],
    'SocialAndPersonalCircumstances': [
        social_and_personal_circumstances_dict
    ],
    'EmploymentStatuses': [
        employment_status_dict
    ],
    'OverseasVisitorChargeCats': [
        overseas_charging_categories_dict
    ],
    'Referrals': [
        referral_dict,
        referral_dict_2
    ],
    'SpecialEducationalNeeds': [
        special_educational_need_dict
    ],
    'SafeguardingVulnerabilityFactors': [
        safeguarding_vulderability_factor_dict
    ],
    'ChildProtectionPlans': [
        child_protection_plan_dict
    ],
    'AssTechToSupportDisabilityTypes': [
        ass_tech_to_support_disability_type_dict
    ],
    'CodedImmunisations': [
        coded_immunisation_dict
    ],
    'Immunisations': [
        immunisation_dict
    ],
    'MedicalHistories': [
        medical_history_dict
    ],
    'DisabilityTypes': [
        disability_type_dict
    ],
    'NewbornHearingScreenings': [
        newborn_hearing_screening_dict
    ],
    'BloodSpotResults': [
        blood_spot_results_dict
    ],
    'InfantPhysicalExaminations': [
        infant_physical_examination_dict
    ],
    'RowNumber': 1,
}

patient_dict = {
    "Referrals": [
        referral_dict
    ],
    "Header": header_dict,
    "META": meta_dict,
    'GPPracticeRegistrations': [
        gp_practice_registration_dict
    ],

}

anonymous_self_assessment_dict = {
    "AssessmentCompletion_Date": date(2018, 8, 17),
    "SNOMED_ID": 2323133434,
    "Score": "HE432",
    "Activity_LocationType": "E23",
    "OrgID_Commissioner": "76U2",
    "META": meta_dict,
    "RowNumber": 1,
    'Header': header_dict
}

staff_details_dict = {
    "CareProfessionalID_Local": "HFHJSW22323234",
    "RegistrationBody": "16",
    "RegistrationEntryID": "LE000000000000000000000",
    "StaffGroup": "04",
    "OccupationCode": "EXE",
    "JobRoleCode": "YE212",
    "META": meta_dict,
    "Header": header_dict,
    "RowNumber": 1,
}

mpi_dict_dummy = {
    'Header': {
        'RP_StartDate': date(2020, 1, 1),
        'RP_EndDate': date(2020, 1, 31)
    }
}


@pytest.mark.parametrize(['values', 'model'], [
    (meta_dict, META),
    (header_dict, Header),
    (gp_practice_registration_dict, GPPracticeRegistration),
    (accomodation_type_dict, AccommodationType),
    (care_plan_agreement_dict, CarePlanAgreement),
    (social_and_personal_circumstances_dict, SocialAndPersonalCircumstances),
    (employment_status_dict, EmploymentStatus),
    (overseas_charging_categories_dict, OverseasVisitorChargeCat),
    (service_type_referred_to_dict, ServiceTypeReferredTo),
    (other_reasons_for_referral_dict, OtherReasonForReferral),
    (referral_to_treatmet_dict, ReferralToTreatment),
    (onward_referral_dict, OnwardReferral),
    (special_educational_need_dict, SpecialEducationalNeed),
    (safeguarding_vulderability_factor_dict, SafeguardingVulnerabilityFactor),
    (child_protection_plan_dict, ChildProtectionPlan),
    (ass_tech_to_support_disability_type_dict, AssTechToSupportDisabilityType),
    (coded_immunisation_dict, CodedImmunisation),
    (immunisation_dict, Immunisation),
    (medical_history_dict, MedicalHistory),
    (disability_type_dict, DisabilityType),
    (newborn_hearing_screening_dict, NewbornHearingScreening),
    (blood_spot_results_dict, BloodSpotResult),
    (infant_physical_examination_dict, InfantPhysicalExamination),
    (provisional_diagnosis_dict, ProvisionalDiagnosis),
    (primary_diagnosis_dict, PrimaryDiagnosis),
    (secondary_diagnosis_dict, SecondaryDiagnosis),
    (coded_score_dassessment_referral_dict, CodedScoredAssessmentReferral),
    (breast_feeding_status_dict, BreastfeedingStatus),
    (observation_dict, Observation),
    (coded_score_assessment_contact_dict, CodedScoredAssessmentContact),
    (care_activity_dict, CareActivity),
    (care_contact_dict, CareContact),
    (referral_dict, Referral),
    (care_plan_type_dict, CarePlanType),
    (mpi_dict, MPI),
])
def test_contains_all_submitted_values(values: Dict[str, Any], model: Type[DSPStructuredModel]):
    given_fields = values.keys()
    required_fields = model.get_field_names(DSPStructuredModel.submitted_fields_only)
    assert set(given_fields) == set(required_fields), \
        "Required {} but got {}".format(required_fields, given_fields)


def patient():
    return copy.deepcopy(patient_dict)


def mpi():
    return copy.deepcopy(mpi_dict)


def anon_self_assessment():
    return copy.deepcopy(anonymous_self_assessment_dict)


def staff_details():
    return copy.deepcopy(staff_details_dict)


def mpi_dummy():
    return copy.deepcopy(mpi_dict_dummy)


def header():
    yield copy.deepcopy(header_dict)


def header_copy():
    return copy.deepcopy(header_dict)


def referral():
    return copy.deepcopy(referral_dict)