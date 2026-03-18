import copy
from datetime import date, datetime

import pytest

meta_dict = {
    'EVENT_ID': '1234:5',
    'RECORD_INDEX': 0,
    'EVENT_RECEIVED_TS': datetime(2018, 8, 1),
}

header_dict = {
    'FileCreationDate': date(2018, 9, 21),
    'FileCreationTime': datetime(1970, 1, 1, 11, 50, 7),
    'OrgIDProvider': 'RAL',
    'OrgIDSubmit': 'RAL',
    'PrimSystemInUse': 'DAT PSF Generator',
    'RowNumber': 1,
    'RPEndDate': date(2018, 8, 31),
    'RPStartDate': date(2018, 8, 1),
    'Version': '2.0'
}

gp_practice_registrations_dict = {
    "EndDateGMPReg": date(2018, 2, 15),
    "LPIDMother": "LPI00000000000000001",
    "OrgCodeGMPMother": "A81001",
    "OrgIDGPPrac": "05F",
    "RowNumber": 1,
    "StartDateGMPReg": date(2010, 1, 15),
}

social_personal_circumstances_dict = {
    "LPIDMother": "LPI00000000000000001",
    "RowNumber": 1,
    "SocPerDate": date(2009, 4, 17),
    "SocPerSNOMED": 413737006,
}

overseas_visitor_charging_categories_dict = {
    "LPIDMother": "LPI00000000000000001",
    "OvsVisChCat": "A",
    "OvsVisChCatAppDate": date(2014, 9, 16),
    "RowNumber": 1,
}

maternity_care_plans_dict = {
    "CarePlanDate": date(2018, 8, 26),
    "CarePlanType": "05",
    "CareProfLID": "CPL00000000000000001",
    "ContCarePathInd": "Y",
    "MatPersCarePlanInd": "Y",
    "OrgSiteIDPlannedDelivery": "RMC01",
    "PlannedDeliverySetting": "01",
    "PregnancyID": "PREG00000000000000000000000000000001",
    "ReasonChangeDelSettingAnt": "01",
    "RowNumber": 1,
    "TeamLocalID": "CPT00000000000000001",
}

dating_scan_procedures_dict = {
    "AbnormalityDatingUltrasound": "Y",
    "ActivityOfferDateUltrasound": date(2018, 3, 15),
    "FetalOrder": "1",
    "GestationDatingUltrasound": 28,
    "LocalFetalID": "FET000000000000000000000000000000001",
    "NoFetusesDatingUltrasound": 9,
    "OfferStatusDatingUltrasound": "01",
    "OrgIDDatingUltrasound": "R0A",
    "PregnancyID": "PREG00000000000000000000000000000001",
    "ProcedureDateDatingUltrasound": date(2018, 8, 29),
    "RowNumber": 1,
}

coded_scored_assessments_pregnancy_dict = {
    "CompDate": date(2018, 8, 12),
    "PregnancyID": "PREG00000000000000000000000000000001",
    "RowNumber": 1,
    "Score": "6.6",
    "ToolTypeSNOMED": 961031000000108,
}

provisional_diagnosis_pregnancies_dict = {
    "DiagScheme": "02",
    "FetalOrder": "1",
    "LocalFetalID": "FET000000000000000000000000000000001",
    "PregnancyID": "PREG00000000000000000000000000000001",
    "ProvDiag": "F05.0",
    "ProvDiagDate": date(2018, 7, 25),
    "RowNumber": 1,
}

diagnosis_pregnancies_dict = {
    "ComplicatingDiagInd": "Y",
    "Diag": "F05.0",
    "DiagDate": date(2018, 6, 12),
    "DiagScheme": "02",
    "FetalOrder": "1",
    "LocalFetalID": "FET000000000000000000000000000000001",
    "PregnancyID": "PREG00000000000000000000000000000001",
    "RowNumber": 1,
}

medical_history_previous_diagnosis_dict = {
    "DiagDate": date(2001, 9, 11),
    "DiagScheme": "02",
    "PregnancyID": "PREG00000000000000000000000000000001",
    "PrevDiag": "F05.0",
    "RowNumber": 1,
}

family_history_at_booking_dict = {
    "PregnancyID": "PREG00000000000000000000000000000001",
    "RowNumber": 1,
    "Situation": "F05.0",
    "SituationScheme": "01",
}

findings_and_observations_mother = {
    "FetalOrder": "1",
    "FindingCode": "F05.0",
    "FindingDate": date(2008, 8, 22),
    "FindingScheme": "01",
    "LocalFetalID": "FET000000000000000000000000000000001",
    "ObsCode": "E0...",
    "ObsDate": date(2007, 3, 19),
    "ObsScheme": "01",
    "ObsValue": "F9ZV6UH7YZ",
    "PregnancyID": "PREG00000000000000000000000000000001",
    "RowNumber": 1,
    "UCUMUnit": "OR6YP7ZL",
}

coded_scored_assessments_dict = {
    "CareActIDMother": "CAI00000000000000001",
    "RowNumber": 1,
    "Score": "19.6",
    "ToolTypeSNOMED": 961031000000108,
}

care_activities_dict = {
    "ActivityDuration": 1262,
    "CareActIDMother": "CAI00000000000000001",
    "CareConID": "CCI00000000000000001",
    "CareProfLID": "CPL00000000000000001",
    "CodedScoredAssessments": [coded_scored_assessments_dict],
    "FetalOrder": "1",
    "FindingCode": "F05.0",
    "FindingScheme": "01",
    "LocalFetalID": "FET000000000000000000000000000000001",
    "ObsCode": "E0...",
    "ObsScheme": "01",
    "ObsValue": "E4YV9TL2MW",
    "ProcedureCode": "812e",
    "ProcedureScheme": "02",
    "RowNumber": 1,
    "TeamLocalID": "CPT00000000000000001",
    "UCUMUnit": "AT3XR7ZI",
}

care_contacts_dict = {
    "AdminCatCode": "01",
    "AttendCode": "5",
    "CCSubject": "01",
    "CContactDate": date(2018, 8, 2),
    "CContactTime": datetime(1970, 1, 1, 19, 48, 6),
    "CancelDate": date(2018, 6, 26),
    "CancelReason": "01",
    "CareActivities": [care_activities_dict],
    "CareConID": "CCI00000000000000001",
    "ConsultType": "01",
    "ContactDuration": 1265,
    "GPTherapyInd": "Y",
    "LocCode": "A01",
    "Medium": "01",
    "OrgIDComm": "12D",
    "OrgSiteIDOfTreat": "R0A01",
    "PregnancyID": "PREG00000000000000000000000000000001",
    "ReplApptDate": date(2018, 3, 7),
    "ReplApptOffDate": date(2019, 1, 20),
    "RowNumber": 1,
}

care_activity_labours_and_deliveries_dict = {
    "LabourDeliveryID": "AK000000000000000023",
    "ClinInterDateMother": date(2018, 9, 25),
    "ClinInterTimeMother": datetime(1970, 1, 1, 6, 12, 13),
    "ActivityDuration": 1245,
    "CareProfLID": "URY2223333",
    "TeamLocalID": "URY2223333",
    "LocalFetalID": "URY2223333",
    "FetalOrder": "U3",
    "MatCritInd": "6",
    "ProcedureScheme": "04",
    "ProcedureCode": "45225",
    "FindingScheme": "12",
    "FindingCode": "2837a",
    "ObsScheme": "02",
    "ObsCode": "14E46",
    "ObsValue": "478787A",
    "RowNumber": 1,
    "UCUMUnit": "1478787A"
}

provisional_diagnosis_neonatals_dict = {
    "DiagScheme": "02",
    "LPIDBaby": "BAB00000000000000001",
    "ProvDiag": "F05.0",
    "ProvDiagDate": date(2018, 8, 27),
    "RowNumber": 1,
}

neonatal_admissions_dict = {
    "LPIDBaby": "BAB00000000000000001",
    "NeoCritCareInd": "Y",
    "NeonatalTransferStartDate": date(2018, 8, 27),
    "NeonatalTransferStartTime": datetime(1970, 1, 1, 18, 47, 24),
    "OrgSiteIDAdmittingNeonatal": "R0A01",
    "RowNumber": 1,
}

coded_scored_assessments_baby_dict = {
    "CareActIDBaby": "BCA00000000000000001",
    "RowNumber": 1,
    "Score": "8.2",
    "ToolTypeSNOMED": 961031000000108,
}
care_activities_baby_dict = {
    "ActivityDuration": 497,
    "CareActIDBaby": "BCA00000000000000001",
    "CareProfLID": "CPL00000000000000001",
    "ClinInterDateBaby": date(2018, 8, 27),
    "ClinInterTimeBaby": datetime(1970, 1, 1, 17, 35, 41),
    "CodedScoredAssessmentsBaby": [coded_scored_assessments_baby_dict],
    "FindingCode": "F05.0",
    "FindingScheme": "01",
    "LPIDBaby": "BAB00000000000000001",
    "NNCritIncInd": "N",
    "ObsCode": "E0...",
    "ObsScheme": "01",
    "ObsValue": "K5RF6MN9KG",
    "OrgIDBloodScreeningLab": "12DAA",
    "ProcedureCode": "812e",
    "ProcedureScheme": "02",
    "RowNumber": 1,
    "TeamLocalID": "CPT00000000000000001",
    "UCUMUnit": "EC5ER8CP",
}

diagnosis_neonatals_dict = {
    "Diag": "F05.0",
    "DiagDate": date(2018, 8, 27),
    "DiagScheme": "02",
    "LPIDBaby": "BAB00000000000000001",
    "RowNumber": 1,
}

baby_demographics_dict = {
    "BabyFirstFeedDate": date(2018, 8, 28),
    "BabyFirstFeedIndCode": "01",
    "BabyFirstFeedTime": datetime(1970, 1, 1, 9, 6, 25),
    "BirthOrderMaternitySUS": "1",
    "CareActivitiesBaby": [care_activities_baby_dict],
    "CareProfLIDDel": "CPL00000000000000001",
    "DeliveryMethodCode": "0",
    "DiagnosisNeonatals": [diagnosis_neonatals_dict],
    "DischargeDateBabyHosp": date(2018, 8, 30),
    "DischargeTimeBabyHosp": datetime(1970, 1, 1, 0, 21, 31),
    "EthnicCategoryBaby": "A",
    "FetusPresentation": "01",
    "GestationLengthBirth": 233,
    "LPIDBaby": "BAB00000000000000001",
    "LabourDeliveryID": "LAD000000000000000000000000000000002",
    "LocalFetalID": "FET000000000000000000000000000000001",
    "NHSNumberBaby": 9254945129,
    "NHSNumberStatusBaby": "01",
    "NeonatalAdmissions": [neonatal_admissions_dict],
    "OrgIDLocalPatientIdBaby": "R0A",
    "OrgSiteIDActualDelivery": "ZZ201",
    "PersonBirthDateBaby": date(2018, 8, 27),
    "PersonBirthTimeBaby": datetime(1970, 1, 1, 8, 9, 17),
    "PersonDeathDateBaby": date(2018, 8, 31),
    "PersonDeathTimeBaby": datetime(1970, 1, 1, 10, 33, 45),
    "PersonPhenSex": "X",
    "PregOutcome": "01",
    "ProvisionalDiagnosisNeonatals": [provisional_diagnosis_neonatals_dict],
    "RowNumber": 1,
    "SettingPlaceBirth": "01",
    "SkinToSkinContact1HourInd": "N",
    "WaterDeliveryInd": "N",
}

labours_and_deliveries_dict = {
    "AdmMethCodeMothDelHospProvSpell": "11",
    "CaesareanDate": date(2018, 8, 18),
    "CaesareanTime": datetime(1970, 1, 1, 0, 22, 41),
    "CareActivityLaboursAndDeliveries": [care_activity_labours_and_deliveries_dict],
    "BabyDemographics": [baby_demographics_dict],
    "DecisionToDeliverDate": date(2018, 6, 29),
    "DecisionToDeliverTime": datetime(1970, 1, 1, 18, 32, 11),
    "DischDestCodeMothPostDelHospProvSpell": "19",
    "DischMethCodeMothPostDelHospProvSpell": "1",
    "DischargeDateMotherHosp": date(2018, 8, 18),
    "DischargeTimeMotherHosp": datetime(1970, 1, 1, 15, 27, 55),
    "LabourDeliveryID": "LAD000000000000000000000000000000001",
    "LabourOnsetDate": date(2018, 8, 18),
    "LabourOnsetMethod": "5",
    "LabourOnsetTime": datetime(1970, 1, 1, 0, 20, 4),
    "OrgIDPostnatalPathLeadProvider": "R0A",
    "OrgSiteIDIntra": "RMC01",
    "PregnancyID": "PREG00000000000000000000000000000001",
    "ReasonChangeDelSettingLab": "02",
    "RowNumber": 1,
    "SettingIntraCare": "01",
    "StartDateMotherDeliveryHospProvSpell": date(2018, 6, 17),
    "StartTimeMotherDeliveryHospProvSpell": datetime(1970, 1, 1, 16, 54, 40),
}

ward_stays_dict = {
    "EndDateWardStay": date(2018, 8, 24),
    "EndTimeWardStay": datetime(1970, 1, 1, 22, 38, 24),
    "HospProvSpellNum": "HSN000000001",
    "OrgSiteIDOfTreat": "R0A01",
    "RowNumber": 1,
    "StartDateWardStay": date(2018, 8, 24),
    "StartTimeWardStay": datetime(1970, 1, 1, 3, 9, 11),
    "WardCode": "O5ZC5ZQ3AN"
}

assigned_care_professional_dict = {
    "CareProfLID": "CPL00000000000000001",
    "EndDateAssCareProf": date(2018, 8, 24),
    "HospProvSpellNum": "HSN000000001",
    "RowNumber": 1,
    "StartDateAssCareProf": date(2018, 8, 23),
    "TeamLocalID": "CPT00000000000000001",
    "TreatFuncCodeMat": "501",
}

hospital_spells_commissioners_dict = {
    "EndDateOrgCodeComm": date(2018, 8, 24),
    "HospProvSpellNum": "HSN000000001",
    "OrgIDComm": "12D",
    "RowNumber": 1,
    "StartDateOrgCodeComm": date(2018, 8, 23),
}

hospital_provider_spells_dict = {
    "AdmMethCodeHospProvSpell": "11",
    "AssignedCareProfessionals": [assigned_care_professional_dict],
    "DischDateHospProvSpell": date(2018, 8, 24),
    "DischDestCodeHospProvSpell": "19",
    "DischMethCodeHospProvSpell": "1",
    "DischTimeHospProvSpell": datetime(1970, 1, 1, 1, 49, 0),
    "HospProvSpellNum": "HSN000000001",
    "HospitalSpellCommissioners": [hospital_spells_commissioners_dict],
    "PatientClassCode": "1",
    "PregnancyID": "PREG00000000000000000000000000000001",
    "RowNumber": 1,
    "RPEndDate": date(2018, 8, 31),
    "RPStartDate": date(2018, 8, 1),
    "SourceAdmCodeHospProvSpell": "19",
    "StartDateHospProvSpell": date(2018, 8, 23),
    "StartTimeHospProvSpell": datetime(1970, 1, 1, 11, 18, 27),
    "WardStays": [ward_stays_dict]
}

motherdemog_dict = {
    'ClinInterDate': date(2018, 8, 28),
    'Postcode': "LS1 3EQ",
    'GPPracticeRegistrations': [gp_practice_registrations_dict],
    'SocialAndPersonalCircumstances': [social_personal_circumstances_dict],
    'OverseasVisitorChargingCategories': [overseas_visitor_charging_categories_dict],
    "RowNumber": 1,
}

pregnancy_and_booking_details_dict = {
    'META': meta_dict,
    'Header': header_dict,
    "AntenatalAppDate": date(2018, 3, 7),
    "ComplexSocialFactorsInd": "N",
    "DisabilityIndMother": "Y",
    "DischReason": "01",
    "DischargeDateMatService": date(2018, 8, 16),
    "EDDAgreed": date(2019, 2, 8),
    "EDDMethodAgreed": "01",
    "EmploymentStatusMother": "01",
    "EmploymentStatusPartner": "01",
    "FolicAcidSupplement": "01",
    "LPIDMother": "LPI00000000000000001",
    "LangCode": "q1",
    "LastMenstrualPeriodDate": date(2017, 10, 5),
    "MHPredictionDetectionIndMother": "Y",
    "Mother": motherdemog_dict,
    "OrgIDComm": "08H",
    "OrgIDProvOrigin": "R0A",
    "OrgIDRecv": "R0A",
    "OrgSiteIDBooking": "RMC01",
    "PregFirstConDate": date(2017, 8, 3),
    "PregFirstContactCareProfType": "01",
    "PregnancyID": "PREG00000000000000000000000000000001",
    "PreviousCaesareanSections": 37,
    "PreviousLiveBirths": 13,
    "PreviousLossesLessThan24Weeks": 8,
    "PreviousStillBirths": 65,
    "ReasonLateBooking": "01",
    "RowNumber": 1,
    "SourceRefMat": "01",
    "SupportStatusIndMother": "Y",
    "MaternityCarePlans": [maternity_care_plans_dict],
    "DatingScanProcedures": [dating_scan_procedures_dict],
    "CodedScoredAssessmentsPregnancy": [coded_scored_assessments_pregnancy_dict],
    "ProvisionalDiagnosisPregnancies": [provisional_diagnosis_pregnancies_dict],
    "DiagnosisPregnancies": [diagnosis_pregnancies_dict],
    "MedicalHistoryPreviousDiagnoses": [medical_history_previous_diagnosis_dict],
    "FamilyHistoryAtBooking": [family_history_at_booking_dict],
    "FindingsAndObservationsMother": [findings_and_observations_mother],
    "CareContacts": [care_contacts_dict],
    "LaboursAndDeliveries": [labours_and_deliveries_dict],
    "HospitalProviderSpells": [hospital_provider_spells_dict],
}

anonymous_self_assessment_dict = {
    "CompDate": date(2018, 8, 17),
    "ToolTypeSNOMED": 2323133434,
    "Score": "HE432",
    "LocCode": "E23",
    "OrgIDComm": "76U2",
    "META": meta_dict,
    "RowNumber": 1,
    'Header': header_dict
}

anonymous_finding_dict = {
    "ClinInterDate": date(2018, 8, 17),
    "FindingScheme": "02",
    "FindingCode": "QY...",
    "OrgIDComm": "76U3",
    "Header": header_dict,
    "META": meta_dict,
    "RowNumber": 1,
}

staff_details_dict = {
    "CareProfLID": "HFHJSW22323234",
    "ProfRegCode": "16",
    "ProfRegID": "LE000000000000000000000",
    "StaffGroup": "04",
    "OccupationCode": "EXE",
    "JobRoleCode": "YE212",
    "META": meta_dict,
    "Header": header_dict,
    "RowNumber": 1,
}


@pytest.fixture(scope='function')
def anonymous_self_assessment():
    yield copy.deepcopy(anonymous_self_assessment_dict)


@pytest.fixture(scope='function')
def anonymous_finding():
    yield copy.deepcopy(anonymous_finding_dict)


@pytest.fixture(scope='function')
def staff_details():
    yield copy.deepcopy(staff_details_dict)


def staff_details_copy():
    return copy.deepcopy(staff_details_dict)


@pytest.fixture(scope='function')
def motherdemog():
    yield copy.deepcopy(motherdemog_dict)


@pytest.fixture(scope='function')
def header():
    yield copy.deepcopy(header_dict)


def header_copy():
    return copy.deepcopy(header_dict)


@pytest.fixture(scope='function')
def pregnancy_and_booking_details():
    yield copy.deepcopy(pregnancy_and_booking_details_dict)


@pytest.fixture(scope='function')
def care_contacts():
    yield copy.deepcopy(care_contacts_dict)


@pytest.fixture(scope='function')
def care_activities():
    yield copy.deepcopy(care_activities_dict)


@pytest.fixture(scope='function')
def labours_and_deliveries():
    yield copy.deepcopy(labours_and_deliveries_dict)
