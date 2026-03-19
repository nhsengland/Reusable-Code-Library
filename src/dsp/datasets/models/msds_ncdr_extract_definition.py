from typing import Dict
from typing import List

from dsp.datasets.models.common import ViewDefinition
from dsp.datasets.models.msds_lead_provider_toggle import DefinitionWithLeadProviderToggle


class MSD000Header(ViewDefinition):
    IncludedFields = [
        'Version',
        'OrgIDProvider',
        'OrgIDSubmit',
        'PrimSystemInUse',
        'RPStartDate',
        'RPEndDate',
        'FileCreationDate',
        'FileCreationTime',
        'MSD000_ID',
        'OrgCodeProvider',
        'UniqSubmissionID',
        'UploadDate',
        'UploadTime',
        'EndProcDate',
        'EndProcTime',
        'TotRec',
        'TotMSD001',
        'TotMSD002',
        'TotMSD003',
        'TotMSD004',
        'TotMSD101',
        'TotMSD102',
        'TotMSD103',
        'TotMSD104',
        'TotMSD105',
        'TotMSD106',
        'TotMSD107',
        'TotMSD108',
        'TotMSD109',
        'TotMSD201',
        'TotMSD202',
        'TotMSD203',
        'TotMSD301',
        'TotMSD302',
        'TotMSD401',
        'TotMSD402',
        'TotMSD403',
        'TotMSD404',
        'TotMSD405',
        'TotMSD406',
        'TotMSD501',
        'TotMSD502',
        'TotMSD503',
        'TotMSD504',
        'TotMSD601',
        'TotMSD602',
        'TotMSD901',

    ]

    Locations = {
        "EndProcDate": "current_date() as EndProcDate",
        "EndProcTime": "date_format(current_timestamp(), 'HH:mm:ss') as EndProcTime",
        "TotRec": "+".join(
            [field for field in IncludedFields if field.startswith("TotMSD")]
        )
        + " as TotRec",
        "UploadDate": "date_format(effective_from, 'yyyy-MM-dd') as UploadDate",
        "UploadTime": "date_format(effective_from, 'HH:mm:ss') as UploadTime",
    }



class MSD001MotherDemog(ViewDefinition):
    IncludedFields = [
        'AgeAtDeathMother',
        'CCGResidenceMother',
        'CountyMother',
        'DayOfWeekOfDeathMother',
        'ElectoralWardMother',
        'EthnicCategoryMother',
        'LAD_UAMother',
        'LPIDMother',
        'LSOAMother2011',
        'MSD001_ID',
        'MeridianOfDeathMother',
        'MonthOfDeathMother',
        'NHSNumberMother',
        'NHSNumberStatusMother',
        'OrgCodeProvider',
        'OrgIDLPID',
        'OrgIDResidenceResp',
        'PersonBirthDateMother',
        'PersonDeathDateMother',
        'PersonDeathTimeMother',
        'Person_ID_Mother',
        'Postcode',
        'PostcodeDistrictMother',
        'Rank_IMD_Decile_2015',
        'RecordNumber',
        'UniqSubmissionID',
        'ValidNHSNoFlagMother',
        'ValidPostcodeFlag',
        'YearOfDeathMother',

    ]

    Locations = {
    }


class MSD002GP(ViewDefinition):
    IncludedFields = [
        'CCGResponsibilityMother',
        'EndDateGMPReg',
        'LPIDMother',
        'MSD002_ID',
        'OrgCodeGMPMother',
        'OrgCodeProvider',
        'OrgIDGPPrac',
        'Person_ID_Mother',
        'RecordNumber',
        'StartDateGMPReg',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD003SocPersCircumstances(ViewDefinition):
    IncludedFields = [
        'LPIDMother',
        'MSD003_ID',
        'OrgCodeProvider',
        'Person_ID_Mother',
        'RecordNumber',
        'SocPerDate',
        'SocPerSNOMED',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD004OverseasVisChargCat(ViewDefinition):
    IncludedFields = [
        'LPIDMother',
        'MSD004_ID',
        'OrgCodeProvider',
        'OvsVisChCat',
        'OvsVisChCatAppDate',
        'Person_ID_Mother',
        'RecordNumber',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD101PregnancyBooking(ViewDefinition):
    IncludedFields = [
        'AgeAtBookingMother',
        'AlcoholUnitsBooking',
        'AlcoholUnitsPerWeekBand',
        'AntePathLevel',
        'AntenatalAppDate',
        'CigarettesPerDayBand',
        'ComplexSocialFactorsInd',
        'DisabilityIndMother',
        'DischReason',
        'DischargeDateMatService',
        'EDDAgreed',
        'EDDMethodAgreed',
        'EmploymentStatusMother',
        'EmploymentStatusPartner',
        'FolicAcidSupplement',
        'GestAgeBooking',
        'LPIDMother',
        'LangCode',
        'LastMenstrualPeriodDate',
        'LeadAnteProvider',
        'MHPredictionDetectionIndMother',
        'MSD101_ID',
        'OrgCodeAPLC',
        'OrgCodeProvider',
        'OrgIDComm',
        'OrgIDProvOrigin',
        'OrgIDRecv',
        'OrgSiteIDBooking',
        'PersonBMIBand',
        'PersonBMIBooking',
        'Person_ID_Mother',
        'PregFirstConDate',
        'PregFirstContactCareProfType',
        'PregnancyID',
        'PreviousCaesareanSections',
        'PreviousLiveBirths',
        'PreviousLossesLessThan24Weeks',
        'PreviousStillBirths',
        'ReasonLateBooking',
        'RecordNumber',
        'SmokingStatusBooking',
        'SmokingStatusDelivery',
        'SmokingStatusDischarge',
        'SourceRefMat',
        'SupportStatusIndMother',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD102MatCarePlan(ViewDefinition):
    IncludedFields = [
        'CarePlanDate',
        'CarePlanType',
        'CareProfLID',
        'ContCarePathInd',
        'MSD102_ID',
        'MatPersCarePlanInd',
        'OrgCodeProvider',
        'OrgSiteIDPlannedDelivery',
        'Person_ID_Mother',
        'PlannedDeliverySetting',
        'PregnancyID',
        'ReasonChangeDelSettingAnt',
        'RecordNumber',
        'TeamLocalID',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD103DatingScan(ViewDefinition):
    IncludedFields = [
        'AbnormalityDatingUltrasound',
        'ActivityOfferDateUltrasound',
        'FetalOrder',
        'GestAgeDatUltraDate',
        'GestationDatingUltrasound',
        'LocalFetalID',
        'MSD103_ID',
        'NoFetusesDatingUltrasound',
        'OfferStatusDatingUltrasound',
        'OrgCodeProvider',
        'OrgIDDatingUltrasound',
        'Person_ID_Mother',
        'PregnancyID',
        'ProcedureDateDatingUltrasound',
        'RecordNumber',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD104CodedScoreAssPreg(ViewDefinition):
    IncludedFields = [
        'CompDate',
        'GestAgeAssToolDate',
        'MSD104_ID',
        'OrgCodeProvider',
        'Person_ID_Mother',
        'PregnancyID',
        'RecordNumber',
        'Score',
        'ToolTypeSNOMED',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD105ProvDiagnosisPreg(ViewDefinition):
    IncludedFields = [
        'DiagScheme',
        'FetalOrder',
        'GestAgeProvDiagDate',
        'LocalFetalID',
        'MSD105_ID',
        'MapICD10ProvCode',
        'MapICD10ProvDesc',
        'MapSnomedCTProvDiagCode',
        'MasterICD10ProvCode',
        'MasterICD10ProvDesc',
        'MasterSnomedCTProvDiagCode',
        'OrgCodeProvider',
        'Person_ID_Mother',
        'PregnancyID',
        'ProvDiag',
        'ProvDiagDate',
        'RecordNumber',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD106DiagnosisPreg(ViewDefinition):
    IncludedFields = [
        'ComplicatingDiagInd',
        'Diag',
        'DiagDate',
        'DiagScheme',
        'FetalOrder',
        'GestAgeDiagDate',
        'LocalFetalID',
        'MSD106_ID',
        'MapICD10Code',
        'MapICD10Desc',
        'MapSnomedCTDiagCode',
        'MapSnomedCTDiagTerm',
        'MasterICD10Code',
        'MasterICD10Desc',
        'MasterSnomedCTDiagCode',
        'MasterSnomedCTDiagTerm',
        'OrgCodeProvider',
        'Person_ID_Mother',
        'PregnancyID',
        'RecordNumber',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD107MedHistory(ViewDefinition):
    IncludedFields = [
        'DiagDate',
        'DiagScheme',
        'MSD107_ID',
        'MapICD10PrevCode',
        'MapICD10PrevDesc',
        'MapSnomedCTPrevDiagCode',
        'MapSnomedCTPrevDiagTerm',
        'MasterICD10PrevCode',
        'MasterICD10PrevDesc',
        'MasterSnomedCTPrevDiagCode',
        'MasterSnomedCTPrevDiagTerm',
        'OrgCodeProvider',
        'Person_ID_Mother',
        'PregnancyID',
        'PrevDiag',
        'RecordNumber',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD108FamHistBooking(ViewDefinition):
    IncludedFields = [
        'MSD108_ID',
        'MapICD10SituationCode',
        'MapICD10SituationDesc',
        'MapSnomedCTSituationCode',
        'MapSnomedCTSituationTerm',
        'MasterICD10SituationCode',
        'MasterICD10SituationDesc',
        'MasterSnomedCTSituationCode',
        'MasterSnomedCTSituationTerm',
        'OrgCodeProvider',
        'Person_ID_Mother',
        'PregnancyID',
        'RecordNumber',
        'Situation',
        'SituationScheme',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD109FindingObsMother(ViewDefinition):
    IncludedFields = [
        'FetalOrder',
        'FindingCode',
        'FindingDate',
        'FindingScheme',
        'LocalFetalID',
        'MSD109_ID',
        'MapICD10FindingCode',
        'MapICD10FindingDesc',
        'MapSnomedCTFindingCode',
        'MapSnomedCTFindingTerm',
        'MapSnomedCTObsCode',
        'MapSnomedCTObsTerm',
        'MasterICD10FindingCode',
        'MasterICD10FindingDesc',
        'MasterSnomedCTFindingCode',
        'MasterSnomedCTObsCode',
        'ObsCode',
        'ObsDate',
        'ObsScheme',
        'ObsValue',
        'OrgCodeProvider',
        'Person_ID_Mother',
        'PregnancyID',
        'RecordNumber',
        'UCUMUnit',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD201CareContactPreg(ViewDefinition):
    IncludedFields = [
        'AdminCatCode',
        'AttendCode',
        'CCSubject',
        'CContactDate',
        'CContactTime',
        'CancelDate',
        'CancelReason',
        'CareConID',
        'ConsultType',
        'ContactDuration',
        'GPTherapyInd',
        'GestAgeCContactDate',
        'LocCode',
        'MSD201_ID',
        'Medium',
        'OrgCodeProvider',
        'OrgIDComm',
        'OrgSiteIDOfTreat',
        'Person_ID_Mother',
        'PregnancyID',
        'RecordNumber',
        'ReplApptDate',
        'ReplApptOffDate',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD202CareActivityPreg(ViewDefinition):
    IncludedFields = [
        'ActivityDuration',
        'AlcoholUnitsPerWeek',
        'COMonReading',
        'CareActIDMother',
        'CareConID',
        'CareProfLID',
        'CigarettesPerDay',
        'FetalOrder',
        'FindingCode',
        'FindingScheme',
        'LocalFetalID',
        'MSD202_ID',
        'MapICD10FindingCode',
        'MapICD10FindingDesc',
        'MapOPCS4ProcedureCode',
        'MapOPCS4ProcedureDesc',
        'MapSnomedCTFindingCode',
        'MapSnomedCTFindingTerm',
        'MapSnomedCTObsCode',
        'MapSnomedCTObsTerm',
        'MapSnomedCTProcedureCode',
        'MapSnomedCTProcedureTerm',
        'MasterICD10FindingCode',
        'MasterICD10FindingDesc',
        'MasterOPCS4ProcedureCode',
        'MasterOPCS4ProcedureDesc',
        'MasterSnomedCTFindingCode',
        'MasterSnomedCTObsCode',
        'MasterSnomedCTProcedureCode',
        'ObsCode',
        'ObsScheme',
        'ObsValue',
        'OrgCodeProvider',
        'PersonBMI',
        'PersonHeight',
        'PersonWeight',
        'Person_ID_Mother',
        'ProcedureCode',
        'ProcedureScheme',
        'RecordNumber',
        'SmokingStatus',
        'TeamLocalID',
        'UCUMUnit',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD203CodedScoreAssContact(ViewDefinition):
    IncludedFields = [
        'CareActIDMother',
        'MSD203_ID',
        'OrgCodeProvider',
        'Person_ID_Mother',
        'RecordNumber',
        'Score',
        'ToolTypeSNOMED',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD301LabourDelivery(ViewDefinition):
    IncludedFields = [
        'AdmMethCodeMothDelHospProvSpell',
        'AgeAtLabourMother',
        'BirthsPerLabandDel',
        'CaesareanDate',
        'CaesareanTime',
        'DecisionToDeliverDate',
        'DecisionToDeliverTime',
        'DischDestCodeMothPostDelHospProvSpell',
        'DischMethCodeMothPostDelHospProvSpell',
        'DischargeDateMotherHosp',
        'DischargeTimeMotherHosp',
        'LabourDeliveryID',
        'LabourOnsetDate',
        'LabourOnsetMethod',
        'LabourOnsetTime',
        'LeadPostProvider',
        'MSD301_ID',
        'OrgCodePPLC',
        'OrgCodeProvider',
        'OrgIDPostnatalPathLeadProvider',
        'OrgSiteIDIntra',
        'Person_ID_Mother',
        'PostPathLevel',
        'PregnancyID',
        'ReasonChangeDelSettingLab',
        'RecordNumber',
        'RobsonGroup',
        'SettingIntraCare',
        'StartDateMotherDeliveryHospProvSpell',
        'StartTimeMotherDeliveryHospProvSpell',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD302CareActivityLabDel(ViewDefinition):
    IncludedFields = [
        'ActivityDuration',
        'CareProfLID',
        'ClinInterDateMother',
        'ClinInterTimeMother',
        'Episiotomy',
        'FetalOrder',
        'FindingCode',
        'FindingScheme',
        'GenitalTractTraumaticLesion',
        'LabourAnaesthesiaType',
        'LabourDeliveryID',
        'LabourInductionMethod',
        'LocalFetalID',
        'MCIType',
        'MSD302_ID',
        'MapICD10FindingCode',
        'MapICD10FindingDesc',
        'MapOPCS4ProcedureCode',
        'MapOPCS4ProcedureDesc',
        'MapSnomedCTFindingCode',
        'MapSnomedCTFindingTerm',
        'MapSnomedCTObsCode',
        'MapSnomedCTObsTerm',
        'MapSnomedCTProcedureCode',
        'MapSnomedCTProcedureTerm',
        'MasterICD10FindingCode',
        'MasterICD10FindingDesc',
        'MasterOPCS4ProcedureCode',
        'MasterOPCS4ProcedureDesc',
        'MasterSnomedCTFindingCode',
        'MasterSnomedCTObsCode',
        'MasterSnomedCTProcedureCode',
        'MatCritInd',
        'ObsCode',
        'ObsScheme',
        'ObsValue',
        'OrgCodeProvider',
        'OxytocinAdministeredInd',
        'Person_ID_Mother',
        'ProcedureCode',
        'ProcedureScheme',
        'AROM',
        'RecordNumber',
        'TeamLocalID',
        'UCUMUnit',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD401BabyDemographics(ViewDefinition):
    IncludedFields = [
        'AgeAtBirthMother',
        'AgeAtDeathBaby',
        'BabyFirstFeedDate',
        'BabyFirstFeedIndCode',
        'BabyFirstFeedTime',
        'BirthOrderMaternitySUS',
        'CareProfLIDDel',
        'DayOfBirthBaby',
        'DayOfDeathBaby',
        'DeliveryMethodCode',
        'DischargeDateBabyHosp',
        'DischargeTimeBabyHosp',
        'EthnicCategoryBaby',
        'FetusPresentation',
        'GestationLengthBirth',
        'LPIDBaby',
        'LabourDeliveryID',
        'LocalFetalID',
        'MSD401_ID',
        'MerOfBirthBaby',
        'MeridianOfDeathBaby',
        'MonthOfBirthBaby',
        'MonthOfDeathBaby',
        'NHSNumberBaby',
        'NHSNumberStatusBaby',
        'OrgCodeProvider',
        'OrgIDLocalPatientIdBaby',
        'OrgSiteIDActualDelivery',
        'PersonBirthDateBaby',
        'PersonBirthTimeBaby',
        'PersonDeathDateBaby',
        'PersonDeathTimeBaby',
        'PersonPhenSex',
        'Person_ID_Baby',
        'Person_ID_Mother',
        'PregOutcome',
        'RecordNumber',
        'SettingPlaceBirth',
        'SkinToSkinContact1HourInd',
        'UniqPregID',
        'UniqSubmissionID',
        'ValidNHSNoFlagBaby',
        'WaterDeliveryInd',
        'YearOfBirthBaby',
        'YearOfDeathBaby',

    ]

    Locations = {
    }


class MSD402NeonatalAdmission(ViewDefinition):
    IncludedFields = [
        'LPIDBaby',
        'MSD402_ID',
        'NeoCritCareInd',
        'NeonatalTransferStartDate',
        'NeonatalTransferStartTime',
        'OrgCodeProvider',
        'OrgSiteIDAdmittingNeonatal',
        'Person_ID_Baby',
        'Person_ID_Mother',
        'RecordNumber',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD403ProvDiagNeonatal(ViewDefinition):
    IncludedFields = [
        'DiagScheme',
        'LPIDBaby',
        'MSD403_ID',
        'MapICD10ProvCode',
        'MapICD10ProvDesc',
        'MapSnomedCTProvDiagCode',
        'MasterICD10ProvCode',
        'MasterICD10ProvDesc',
        'MasterSnomedCTProvDiagCode',
        'MasterSnomedCTProvDiagTerm',
        'OrgCodeProvider',
        'Person_ID_Baby',
        'Person_ID_Mother',
        'ProvDiag',
        'ProvDiagDate',
        'RecordNumber',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD404DiagnosisNeonatal(ViewDefinition):
    IncludedFields = [
        'Diag',
        'DiagDate',
        'DiagScheme',
        'LPIDBaby',
        'MSD404_ID',
        'MapICD10Code',
        'MapICD10Desc',
        'MapSnomedCTDiagCode',
        'MapSnomedCTDiagTerm',
        'MasterICD10Code',
        'MasterICD10Desc',
        'MasterSnomedCTDiagCode',
        'MasterSnomedCTDiagTerm',
        'OrgCodeProvider',
        'Person_ID_Baby',
        'Person_ID_Mother',
        'RecordNumber',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD405CareActivityBaby(ViewDefinition):
    IncludedFields = [
        'ActivityDuration',
        'ApgarScore',
        'BirthWeight',
        'CareActIDBaby',
        'CareProfLID',
        'ClinInterDateBaby',
        'ClinInterTimeBaby',
        'FindingCode',
        'FindingScheme',
        'LPIDBaby',
        'MSD405_ID',
        'MapICD10FindingCode',
        'MapICD10FindingDesc',
        'MapOPCS4ProcedureCode',
        'MapOPCS4ProcedureDesc',
        'MapSnomedCTFindingCode',
        'MapSnomedCTFindingTerm',
        'MapSnomedCTObsCode',
        'MapSnomedCTObsTerm',
        'MapSnomedCTProcedureCode',
        'MapSnomedCTProcedureTerm',
        'MasterICD10FindingCode',
        'MasterICD10FindingDesc',
        'MasterOPCS4ProcedureCode',
        'MasterOPCS4ProcedureDesc',
        'MasterSnomedCTFindingCode',
        'MasterSnomedCTObsCode',
        'MasterSnomedCTProcedureCode',
        'NNCritIncInd',
        'ObsCode',
        'ObsScheme',
        'ObsValue',
        'OrgCodeProvider',
        'OrgIDBloodScreeningLab',
        'Person_ID_Baby',
        'Person_ID_Mother',
        'ProcedureCode',
        'ProcedureScheme',
        'RecordNumber',
        'TeamLocalID',
        'UCUMUnit',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD406CodedScoreAssBaby(ViewDefinition):
    IncludedFields = [
        'CareActIDBaby',
        'MSD406_ID',
        'OrgCodeProvider',
        'Person_ID_Baby',
        'Person_ID_Mother',
        'RecordNumber',
        'Score',
        'ToolTypeSNOMED',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD501HospProvSpell(ViewDefinition):
    IncludedFields = [
        'AdmMethCodeHospProvSpell',
        'AgeAtDischargeMother',
        'DischDateHospProvSpell',
        'DischDestCodeHospProvSpell',
        'DischMethCodeHospProvSpell',
        'DischTimeHospProvSpell',
        'HospProvSpellNum',
        'MSD501_ID',
        'OrgCodeProvider',
        'PatientClassCode',
        'Person_ID_Mother',
        'PregnancyID',
        'RecordNumber',
        'SourceAdmCodeHospProvSpell',
        'StartDateHospProvSpell',
        'StartTimeHospProvSpell',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD502HospSpellComm(ViewDefinition):
    IncludedFields = [
        'EndDateOrgCodeComm',
        'HospProvSpellNum',
        'MSD502_ID',
        'OrgCodeProvider',
        'OrgIDComm',
        'Person_ID_Mother',
        'RecordNumber',
        'StartDateOrgCodeComm',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD503WardStay(ViewDefinition):
    IncludedFields = [
        'EndDateWardStay',
        'EndTimeWardStay',
        'HospProvSpellNum',
        'MSD503_ID',
        'OrgCodeProvider',
        'OrgSiteIDOfTreat',
        'Person_ID_Mother',
        'RecordNumber',
        'StartDateWardStay',
        'StartTimeWardStay',
        'UniqPregID',
        'UniqSubmissionID',
        'WardCode',

    ]

    Locations = {
    }


class MSD504AssignedCareProf(ViewDefinition):
    IncludedFields = [
        'CareProfLID',
        'EndDateAssCareProf',
        'HospProvSpellNum',
        'MSD504_ID',
        'OrgCodeProvider',
        'Person_ID_Mother',
        'RecordNumber',
        'StartDateAssCareProf',
        'TeamLocalID',
        'TreatFuncCodeMat',
        'UniqPregID',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD601AnonSelfAssessment(ViewDefinition):
    IncludedFields = [
        'CompDate',
        'LocCode',
        'MSD601_ID',
        'OrgCodeProvider',
        'OrgIDComm',
        'AnonRecordNumber',
        'Score',
        'ToolTypeSNOMED',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD602AnonFindings(ViewDefinition):
    IncludedFields = [
        'ClinInterDate',
        'FindingCode',
        'FindingScheme',
        'MSD602_ID',
        'MapICD10FindingCode',
        'MapICD10FindingDesc',
        'MapSnomedCTFindingCode',
        'MapSnomedCTFindingTerm',
        'MasterICD10FindingCode',
        'MasterICD10FindingDesc',
        'MasterSnomedCTFindingCode',
        'OrgCodeProvider',
        'OrgIDComm',
        'AnonRecordNumber',
        'UniqSubmissionID',

    ]

    Locations = {
    }


class MSD901StaffDetails(ViewDefinition):
    IncludedFields = [
        'CareProfLID',
        'JobRoleCode',
        'MSD901_ID',
        'OccupationCode',
        'OrgCodeProvider',
        'ProfRegCode',
        'RecordNumber',
        'StaffGroup',
        'UniqSubmissionID',

    ]

    Locations = {
    }


MSD000Header = DefinitionWithLeadProviderToggle(MSD000Header)
MSD001MotherDemog = DefinitionWithLeadProviderToggle(MSD001MotherDemog)
MSD002GP = DefinitionWithLeadProviderToggle(MSD002GP)
MSD003SocPersCircumstances = DefinitionWithLeadProviderToggle(MSD003SocPersCircumstances)
MSD004OverseasVisChargCat = DefinitionWithLeadProviderToggle(MSD004OverseasVisChargCat)
MSD101PregnancyBooking = DefinitionWithLeadProviderToggle(MSD101PregnancyBooking)
MSD102MatCarePlan = DefinitionWithLeadProviderToggle(MSD102MatCarePlan)
MSD103DatingScan = DefinitionWithLeadProviderToggle(MSD103DatingScan)
MSD104CodedScoreAssPreg = DefinitionWithLeadProviderToggle(MSD104CodedScoreAssPreg)
MSD105ProvDiagnosisPreg = DefinitionWithLeadProviderToggle(MSD105ProvDiagnosisPreg)
MSD106DiagnosisPreg = DefinitionWithLeadProviderToggle(MSD106DiagnosisPreg)
MSD107MedHistory = DefinitionWithLeadProviderToggle(MSD107MedHistory)
MSD108FamHistBooking = DefinitionWithLeadProviderToggle(MSD108FamHistBooking)
MSD109FindingObsMother = DefinitionWithLeadProviderToggle(MSD109FindingObsMother)
MSD201CareContactPreg = DefinitionWithLeadProviderToggle(MSD201CareContactPreg)
MSD202CareActivityPreg = DefinitionWithLeadProviderToggle(MSD202CareActivityPreg)
MSD203CodedScoreAssContact = DefinitionWithLeadProviderToggle(MSD203CodedScoreAssContact)
MSD301LabourDelivery = DefinitionWithLeadProviderToggle(MSD301LabourDelivery)
MSD302CareActivityLabDel = DefinitionWithLeadProviderToggle(MSD302CareActivityLabDel)
MSD401BabyDemographics = DefinitionWithLeadProviderToggle(MSD401BabyDemographics)
MSD402NeonatalAdmission = DefinitionWithLeadProviderToggle(MSD402NeonatalAdmission)
MSD403ProvDiagNeonatal = DefinitionWithLeadProviderToggle(MSD403ProvDiagNeonatal)
MSD404DiagnosisNeonatal = DefinitionWithLeadProviderToggle(MSD404DiagnosisNeonatal)
MSD405CareActivityBaby = DefinitionWithLeadProviderToggle(MSD405CareActivityBaby)
MSD406CodedScoreAssBaby = DefinitionWithLeadProviderToggle(MSD406CodedScoreAssBaby)
MSD501HospProvSpell = DefinitionWithLeadProviderToggle(MSD501HospProvSpell)
MSD502HospSpellComm = DefinitionWithLeadProviderToggle(MSD502HospSpellComm)
MSD503WardStay = DefinitionWithLeadProviderToggle(MSD503WardStay)
MSD504AssignedCareProf = DefinitionWithLeadProviderToggle(MSD504AssignedCareProf)
MSD601AnonSelfAssessment = DefinitionWithLeadProviderToggle(MSD601AnonSelfAssessment)
MSD602AnonFindings = DefinitionWithLeadProviderToggle(MSD602AnonFindings)
MSD901StaffDetails = DefinitionWithLeadProviderToggle(MSD901StaffDetails)


def get_extract_classes_by_name() -> Dict[str, ViewDefinition]:
    # TODO: Use of globals() is safe in this context but scary in general, should be revised
    return {
        k.lower(): v for k, v in globals().items()
        if k.startswith("MSD")
    }


def get_all_extract_classes() -> List[ViewDefinition]:
    return list(get_extract_classes_by_name().values())
