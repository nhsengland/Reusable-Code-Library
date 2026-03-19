from dsp.datasets.definitions.msds.submission_constants import ALL_TABLES
from dsp.datasets.models.common import OrderedViewDefinition
from dsp.datasets.models.msds_lead_provider_toggle import DefinitionWithLeadProviderToggle


class MSD000Header(OrderedViewDefinition):
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

    # if changed, add equivalent functionality to _MSDs/dscro/executor.py get_relevant_headers
    Locations = {
        'TotRec': '0 as TotRec',
        **{"Tot{}".format(table_name): '0 as ' + "Tot{}".format(table_name) for table_name in ALL_TABLES.keys()},
        'UploadDate': "to_date(PregnancyAndBookingDetails.META.EVENT_RECEIVED_TS, 'yyyy-MM-dd') as UploadDate",
        'UploadTime': "date_format(PregnancyAndBookingDetails.META.EVENT_RECEIVED_TS, 'HH:mm:ss') as UploadTime",
        'EndProcDate': "'1970-01-01' as EndProcDate",
        'EndProcTime': "'00:00:00' as EndProcTime",
    }


class MSD001MotherDemog(OrderedViewDefinition):
    IncludedFields = [
        'LPIDMother',
        'OrgIDLPID',
        'PersonBirthDateMother',
        'OrgIDResidenceResp',
        'NHSNumberMother',
        'NHSNumberStatusMother',
        'Postcode',
        'EthnicCategoryMother',
        'PersonDeathDateMother',
        'PersonDeathTimeMother',
        'MSD001_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'ValidNHSNoFlagMother',
        'ValidPostcodeFlag',
        'PostcodeDistrictMother',
        'LSOAMother2011',
        'LAD_UAMother',
        'CCGResidenceMother',
        'CountyMother',
        'ElectoralWardMother',
        'AgeAtDeathMother',
        'YearOfDeathMother',
        'MonthOfDeathMother',
        'DayOfWeekOfDeathMother',
        'MeridianOfDeathMother',
        'Rank_IMD_Decile_2015',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
    }


class MSD002GP(OrderedViewDefinition):
    IncludedFields = [
        'LPIDMother',
        'OrgCodeGMPMother',
        'StartDateGMPReg',
        'EndDateGMPReg',
        'OrgIDGPPrac',
        'MSD002_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'CCGResponsibilityMother',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
    }


class MSD003SocPersCircumstances(OrderedViewDefinition):
    IncludedFields = [
        'LPIDMother',
        'SocPerSNOMED',
        'SocPerDate',
        'MSD003_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
    }


class MSD004OverseasVisChargCat(OrderedViewDefinition):
    IncludedFields = [
        'LPIDMother',
        'OvsVisChCat',
        'OvsVisChCatAppDate',
        'MSD004_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
    }


class MSD101PregnancyBooking(OrderedViewDefinition):
    IncludedFields = [
        'PregnancyID',
        'LPIDMother',
        'OrgIDComm',
        'AntenatalAppDate',
        'PregFirstConDate',
        'EDDAgreed',
        'OrgSiteIDBooking',
        'EDDMethodAgreed',
        'SourceRefMat',
        'OrgIDProvOrigin',
        'OrgIDRecv',
        'ReasonLateBooking',
        'PregFirstContactCareProfType',
        'LastMenstrualPeriodDate',
        'DisabilityIndMother',
        'LangCode',
        'MHPredictionDetectionIndMother',
        'ComplexSocialFactorsInd',
        'EmploymentStatusMother',
        'SupportStatusIndMother',
        'EmploymentStatusPartner',
        'PreviousCaesareanSections',
        'PreviousLiveBirths',
        'PreviousStillBirths',
        'PreviousLossesLessThan24Weeks',
        'FolicAcidSupplement',
        'DischargeDateMatService',
        'DischReason',
        'MSD101_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
        'AgeAtBookingMother',
        'OrgCodeAPLC',
        'LeadAnteProvider',
        'AntePathLevel',
        'GestAgeBooking',
        'SmokingStatusBooking',
        'SmokingStatusDelivery',
        'SmokingStatusDischarge',
        'AlcoholUnitsBooking',
        'PersonBMIBooking',
        'PersonBMIBand',
        'CigarettesPerDayBand',
        'AlcoholUnitsPerWeekBand',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID'
    }


class MSD102MatCarePlan(OrderedViewDefinition):
    IncludedFields = [
        'PregnancyID',
        'CarePlanDate',
        'CarePlanType',
        'MatPersCarePlanInd',
        'ContCarePathInd',
        'CareProfLID',
        'TeamLocalID',
        'OrgSiteIDPlannedDelivery',
        'PlannedDeliverySetting',
        'ReasonChangeDelSettingAnt',
        'MSD102_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD103DatingScan(OrderedViewDefinition):
    IncludedFields = [
        'PregnancyID',
        'ActivityOfferDateUltrasound',
        'OfferStatusDatingUltrasound',
        'ProcedureDateDatingUltrasound',
        'GestationDatingUltrasound',
        'NoFetusesDatingUltrasound',
        'LocalFetalID',
        'FetalOrder',
        'AbnormalityDatingUltrasound',
        'OrgIDDatingUltrasound',
        'MSD103_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
        'GestAgeDatUltraDate',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD104CodedScoreAssPreg(OrderedViewDefinition):
    IncludedFields = [
        'PregnancyID',
        'ToolTypeSNOMED',
        'Score',
        'CompDate',
        'MSD104_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
        'GestAgeAssToolDate',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD105ProvDiagnosisPreg(OrderedViewDefinition):
    IncludedFields = [
        'PregnancyID',
        'DiagScheme',
        'ProvDiag',
        'ProvDiagDate',
        'LocalFetalID',
        'FetalOrder',
        'MSD105_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
        'GestAgeProvDiagDate',
        'MapSnomedCTProvDiagCode',
        'MapSnomedCTProvDiagTerm',
        'MasterSnomedCTProvDiagCode',
        'MasterSnomedCTProvDiagTerm',
        'MapICD10ProvCode',
        'MapICD10ProvDesc',
        'MasterICD10ProvCode',
        'MasterICD10ProvDesc',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD106DiagnosisPreg(OrderedViewDefinition):
    IncludedFields = [
        'PregnancyID',
        'DiagScheme',
        'Diag',
        'ComplicatingDiagInd',
        'DiagDate',
        'LocalFetalID',
        'FetalOrder',
        'MSD106_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
        'GestAgeDiagDate',
        'MapSnomedCTDiagCode',
        'MapSnomedCTDiagTerm',
        'MasterSnomedCTDiagCode',
        'MasterSnomedCTDiagTerm',
        'MapICD10Code',
        'MapICD10Desc',
        'MasterICD10Code',
        'MasterICD10Desc',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD107MedHistory(OrderedViewDefinition):
    IncludedFields = [
        'PregnancyID',
        'DiagScheme',
        'PrevDiag',
        'DiagDate',
        'MSD107_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
        'MapSnomedCTPrevDiagCode',
        'MapSnomedCTPrevDiagTerm',
        'MasterSnomedCTPrevDiagCode',
        'MasterSnomedCTPrevDiagTerm',
        'MapICD10PrevCode',
        'MapICD10PrevDesc',
        'MasterICD10PrevCode',
        'MasterICD10PrevDesc',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD108FamHistBooking(OrderedViewDefinition):
    IncludedFields = [
        'PregnancyID',
        'SituationScheme',
        'Situation',
        'MSD108_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
        'MapSnomedCTSituationCode',
        'MapSnomedCTSituationTerm',
        'MasterSnomedCTSituationCode',
        'MasterSnomedCTSituationTerm',
        'MapICD10SituationCode',
        'MapICD10SituationDesc',
        'MasterICD10SituationCode',
        'MasterICD10SituationDesc',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD109FindingObsMother(OrderedViewDefinition):
    IncludedFields = [
        'PregnancyID',
        'LocalFetalID',
        'FetalOrder',
        'FindingDate',
        'FindingScheme',
        'FindingCode',
        'ObsDate',
        'ObsScheme',
        'ObsCode',
        'ObsValue',
        'UCUMUnit',
        'MSD109_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
        'MapSnomedCTFindingCode',
        'MapSnomedCTFindingTerm',
        'MasterSnomedCTFindingCode',
        'MasterSnomedCTFindingTerm',
        'MapICD10FindingCode',
        'MapICD10FindingDesc',
        'MasterICD10FindingCode',
        'MasterICD10FindingDesc',
        'MapSnomedCTObsCode',
        'MapSnomedCTObsTerm',
        'MasterSnomedCTObsCode',
        'MasterSnomedCTObsTerm',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD201CareContactPreg(OrderedViewDefinition):
    IncludedFields = [
        'CareConID',
        'PregnancyID',
        'CContactDate',
        'CContactTime',
        'OrgIDComm',
        'AdminCatCode',
        'ContactDuration',
        'ConsultType',
        'CCSubject',
        'Medium',
        'LocCode',
        'OrgSiteIDOfTreat',
        'GPTherapyInd',
        'AttendCode',
        'CancelDate',
        'CancelReason',
        'ReplApptOffDate',
        'ReplApptDate',
        'MSD201_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
        'GestAgeCContactDate',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD202CareActivityPreg(OrderedViewDefinition):
    IncludedFields = [
        'CareActIDMother',
        'CareConID',
        'CareProfLID',
        'TeamLocalID',
        'ActivityDuration',
        'LocalFetalID',
        'FetalOrder',
        'ProcedureScheme',
        'ProcedureCode',
        'FindingScheme',
        'FindingCode',
        'ObsScheme',
        'ObsCode',
        'ObsValue',
        'UCUMUnit',
        'MSD202_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
        'MapSnomedCTProcedureCode',
        'MapSnomedCTProcedureTerm',
        'MasterSnomedCTProcedureCode',
        'MasterSnomedCTProcedureTerm',
        'MapOPCS4ProcedureCode',
        'MapOPCS4ProcedureDesc',
        'MasterOPCS4ProcedureCode',
        'MasterOPCS4ProcedureDesc',
        'MapSnomedCTFindingCode',
        'MapSnomedCTFindingTerm',
        'MasterSnomedCTFindingCode',
        'MasterSnomedCTFindingTerm',
        'MapICD10FindingCode',
        'MapICD10FindingDesc',
        'MasterICD10FindingCode',
        'MasterICD10FindingDesc',
        'MapSnomedCTObsCode',
        'MapSnomedCTObsTerm',
        'MasterSnomedCTObsCode',
        'MasterSnomedCTObsTerm',
        'AlcoholUnitsPerWeek',
        'PersonHeight',
        'PersonWeight',
        'PersonBMI',
        'CigarettesPerDay',
        'COMonReading',
        'SmokingStatus',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD203CodedScoreAssContact(OrderedViewDefinition):
    IncludedFields = [
        'CareActIDMother',
        'ToolTypeSNOMED',
        'Score',
        'MSD203_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD301LabourDelivery(OrderedViewDefinition):
    IncludedFields = [
        'LabourDeliveryID',
        'PregnancyID',
        'OrgSiteIDIntra',
        'SettingIntraCare',
        'ReasonChangeDelSettingLab',
        'LabourOnsetMethod',
        'LabourOnsetDate',
        'LabourOnsetTime',
        'CaesareanDate',
        'CaesareanTime',
        'StartDateMotherDeliveryHospProvSpell',
        'StartTimeMotherDeliveryHospProvSpell',
        'DecisionToDeliverDate',
        'DecisionToDeliverTime',
        'AdmMethCodeMothDelHospProvSpell',
        'DischargeDateMotherHosp',
        'DischargeTimeMotherHosp',
        'DischMethCodeMothPostDelHospProvSpell',
        'DischDestCodeMothPostDelHospProvSpell',
        'OrgIDPostnatalPathLeadProvider',
        'MSD301_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
        'AgeAtLabourMother',
        'LeadPostProvider',
        'PostPathLevel',
        'OrgCodePPLC',
        'BirthsPerLabandDel',
        'RobsonGroup',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD302CareActivityLabDel(OrderedViewDefinition):
    IncludedFields = [
        'LabourDeliveryID',
        'ClinInterDateMother',
        'ClinInterTimeMother',
        'ActivityDuration',
        'CareProfLID',
        'TeamLocalID',
        'LocalFetalID',
        'FetalOrder',
        'MatCritInd',
        'ProcedureScheme',
        'ProcedureCode',
        'FindingScheme',
        'FindingCode',
        'ObsScheme',
        'ObsCode',
        'ObsValue',
        'UCUMUnit',
        'MSD302_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
        'MapSnomedCTProcedureCode',
        'MapSnomedCTProcedureTerm',
        'MasterSnomedCTProcedureCode',
        'MasterSnomedCTProcedureTerm',
        'MapOPCS4ProcedureCode',
        'MapOPCS4ProcedureDesc',
        'MasterOPCS4ProcedureCode',
        'MasterOPCS4ProcedureDesc',
        'MapSnomedCTFindingCode',
        'MapSnomedCTFindingTerm',
        'MasterSnomedCTFindingCode',
        'MasterSnomedCTFindingTerm',
        'MapICD10FindingCode',
        'MapICD10FindingDesc',
        'MasterICD10FindingCode',
        'MasterICD10FindingDesc',
        'MapSnomedCTObsCode',
        'MapSnomedCTObsTerm',
        'MasterSnomedCTObsCode',
        'MasterSnomedCTObsTerm',
        'Episiotomy',
        'GenitalTractTraumaticLesion',
        'LabourAnaesthesiaType',
        'LabourInductionMethod',
        'MCIType',
        'AROM',
        'OxytocinAdministeredInd',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD401BabyDemographics(OrderedViewDefinition):
    IncludedFields = [
        'LPIDBaby',
        'LabourDeliveryID',
        'OrgIDLocalPatientIdBaby',
        'PersonBirthDateBaby',
        'PersonBirthTimeBaby',
        'PregOutcome',
        'PersonPhenSex',
        'EthnicCategoryBaby',
        'NHSNumberBaby',
        'NHSNumberStatusBaby',
        'LocalFetalID',
        'BirthOrderMaternitySUS',
        'PersonDeathDateBaby',
        'PersonDeathTimeBaby',
        'FetusPresentation',
        'GestationLengthBirth',
        'DeliveryMethodCode',
        'WaterDeliveryInd',
        'OrgSiteIDActualDelivery',
        'CareProfLIDDel',
        'SettingPlaceBirth',
        'BabyFirstFeedDate',
        'BabyFirstFeedTime',
        'BabyFirstFeedIndCode',
        'SkinToSkinContact1HourInd',
        'DischargeDateBabyHosp',
        'DischargeTimeBabyHosp',
        'MSD401_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Baby',
        'Person_ID_Mother',
        'UniqPregID',
        'YearOfBirthBaby',
        'MonthOfBirthBaby',
        'DayOfBirthBaby',
        'MerOfBirthBaby',
        'AgeAtBirthMother',
        'AgeAtDeathBaby',
        'YearOfDeathBaby',
        'MonthOfDeathBaby',
        'DayOfDeathBaby',
        'MeridianOfDeathBaby',
        'ValidNHSNoFlagBaby',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD402NeonatalAdmission(OrderedViewDefinition):
    IncludedFields = [
        'LPIDBaby',
        'NeonatalTransferStartDate',
        'NeonatalTransferStartTime',
        'OrgSiteIDAdmittingNeonatal',
        'NeoCritCareInd',
        'MSD402_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Baby',
        'Person_ID_Mother',
        'UniqPregID',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Baby': '_BabyDemographics.Person_ID_Baby',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD403ProvDiagNeonatal(OrderedViewDefinition):
    IncludedFields = [
        'LPIDBaby',
        'DiagScheme',
        'ProvDiag',
        'ProvDiagDate',
        'MSD403_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Baby',
        'Person_ID_Mother',
        'UniqPregID',
        'MapSnomedCTProvDiagCode',
        'MapSnomedCTProvDiagTerm',
        'MasterSnomedCTProvDiagCode',
        'MasterSnomedCTProvDiagTerm',
        'MapICD10ProvCode',
        'MapICD10ProvDesc',
        'MasterICD10ProvCode',
        'MasterICD10ProvDesc',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Baby': '_BabyDemographics.Person_ID_Baby',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD404DiagnosisNeonatal(OrderedViewDefinition):
    IncludedFields = [
        'LPIDBaby',
        'DiagScheme',
        'Diag',
        'DiagDate',
        'MSD404_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Baby',
        'Person_ID_Mother',
        'UniqPregID',
        'MapSnomedCTDiagCode',
        'MapSnomedCTDiagTerm',
        'MasterSnomedCTDiagCode',
        'MasterSnomedCTDiagTerm',
        'MapICD10Code',
        'MapICD10Desc',
        'MasterICD10Code',
        'MasterICD10Desc',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Baby': '_BabyDemographics.Person_ID_Baby',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD405CareActivityBaby(OrderedViewDefinition):
    IncludedFields = [
        'CareActIDBaby',
        'LPIDBaby',
        'ClinInterDateBaby',
        'ClinInterTimeBaby',
        'ActivityDuration',
        'CareProfLID',
        'TeamLocalID',
        'NNCritIncInd',
        'ProcedureScheme',
        'ProcedureCode',
        'FindingScheme',
        'FindingCode',
        'ObsScheme',
        'ObsCode',
        'ObsValue',
        'UCUMUnit',
        'OrgIDBloodScreeningLab',
        'MSD405_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Baby',
        'Person_ID_Mother',
        'UniqPregID',
        'MapSnomedCTProcedureCode',
        'MapSnomedCTProcedureTerm',
        'MasterSnomedCTProcedureCode',
        'MasterSnomedCTProcedureTerm',
        'MapOPCS4ProcedureCode',
        'MapOPCS4ProcedureDesc',
        'MasterOPCS4ProcedureCode',
        'MasterOPCS4ProcedureDesc',
        'MapSnomedCTFindingCode',
        'MapSnomedCTFindingTerm',
        'MasterSnomedCTFindingCode',
        'MasterSnomedCTFindingTerm',
        'MapICD10FindingCode',
        'MapICD10FindingDesc',
        'MasterICD10FindingCode',
        'MasterICD10FindingDesc',
        'MapSnomedCTObsCode',
        'MapSnomedCTObsTerm',
        'MasterSnomedCTObsCode',
        'MasterSnomedCTObsTerm',
        'ApgarScore',
        'BirthWeight',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Baby': '_BabyDemographics.Person_ID_Baby',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD406CodedScoreAssBaby(OrderedViewDefinition):
    IncludedFields = [
        'CareActIDBaby',
        'ToolTypeSNOMED',
        'Score',
        'MSD406_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Baby',
        'Person_ID_Mother',
        'UniqPregID',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Baby': '_BabyDemographics.Person_ID_Baby',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD501HospProvSpell(OrderedViewDefinition):
    IncludedFields = [
        'HospProvSpellNum',
        'PregnancyID',
        'StartDateHospProvSpell',
        'StartTimeHospProvSpell',
        'SourceAdmCodeHospProvSpell',
        'PatientClassCode',
        'AdmMethCodeHospProvSpell',
        'DischDateHospProvSpell',
        'DischTimeHospProvSpell',
        'DischMethCodeHospProvSpell',
        'DischDestCodeHospProvSpell',
        'MSD501_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
        'AgeAtDischargeMother',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD502HospSpellComm(OrderedViewDefinition):
    IncludedFields = [
        'HospProvSpellNum',
        'OrgIDComm',
        'StartDateOrgCodeComm',
        'EndDateOrgCodeComm',
        'MSD502_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD503WardStay(OrderedViewDefinition):
    IncludedFields = [
        'HospProvSpellNum',
        'StartDateWardStay',
        'StartTimeWardStay',
        'EndDateWardStay',
        'EndTimeWardStay',
        'OrgSiteIDOfTreat',
        'WardCode',
        'MSD503_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD504AssignedCareProf(OrderedViewDefinition):
    IncludedFields = [
        'HospProvSpellNum',
        'CareProfLID',
        'TeamLocalID',
        'StartDateAssCareProf',
        'EndDateAssCareProf',
        'TreatFuncCodeMat',
        'MSD504_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
        'Person_ID_Mother',
        'UniqPregID',
    ]

    Locations = {
        'OrgCodeProvider': 'PregnancyAndBookingDetails.Header.OrgCodeProvider',
        'RecordNumber': 'PregnancyAndBookingDetails.RecordNumber',
        'UniqSubmissionID': 'PregnancyAndBookingDetails.Header.UniqSubmissionID',
        'Person_ID_Mother': 'PregnancyAndBookingDetails.Mother.Person_ID_Mother',
        'UniqPregID': 'PregnancyAndBookingDetails.UniqPregID',
    }


class MSD601AnonSelfAssessment(OrderedViewDefinition):
    IncludedFields = [
        'CompDate',
        'ToolTypeSNOMED',
        'Score',
        'LocCode',
        'OrgIDComm',
        'MSD601_ID',
        'OrgCodeProvider',
        'AnonRecordNumber',
        'UniqSubmissionID',
    ]

    Locations = {
        'OrgCodeProvider': 'Header.OrgCodeProvider',
        'UniqSubmissionID': 'Header.UniqSubmissionID',
    }


class MSD602AnonFindings(OrderedViewDefinition):
    IncludedFields = [
        'ClinInterDate',
        'FindingScheme',
        'FindingCode',
        'OrgIDComm',
        'MSD602_ID',
        'OrgCodeProvider',
        'AnonRecordNumber',
        'UniqSubmissionID',
        'MapSnomedCTFindingCode',
        'MapSnomedCTFindingTerm',
        'MasterSnomedCTFindingCode',
        'MasterSnomedCTFindingTerm',
        'MapICD10FindingCode',
        'MapICD10FindingDesc',
        'MasterICD10FindingCode',
        'MasterICD10FindingDesc',
    ]

    Locations = {
        'OrgCodeProvider': 'Header.OrgCodeProvider',
        'UniqSubmissionID': 'Header.UniqSubmissionID',
    }


class MSD901StaffDetails(OrderedViewDefinition):
    IncludedFields = [
        'CareProfLID',
        'ProfRegCode',
        'StaffGroup',
        'OccupationCode',
        'JobRoleCode',
        'MSD901_ID',
        'OrgCodeProvider',
        'RecordNumber',
        'UniqSubmissionID',
    ]

    Locations = {
        'OrgCodeProvider': 'Header.OrgCodeProvider',
        'UniqSubmissionID': 'Header.UniqSubmissionID',
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
