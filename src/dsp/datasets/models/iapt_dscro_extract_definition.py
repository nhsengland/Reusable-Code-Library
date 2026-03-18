from dsp.datasets.models.common import OrderedViewDefinition


class IDS000Header(OrderedViewDefinition):
    IncludedFields = [
        'DatSetVer',
        'OrgIDProv',
        'OrgIDSubmit',
        'PrimSystemInUse',
        'ReportingPeriodStartDate',
        'ReportingPeriodEndDate',
        'DateTimeDatSetCreate',
        'UniqueID_IDS000',
        'File_Type',
        'TotalRecords',
        'UniqueSubmissionID',
        'Total_IDS001',
        'Total_IDS002',
        'Total_IDS004',
        'Total_IDS007',
        'Total_IDS011',
        'Total_IDS012',
        'Total_IDS101',
        'Total_IDS105',
        'Total_IDS108',
        'Total_IDS201',
        'Total_IDS202',
        'Total_IDS205',
        'Total_IDS602',
        'Total_IDS603',
        'Total_IDS606',
        'Total_IDS607',
        'Total_IDS803',
        'Total_IDS902',
    ]

    Locations = {
    }


class IDS001MPI(OrderedViewDefinition):
    IncludedFields = [
        'LocalPatientId',
        'OrgIDLocalPatientId',
        'OrgIDResidenceResp',
        'NHSNumber',
        'NHSNumberStatus',
        'PersonBirthDate',
        'Postcode',
        'Gender',
        'EthnicCategory',
        'ExBAFIndicator',
        'LanguageCodePreferred',
        'EduEstabTypeIAPT',
        'RecordNumber',
        'UniqueID_IDS001',
        'OrgID_Provider',
        'OrgID_CCG_Residence',
        'Person_ID',
        'UniqueSubmissionID',
        'Age_RP_StartDate',
        'Age_RP_EndDate',
        'Postcode_District',
        'Postcode_Default',
        'LSOA',
        'LocalAuthority',
        'County',
        'ElectoralWard',
    ]

    Locations = {
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
    }


class IDS002GP(OrderedViewDefinition):
    IncludedFields = [
        'LocalPatientId',
        'GMPCodeReg',
        'StartDateGMPRegistration',
        'EndDateGMPRegistration',
        'OrgIDGPPrac',
        'RecordNumber',
        'UniqueID_IDS002',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
        'OrgID_CCG_GP',
        'DistanceFromHome_GP',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
    }


class IDS004EmpStatus(OrderedViewDefinition):
    IncludedFields = [
        'LocalPatientId',
        'EmployStatus',
        'EmployStatusRecDate',
        'WeekHoursWorked',
        'SelfEmployInd',
        'SickAbsenceInd',
        'SSPInd',
        'BenefitRecInd',
        'JSAInd',
        'ESAInd',
        'UCInd',
        'PIPInd',
        'OtherBenefitInd',
        'EmpSupportInd',
        'EmpSupportReferral',
        'RecordNumber',
        'UniqueID_IDS004',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
    }


class IDS007DisabilityType(OrderedViewDefinition):
    IncludedFields = [
        'LocalPatientId',
        'DisabCode',
        'RecordNumber',
        'UniqueID_IDS007',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
    }


class IDS011SocPerCircumstances(OrderedViewDefinition):
    IncludedFields = [
        'LocalPatientId',
        'SocPerCircumstance',
        'SocPerCircumstanceRecDate',
        'RecordNumber',
        'UniqueID_IDS011',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
    }


class IDS012OverseasVisitorChargCat(OrderedViewDefinition):
    IncludedFields = [
        'LocalPatientId',
        'OvsVisChCat',
        'OvsVisChCatAppDate',
        'RecordNumber',
        'UniqueID_IDS012',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
    }


class IDS101Referral(OrderedViewDefinition):
    IncludedFields = [
        'ServiceRequestId',
        'LocalPatientId',
        'OrgIDComm',
        'ReferralRequestReceivedDate',
        'SourceOfReferralMH',
        'OnsetDate',
        'PrevDiagCondInd',
        'EndCode',
        'ServDischDate',
        'RecordNumber',
        'UniqueID_IDS101',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
        'Unique_ServiceRequestID',
        'Age_ReferralRequest_ReceivedDate',
        'Age_ServiceDischarge_Date',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
    }


class IDS105OnwardReferral(OrderedViewDefinition):
    IncludedFields = [
        'ServiceRequestId',
        'OnwardReferDate',
        'OnwardReferTime',
        'OnwardReferReason',
        'OrgIDReceiving',
        'RecordNumber',
        'UniqueID_IDS105',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
        'Unique_ServiceRequestID',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_ServiceRequestID': 'Referral.Unique_ServiceRequestID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
        'PathwayID': 'Referral.PathwayID',
    }


class IDS108WaitingTimePauses(OrderedViewDefinition):
    IncludedFields = [
        'PauseID',
        'ServiceRequestId',
        'PauseStartDate',
        'PauseEndDate',
        'PauseReason',
        'RecordNumber',
        'UniqueID_IDS108',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
        'Unique_ServiceRequestID',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_ServiceRequestID': 'Referral.Unique_ServiceRequestID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
        'PathwayID': 'Referral.PathwayID',
    }


class IDS201CareContact(OrderedViewDefinition):
    IncludedFields = [
        'CareContactId',
        'ServiceRequestId',
        'CareContDate',
        'CareContTime',
        'OrgIDComm',
        'PlannedCareContIndicator',
        'AttendOrDNACode',
        'Cancellation',
        'ClinContDurOfCareCont',
        'IAPTLTCServiceInd',
        'AppType',
        'ConsMediumUsed',
        'IntEnabledTherProg',
        'CareContPatientTherMode',
        'NumGroupTherParticipants',
        'NumGroupTherFacilitators',
        'PsychMed',
        'ActLocTypeCode',
        'SiteIDOfTreat',
        'LanguageCodeTreat',
        'InterpreterPresentInd',
        'RecordNumber',
        'UniqueID_IDS201',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
        'Unique_ServiceRequestID',
        'Unique_CareContactID',
        'Age_CareContact_Date',
        'DistanceFromHome_ContactLocation',
        'Time_Referral_to_CareContact',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_ServiceRequestID': 'Referral.Unique_ServiceRequestID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
        'PathwayID': 'Referral.PathwayID',
    }


class IDS202CareActivity(OrderedViewDefinition):
    IncludedFields = [
        'CareActId',
        'CareContactId',
        'CarePersLocalId',
        'ClinContactDurOfCareAct',
        'CodeProcAndProcStatus',
        'FindSchemeInUse',
        'CodeFind',
        'CodeObs',
        'ObsValue',
        'UnitMeasure',
        'RecordNumber',
        'UniqueID_IDS202',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
        'Unique_ServiceRequestID',
        'Unique_CareContactID',
        'Unique_CareActivityID',
        'Unique_CarePersonnelID_Local',
        'FindingCode_ICD10_Mapped',
        'FindingCode_ICD10_Master',
        'FindingDescription_ICD10_Master',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_ServiceRequestID': 'Referral.Unique_ServiceRequestID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
        'PathwayID': 'Referral.PathwayID',
    }


class IDS205InternetTherLog(OrderedViewDefinition):
    IncludedFields = [
        'ServiceRequestId',
        'StartDateIntEnabledTherLog',
        'EndDateIntEnabledTherLog',
        'IntEnabledTherProg',
        'DurationIntEnabledTher',
        'CarePersLocalId',
        'IntegratedSoftwareInd',
        'RecordNumber',
        'UniqueID_IDS205',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
        'Unique_ServiceRequestID',
        'Unique_CarePersonnelID_Local',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_ServiceRequestID': 'Referral.Unique_ServiceRequestID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
        'PathwayID': 'Referral.PathwayID',
    }


class IDS602LongTermCondition(OrderedViewDefinition):
    IncludedFields = [
        'ServiceRequestId',
        'FindSchemeInUse',
        'LongTermCondition',
        'RecordNumber',
        'UniqueID_IDS602',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
        'Unique_ServiceRequestID',
        'LongTermConditionCode_ICD10_Mapped',
        'LongTermConditionCode_ICD10_Master',
        'LongTermConditionDescription_ICD10_Master',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_ServiceRequestID': 'Referral.Unique_ServiceRequestID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
        'PathwayID': 'Referral.PathwayID',
    }


class IDS603PresentingComplaints(OrderedViewDefinition):
    IncludedFields = [
        'ServiceRequestId',
        'FindSchemeInUse',
        'PresComp',
        'PresCompCodSig',
        'PresCompDate',
        'RecordNumber',
        'UniqueID_IDS603',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
        'Unique_ServiceRequestID',
        'PresentingComplaintCode_ICD10_Mapped',
        'PresentingComplaintCode_ICD10_Master',
        'PresentingComplaintDescription_ICD10_Master',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_ServiceRequestID': 'Referral.Unique_ServiceRequestID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
        'PathwayID': 'Referral.PathwayID',
    }


class IDS606CodedScoreAssessmentRefer(OrderedViewDefinition):
    IncludedFields = [
        'ServiceRequestId',
        'CodedAssToolType',
        'PersScore',
        'AssToolCompDate',
        'AssToolCompTime',
        'RecordNumber',
        'UniqueID_IDS606',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
        'Unique_ServiceRequestID',
        'Age_AssessmentCompletion_Date',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_ServiceRequestID': 'Referral.Unique_ServiceRequestID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
        'PathwayID': 'Referral.PathwayID',
    }


class IDS607CodedScoreAssessmentAct(OrderedViewDefinition):
    IncludedFields = [
        'CareActId',
        'CodedAssToolType',
        'PersScore',
        'RecordNumber',
        'UniqueID_IDS607',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
        'Unique_ServiceRequestID',
        'Unique_CareContactID',
        'Unique_CareActivityID',
        'Age_AssessmentCompletion_Date',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_ServiceRequestID': 'Referral.Unique_ServiceRequestID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
        'PathwayID': 'Referral.PathwayID',
    }


class IDS803CareCluster(OrderedViewDefinition):
    IncludedFields = [
        'LocalPatientId',
        'AMHCareClustCodeFin',
        'StartDateCareClust',
        'StartTimeCareClust',
        'EndDateCareClust',
        'EndTimeCareClust',
        'RecordNumber',
        'UniqueID_IDS803',
        'OrgID_Provider',
        'Person_ID',
        'UniqueSubmissionID',
        'CareCluster_StartedInRP_Flag',
        'CareCluster_EndedInRP_Flag',
        'CareCluster_OpenAtRPEnd_Flag',
        'CareCluster_DayCount',
    ]

    Locations = {
        'RecordNumber': 'Referral.Patient.RecordNumber',
        'OrgID_Provider': 'Referral.Header.OrgIDProv as OrgID_Provider',
        'Person_ID': 'Referral.Patient.Person_ID',
        'UniqueSubmissionID': 'Referral.Header.UniqueSubmissionID',
        'Unique_MonthID': 'Referral.Header.Unique_MonthID',
    }


class IDS902CarePersQual(OrderedViewDefinition):
    IncludedFields = [
        'CarePersLocalId',
        'QualAttainLevelIAPT',
        'QualAwardedDate',
        'QualPlannedCompletionDate',
        'UniqueID_IDS902',
        'OrgID_Provider',
        'UniqueSubmissionID',
        'Unique_CarePersonnelID_Local',
    ]

    Locations = {
        'OrgID_Provider': 'Header.OrgIDProv as OrgID_Provider',
        'UniqueSubmissionID': 'Header.UniqueSubmissionID',
    }


