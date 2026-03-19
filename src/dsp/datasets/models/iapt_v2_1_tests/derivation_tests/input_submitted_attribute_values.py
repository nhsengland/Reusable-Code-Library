from datetime import datetime, date
from dsp.dam import current_record_version
from dsp.shared.constants import DS

meta_dict = {
    'DATASET_VERSION': '2.1',
    'EVENT_ID': '1:1',
    'EVENT_RECEIVED_TS': datetime(2018, 8, 1),
    'RECORD_INDEX': 0,
    'RECORD_VERSION': current_record_version(DS.IAPT_V2_1),
}

header_dict = {
    'DatSetVer': '2.1',
    'OrgIDProv': 'RBA',
    'OrgIDSubmit': 'RBA',
    'PrimSystemInUse': 'OLD SYSTEM',
    'ReportingPeriodStartDate': date(2020, 1, 1),
    'ReportingPeriodEndDate': date(2020, 1, 31),
    'DateTimeDatSetCreate': datetime(2019, 10, 3, 10, 47, 39),
    'RowNumber': 1,
}

gp_practice_registration_dict = {
    'LocalPatientId': 'LPI00000000000000001',
    'GMPCodeReg': 'A81004',
    'StartDateGMPRegistration': date(1948, 3, 31),
    'EndDateGMPRegistration': None,
    'OrgIDGPPrac': 'R1D',
    'Header': header_dict,
    'RowNumber': 1,
}

accomodation_status_dict = {
    'LocalPatientId': 'LPI00000000000000001',
    'AccommodationType' : '01',
    'SettledAccommodationInd': '1',
    'AccommodationTypeDate': date(2020, 1, 1),
    'AccommodationTypeStartDate': date(2019, 1, 1),
    'AccommodationTypeEndDate':''
}

employment_status_dict = {
    'LocalPatientId': 'LPI00000000000000001',
    'EmployStatus': '03',
    'EmployStatusRecDate': date(2007, 9, 5),
    'WeekHoursWorked': '03',
    'SelfEmployInd': '',
    'SickAbsenceInd': '',
    'SSPIND': '',
    'BENEFITRECIND': '',
    'JSASTATUS': '',
    'ESASTATUS': '',
    'UCSTATUS': '',
    'PIPSTATUS': '',
    'OTHERBENEFITIND': '',
    'EMPSUPPORTIND': '',
    'EMPSUPPORTREFERRAL': None,
    'RowNumber': 1,
    'EmpSupportDischargeDate':None
}

disability_type_dict = {
    'LocalPatientId': 'LPI00000000000000001',
    'DisabCode': '03',
    'RowNumber': 1,
}

social_and_personal_circumstances_dict = {
    'LocalPatientId': 'LPI00000000000000001',
    'SocPerCircumstance': '960321000000103',
    'SocPerCircumstanceRecDate': date(1982, 10, 27),
    'RowNumber': 1,
}

overseas_visitor_charging_category_dict = {
    'LocalPatientId': 'LPI00000000000000001',
    'OvsVisChCat': 'C',
    'OvsVisChCatAppDate': date(2011, 10, 6),
    'RowNumber': 1,
}

care_cluster_dict = {
    'LocalPatientId': 'LPI00000000000000001',
    'AMHCareClustCodeFin': '02',
    'StartDateCareClust': date(2015, 5, 7),
    'StartTimeCareClust': datetime(1970, 1, 1, 6, 8, 27),
    'EndDateCareClust': date(2015, 5, 28),
    'EndTimeCareClust': datetime(1970, 1, 1, 20, 24, 27),
    'RowNumber': 1,
}

master_patient_index_dict = {
    'EduEstabTypeIAPT': '',
    'EthnicCategory': 'C',
    'ExBAFIndicator': '',
    'Gender': '1',
    'GenderIdentitySameAtBirth':'Y',
    'LanguageCodePreferred': 'q3',
    'LocalPatientId': 'LPI00000000000000001',
    'NHSNumber': '9018978914',
    'NHSNumberStatus': '03',
    'OrgIDLocalPatientId': 'RAL',
    'OrgIDResidenceResp': 'R1A',
    'PersonBirthDate': date(1898, 8, 28),
    'Postcode': 'B10  0AU',
    'GPPracticeRegistrations': [
        gp_practice_registration_dict
    ],
    'EmploymentStatuses': [
        employment_status_dict
    ],
    'DisabilityTypes': [
        disability_type_dict
    ],
    'SocialAndPersonalCircumstances': [
        social_and_personal_circumstances_dict
    ],
    'OverseasVisitorChargingCategories': [
        overseas_visitor_charging_category_dict
    ],
    'CareClusters': [
        care_cluster_dict
    ],
    'RowNumber': 1,
}

onward_referral_dict = {
    'ServiceRequestId': 'SRI00000000000000001',
    'OnwardReferDate': date(2015, 5, 27),
    'OnwardReferTime': datetime(1970, 1, 1, 2, 23, 11),
    'OnwardReferReason': '03',
    'OrgIDReceiving': 'R1D',
    'RowNumber': 1,
}

waiting_time_pauses_dict = {
    'PAUSEID': '',
    'ServiceRequestId': 'SRI00000000000000001',
    'PAUSESTARTDATE': date(2015, 5, 27),
    'PAUSEENDDATE': date(2015, 6, 27),
    'PAUSEREASON': '01',
    'RowNumber': 1,
}

coded_scored_assessment_care_activity_dict = {
    'CareActId': 'CAI00000000000000001',
    'CodedAssToolType': '960321000000103',
    'PersScore': '6.6',
    'AssToolCompDate': date(2020, 1, 1),
    'AssToolCompTime': datetime(2020, 1, 1, 15, 32, 5),
    'RowNumber': 1,
}

care_activity_dict = {
    'CareActId': 'CAI00000000000000001',
    'CareContactId': 'CCI00000000000000001',
    'CarePersLocalId': 'CPL00000000000000001',
    'ClinContactDurOfCareAct': 1,
    'CodeProcAndProcStatus': '960321000000103',
    'FindSchemeInUse': '03',
    'CodeFind': 'Qyu..',
    'CodeObs': '413737006',
    'ObsValue': 'L4YI3CW5GW',
    'UnitMeasure': 'KS2NY3NN',
    'CodedScoredAssessments': [
        coded_scored_assessment_care_activity_dict
    ],
    'RowNumber': 1,
}

care_contact_dict = {
    'CareContactId': 'CCI00000000000000001',
    'ServiceRequestId': 'SRI00000000000000001',
    'CareContDate': date(2015, 5, 19),
    'CareContTime': datetime(1970, 1, 1, 20, 9, 10),
    'OrgIDComm': '11H',
    'PlannedCareContIndicator': 'Y',
    'AttendOrDNACode': '7',
    'CANCELLATION': 'Y',
    'ClinContDurOfCareCont': 8578,
    'IAPTLTCServiceInd':'Y',
    'APPTYPE': '02',
    'ConsMechanism': '03',
    'IntEnabledTherPrg': '',
    'CareContPatientTherMode': '1',
    'NumGroupTherParticipants': 10,
    'NumGroupTherFacilitators': 5,
    'PSYCHMED': '01',
    'ActLocTypeCode': 'A03',
    'SiteIDOfTreat': 'R1A03',
    'LanguageCodeTreat': 'q1',
    'InterpreterPresentInd': '1',
    'CareActivities': [
        care_activity_dict
    ],
    'RowNumber': 1,
}

internet_therapy_log_dict = {
    'ServiceRequestId': 'SRI00000000000000001',
    'StartDateIntEnabledTherLog': date(2019, 5, 17),
    'EndDateIntEnabledTherLog': date(2019, 6, 17),
    'IntEnabledTherLog': '',
    'DurationIntEnabledTher': 10,
    'CarePersLocalId': 'CPL00000000000000001',
    'IntegratedSoftwareInd': 'Y',
    'RowNumber': 1,
}

long_term_condition_dict = {
    'ServiceRequestId': 'SRI00000000000000001',
    'FindSchemeInUse': '01',
    'LongTermCondition': 'TEST',
    'RowNumber': 1,
}

presenting_complaints_list = [
    {
        'ServiceRequestId': 'SRI00000000000000001',
        'FindSchemeInUse': '01',
        'PresComp': 'TEST',
        'PresCompCodSig': '1',
        'PresCompDate': None,
        'RowNumber': 1,
    },
    {
        'ServiceRequestId': 'SRI00000000000000001',
        'FindSchemeInUse': '01',
        'PresComp': 'F40',
        'PresCompCodSig': '1',
        'PresCompDate': date(2019, 5, 13),
        'RowNumber': 2,
    },
    {
        'ServiceRequestId': 'SRI00000000000000001',
        'FindSchemeInUse': '01',
        'PresComp': 'F32',
        'PresCompCodSig': '1',
        'PresCompDate': date(2019, 5, 16),
        'RowNumber': 3,
    },
    {
        'ServiceRequestId': 'SRI00000000000000001',
        'FindSchemeInUse': '01',
        'PresComp': 'F32',
        'PresCompCodSig': '1',
        'PresCompDate': None,
        'RowNumber': 4,
    }
]

coded_score_dassessment_dict = {
    'ServiceRequestId': 'SRI00000000000000001',
    'CodedAssToolType': '960321000000103',
    'PersScore': '6.6',
    'AssToolCompDate': date(2015, 5, 7),
    'AssToolCompTime': datetime(1970, 1, 1, 10, 59, 40),
    'RowNumber': 1,
}

care_personnel_qualification_dict = {
    'CarePersLocalId': 'CPL00000000000000001',
    'QualAttainLevelIAPT': '10',
    'QualAwardedDate': date(2019, 1, 1),
    'QualPlannedCompletionDate': date(2018, 11, 10),
    'META': meta_dict,
    'Header': header_dict,
    'RowNumber': 1,
}

referral_dict = {
    'LocalPatientId': 'LPI00000000000000001',
    'OrgIDComm': '08H',
    'ReferralRequestReceivedDate': date(2001, 7, 13),
    'ServiceRequestId': 'SRI00000000000000001',
    'SourceOfReferralIAPT': 'A3',
    'OrgID_CCG_Residence': '15E',
    'OrgID_CCG_GP': '00M',
    'ONSETDATE': '',
    'PrevDiagCondInd': '',
    'ENDCODE': '',
    'ServDischDate': date(2015, 5, 17),
    'Patient': master_patient_index_dict,
    'OnwardReferrals': [
        onward_referral_dict
    ],
    'WaitingTimePauses': [
        waiting_time_pauses_dict
    ],
    'CareContacts': [
        care_contact_dict
    ],
    'InternetTherapyLogs': [
        internet_therapy_log_dict
    ],
    'LongTermConditions': [
        long_term_condition_dict
    ],
    'PresentingComplaints':
        presenting_complaints_list,
    'CodedScoredAssessments': [
        coded_score_dassessment_dict
    ],
    'META': meta_dict,
    'Header': header_dict,
    'RowNumber': 1,
}
