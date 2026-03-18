import copy
from datetime import datetime, date

import pytest

from dsp.dam import current_record_version
from dsp.shared.constants import DS


gp_practice_registration_derivations_dict = {
    'DistanceFromHome_GP': 234,
    'OrgID_CCG_GP': '00M',
    'LADistrictAuthGPPractice': "E06000002"
}

internet_therapy_derivations_dict = {
    "Unique_CarePersonnelID_Local": "RBACPL00000000000000001",
}

long_term_condition_derivations_dict = {
    "Validated_LongTermConditionCode": "-3",
    "UniqueID_IDS602": 1000000001,
}

presenting_complaints_derivations_list = [
    {
        "Validated_PresentingComplaint": "-3",
        "UniqueID_IDS603": 1000000001,
    },
    {
        "Validated_PresentingComplaint": "F409",
        "UniqueID_IDS603": 1000000002,
    },
    {
        "Validated_PresentingComplaint": "F329",
        "UniqueID_IDS603": 1000000003,
    },
    {
        "Validated_PresentingComplaint": "F329",
        "UniqueID_IDS603": 1000000004,
    },
]

coded_scored_assessment_referral_derivations_dict = {
    "UniqueID_IDS606": 1000000001,
    "Age_AssessmentCompletion_Date": 116,
}
coded_scored_assessment_activity_derivations_dict = {
    "UniqueID_IDS607": 1000000001,
    "Unique_CareContactID": "RBACCI00000000000000001",
    "Unique_CareActivityID": "RBACAI00000000000000001",
    "Age_AssessmentCompletion_Date": 116,
}

care_cluster_derivations_dict = {
    "UniqueID_IDS803": 1000000001,
    "CareCluster_StartedInRP_Flag": False,
    "CareCluster_EndedInRP_Flag": False,
    "CareCluster_OpenAtRPEnd_Flag": False,
    "CareCluster_DayCount": -1678,
}

care_activity_derivations_dict = {
    "CodedScoredAssessments": [coded_scored_assessment_activity_derivations_dict],
    "UniqueID_IDS202": 1000000001,
    "Unique_CareContactID": "RBACCI00000000000000001",
    "Unique_CareActivityID": "RBACAI00000000000000001",
    "Unique_CarePersonnelID_Local": "RBACPL00000000000000001",
    "Validated_FindingCode": "-3",
}

care_contact_derivations_dict = {
    "CareActivities": [care_activity_derivations_dict],
    "UniqueID_IDS201": 1000000001,
    "Unique_CareContactID": "RBACCI00000000000000001",
    "Time_Referral_to_CareContact": 5058,
    "Age_CareContact_Date": 116,
    "DistanceFromHome_ContactLocation": 3,
}

master_patient_index_derivations_dict = {
    'GPPracticeRegistrations': [
        gp_practice_registration_derivations_dict
    ],
    "CareClusters": [care_cluster_derivations_dict],

    "UniqueID_IDS001": 1000000001,
    "RecordNumber": 1000000001,
    "ValidPostcode_Flag": "Y",
    "OrgID_CCG_Residence": "15E",
    "MPSConfidence": None,
    "Age_RP_StartDate": 121,
    "Age_RP_EndDate": 121,
    "Postcode_District": "B10",
    "Postcode_Default": None,
    "LSOA": "E01009363",
    "Census_Year": 2011,
    "LocalAuthority": "E08000025",
    "County": "E99999999",
    "ElectoralWard": "E05011127",
    "IndicesOfDeprivationDecile": 1,
    "IndicesOfDeprivationQuartile": 1,
    "IMD_YEAR": 2019,
    "Validated_EthnicCategory": "C",
}

referral_derivations_dict = {
    'Patient': master_patient_index_derivations_dict,
    "CareContacts": [care_contact_derivations_dict],
    "InternetTherapyLogs": [internet_therapy_derivations_dict],
    "LongTermConditions": [long_term_condition_derivations_dict],
    "PresentingComplaints": presenting_complaints_derivations_list,
    "CodedScoredAssessments": [coded_scored_assessment_referral_derivations_dict],

    "RFAs": ['C:08H', 'G:00M', 'H:15E', 'P:RBA', 'S:RBA'],
    "UniqueID_IDS101": 1000000001,
    "Unique_MonthID": 1438,
    "Unique_ServiceRequestID": "RBASRI00000000000000001",
    "Age_ReferralRequest_ReceivedDate": 102,
    "Age_ServiceDischarge_Date": 116,
    "CommissioningRegion": "Y56",
}
