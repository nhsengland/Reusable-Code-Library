import copy
from datetime import datetime, date
from decimal import Decimal

import pytest

from dsp.dam import current_record_version
from dsp.shared.constants import DS

meta_dict = {
    'DATASET_VERSION': '1',
    'EVENT_ID': '1:1',
    'EVENT_RECEIVED_TS': datetime(2020, 5, 1),
    'RECORD_INDEX': 0,
    'RECORD_VERSION': current_record_version(DS.EPMAWSPC2),
}

epmawspc2_dict = {
    'ODS': 'ODS1',
    'SPR': 'SPR1',
    'NHS': 9910231042,
    'Type': 'Outpatient',
    'Drug': 'Drug1',
    'MDDF': 'MDDF1',
    'DmdName': 'DmdName1',
    'DmdId': 'DmdId1',
    'PSCName': 'PSCName1',
    'PSCStrength': 'PSCStrength1',
    'PSCForm': 'PSCForm1',
    'OrderChangeDate': datetime(2020, 4, 14, 15, 5),
    'DoseStatus': 'DoseStatus1',
    'PriDose': Decimal(972.6099853515625),
    'PriDoseUnit': 'g',
    'SecDose': Decimal(192.08999633789062),
    'SecDoseUnit': 'tablet',
    'Route': 'Route1',
    'Frequency': '12 hours',
    'PRN': 'Y',
    'PRNReason': 'PRNReason1',
    'OriginalStartDate': date(2020, 4, 20),
    'StopDate': date(2020, 4, 30),
    'LinkType': 'E',
    'LinkFrom': 36,
    'LinkTo': 40,
    'OrderID': 17,
    'Version': 'Version1',
    'ADMLink': 'ADMLink1',
    'RunDate': '30/01/2020',
    'RunTime': '12:00:00',
    #   "NHSBeginsWith": True,
    # "NHSorCHINumber": 'Removed',
    'SourceDMD': 'Recorded by Trust',
    # 'TypeAbbr': 'I',
    # 'TempMddf': None,

    'META': meta_dict
}

epmawsad2_dict = {
    'ODS': 'ODS2',
    'SPR': 'SPR2',
    'ADMLink': 'ADMLink1',
    'NHS': 9910231042,
    'Drug': 'Drug1',
    'MDDF': 'MDDF1',
    'DmdId': 'DmdId1',
    'Route': 'Route1',
    'PRN': 'Y',
    'OrderID': 17,
    'ScheduledAdmin': datetime(2019, 10, 14, 12, 0),
    'GivenAdminDateTime': datetime(2019, 10, 14, 12, 5),
    'AdminReasonNotGiven': 'AdminReason1',
    'SubmissionDateTime': datetime(2019, 10, 14, 12, 5),
    'Source': 'Recorded by Trust',
    'RunDate': '30/01/2020',
    'RunTime': '12:00:00',
    #"NHSorCHINumber": 'Removed',
    #"NHSlrRemoved": '9910231042',
    #"NHSlrRemoved": 'Removed',
    # 'TypeAbbr': 'I',
    'META': meta_dict
}


def epmawspc2_test_data():
    return copy.deepcopy(epmawspc2_dict)


def epmawsad2_test_data():
    return copy.deepcopy(epmawsad2_dict)
