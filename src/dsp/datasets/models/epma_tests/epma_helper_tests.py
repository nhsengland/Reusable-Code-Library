import copy
from datetime import datetime, date

import pytest

from dsp.dam import current_record_version
from dsp.shared.constants import DS

meta_dict = {
    'DATASET_VERSION': '1',
    'EVENT_ID': '1:1',
    'EVENT_RECEIVED_TS': datetime(2020, 5, 1),
    'RECORD_INDEX': 0,
    'RECORD_VERSION': current_record_version(DS.EPMAWSPC),
}

epmawspc_dict = {
    'ODS': 'ODS1',
    'SPR': 'SPR1',
    'NHS': 9910231042,
    'Type': 'Inpatient',
    'Drug': 'Drug1',
    'MDDF': 'MDDF1',
    'DmdName': 'DmdName1',
    'DmdId': 'DmdId1',
    'PSCName': 'PSCName1',
    'PSCStrength': 'PSCStrength1',
    'PSCForm': 'PSCForm1',
    'OrderChangeDate': datetime(2020, 4, 14, 15, 5),
    'DoseStatus': 'DoseStatus1',
    'PriDose': 972.6099853515625,
    'PriDoseUnit': 'g',
    'SecDose': 192.08999633789062,
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

    'META': meta_dict
}


def epmawspc_test_data():
    return copy.deepcopy(epmawspc_dict)
