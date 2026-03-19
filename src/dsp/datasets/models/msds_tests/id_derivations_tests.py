import copy
from typing import Dict, Any, Type

import datetime
import pytest

from dsp.datasets.models.msds import PregnancyAndBookingDetails
from dsp.datasets.models.msds_tests.msds_helper_tests import pregnancy_and_booking_details


@pytest.mark.parametrize("rep_period_start_date, org_code_gmp_mother, expected", [
    (datetime.date(2013, 4, 1), 'A81001', '00K'), # valid before 20230401
    (datetime.date(2023, 4, 1), 'A81001', None), # valid after 20230401
    
 
])
def test_ccgresponsibilitymother (pregnancy_and_booking_details, rep_period_start_date, org_code_gmp_mother, expected):
    pregnancy_and_booking_details['Header']['RPStartDate'] = rep_period_start_date
    pregnancy_and_booking_details['Mother']['GPPracticeRegistrations'][0]['OrgCodeGMPMother'] = org_code_gmp_mother
    new_pregnancy_and_booking_details = PregnancyAndBookingDetails(pregnancy_and_booking_details)
    assert new_pregnancy_and_booking_details.Mother.GPPracticeRegistrations[0].CCGResponsibilityMother == expected