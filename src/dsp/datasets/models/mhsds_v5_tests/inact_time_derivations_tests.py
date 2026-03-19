import datetime

import pytest

from dsp.datasets.models.mhsds_v5_tests.mhsds_v5_helper_tests import referral

from dsp.datasets.models.mhsds_v5 import Referral


@pytest.mark.parametrize("rep_period_end_date, end_date, expected", [
    (None, None, None),
    (datetime.date(2017, 5, 1), None, datetime.date(2017, 5, 2)),
    (datetime.date(2017, 5, 20), None, datetime.date(2017, 5, 21)),
    (datetime.date(2017, 5, 1), datetime.date(2017, 3, 10), None),
])
def test_inacttime_care_cord(referral, rep_period_end_date, end_date, expected):
    referral['Header']['ReportingPeriodEndDate'] = rep_period_end_date
    referral['Patient']['MentalHealthCareCoordinators'][0]['EndDateAssCareCoord'] = end_date
    new_referral = Referral(referral)

    assert expected == new_referral.Patient.MentalHealthCareCoordinators[0].InactTimeCC


@pytest.mark.parametrize("rep_period_end_date, end_date, expected", [
    (None, None, None),
    (datetime.date(2017, 5, 1), None, datetime.date(2017, 5, 2)),
    (datetime.date(2017, 5, 20), None, datetime.date(2017, 5, 21)),
    (datetime.date(2017, 5, 1), datetime.date(2017, 3, 10), None),
])
def test_inacttime_ref(referral, rep_period_end_date, end_date, expected):
    referral['Header']['ReportingPeriodEndDate'] = rep_period_end_date
    referral['ServDischDate'] = end_date
    new_referral = Referral(referral)

    assert expected == new_referral.InactTimeRef


@pytest.mark.parametrize("rep_period_end_date, end_date, expected", [
    (None, None, None),
    (datetime.date(2017, 5, 1), None, datetime.date(2017, 5, 2)),
    (datetime.date(2017, 5, 20), None, datetime.date(2017, 5, 21)),
    (datetime.date(2017, 5, 1), datetime.date(2017, 3, 10), None),
])
def test_inacttime_st(referral, rep_period_end_date, end_date, expected):
    referral['Header']['ReportingPeriodEndDate'] = rep_period_end_date
    referral['ServiceTypesReferredTo'][0]['ReferClosureDate'] = end_date
    new_referral = Referral(referral)

    assert expected == new_referral.ServiceTypesReferredTo[0].InactTimeST


@pytest.mark.parametrize("rep_period_end_date, end_date, expected", [
    (None, None, None),
    (datetime.date(2017, 5, 1), None, datetime.date(2017, 5, 2)),
    (datetime.date(2017, 5, 20), None, datetime.date(2017, 5, 21)),
    (datetime.date(2017, 5, 1), datetime.date(2017, 3, 10), None),
])
def test_inacttime_hps(referral, rep_period_end_date, end_date, expected):
    referral['Header']['ReportingPeriodEndDate'] = rep_period_end_date
    referral['HospitalProviderSpells'][0]['DischDateHospProvSpell'] = end_date
    new_referral = Referral(referral)

    assert expected == new_referral.HospitalProviderSpells[0].InactTimeHPS


@pytest.mark.parametrize("rep_period_end_date, end_date, expected", [
    (None, None, None),
    (datetime.date(2017, 5, 1), None, datetime.date(2017, 5, 2)),
    (datetime.date(2017, 5, 20), None, datetime.date(2017, 5, 21)),
    (datetime.date(2017, 5, 1), datetime.date(2017, 3, 10), None),
])
def test_inacttime_ws(referral, rep_period_end_date, end_date, expected):
    referral['Header']['ReportingPeriodEndDate'] = rep_period_end_date
    referral['HospitalProviderSpells'][0]['WardStays'][0]['EndDateWardStay'] = end_date
    new_referral = Referral(referral)

    assert expected == new_referral.HospitalProviderSpells[0].WardStays[0].InactTimeWS


@pytest.mark.parametrize("rep_period_end_date, end_date, expected", [
    (None, None, None),
    (datetime.date(2017, 5, 1), None, datetime.date(2017, 5, 2)),
    (datetime.date(2017, 5, 20), None, datetime.date(2017, 5, 21)),
    (datetime.date(2017, 5, 1), datetime.date(2017, 3, 10), None),
])
def test_inacttime_care_cluster(referral, rep_period_end_date, end_date, expected):
    referral['Header']['ReportingPeriodEndDate'] = rep_period_end_date
    referral['Patient']['ClusteringToolAssessments'][0]['CareClusters'][0]['EndDateCareClust'] = end_date
    new_referral = Referral(referral)

    assert expected == new_referral.Patient.ClusteringToolAssessments[0].CareClusters[0].InactTimeCC


@pytest.mark.parametrize("rep_period_end_date, end_date, expected", [
    (None, None, None),
    (datetime.date(2017, 5, 1), None, datetime.date(2017, 5, 2)),
    (datetime.date(2017, 5, 20), None, datetime.date(2017, 5, 21)),
    (datetime.date(2017, 5, 1), datetime.date(2017, 3, 10), None),
])
def test_inacttime_mhs_period(referral, rep_period_end_date, end_date, expected):
    referral['Header']['ReportingPeriodEndDate'] = rep_period_end_date
    referral['Patient']['MentalHealthActLegalStatusClassificationAssignmentPeriods'][0]['EndDateMHActLegalStatusClass'] = end_date
    new_referral = Referral(referral)

    assert expected == new_referral.Patient.MentalHealthActLegalStatusClassificationAssignmentPeriods[0].InactTimeMHAPeriod
