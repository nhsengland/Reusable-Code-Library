from datetime import datetime, date

import pytest

# this import is to make pytest fixture available
from dsp.datasets.models.msds import MotherDemog
from dsp.datasets.models.msds import PregnancyAndBookingDetails
from dsp.datasets.models.msds_tests.msds_helper_tests import pregnancy_and_booking_details


@pytest.fixture(scope='function')
def motherdemog():
    yield {
        'PersonDeathTimeMother': None,
        'PersonDeathDateMother': None,
    }


@pytest.mark.parametrize("deathdate, expected", [
    (None, None),
    (date(2018, 12, 6), 2018),
])
def test_extract_year(motherdemog, deathdate, expected):
    motherdemog['PersonDeathDateMother'] = deathdate
    new_demog = MotherDemog(motherdemog)
    assert expected == new_demog.YearOfDeathMother


@pytest.mark.parametrize("deathdate, expected", [
    (None, None),
    (date(2018, 12, 6), "12"),
    (date(2018, 5, 6),  "05"),
])
def test_extract_month(motherdemog, deathdate, expected):
    motherdemog['PersonDeathDateMother'] = deathdate
    new_demog = MotherDemog(motherdemog)
    assert expected == new_demog.MonthOfDeathMother


@pytest.mark.parametrize("deathdate, expected", [
    (None, None),
    (date(2018, 12, 6), "04"),  # Thursday
])
def test_extract_day_of_week(motherdemog, deathdate, expected):
    motherdemog['PersonDeathDateMother'] = deathdate
    new_demog = MotherDemog(motherdemog)
    assert expected == new_demog.DayOfWeekOfDeathMother


@pytest.mark.parametrize("deathtime, expected", [
    (None, None),
    (datetime(1970, 1, 1,  0,  0,  0), "01"),
    (datetime(1970, 1, 1,  9,  3,  6), "01"),
    (datetime(1970, 1, 1, 11, 59, 59), "01"),
    (datetime(1970, 1, 1, 12,  0,  0), "02"),
    (datetime(1970, 1, 1, 23, 59, 59), "02"),
])
def test_extract_meridian(motherdemog, deathtime, expected):
    motherdemog['PersonDeathTimeMother'] = deathtime
    new_demog = MotherDemog(motherdemog)
    assert expected == new_demog.MeridianOfDeathMother

@pytest.mark.parametrize("birthdate, reporting_end, expected", [
    (None, None, None),
    (date(1992, 12, 6), date(2019, 11, 30), 26),
    (date(1992, 11, 6), date(2019, 11, 30), 27),
])
def test_derive_years_old_at_end_of_reporting_period(
    pregnancy_and_booking_details, birthdate, reporting_end, expected
):
    data = pregnancy_and_booking_details
    data["Header"]["RPEndDate"] = reporting_end
    data["Mother"]["PersonBirthDateMother"] = birthdate
    assert PregnancyAndBookingDetails(data).Mother.AgeRPEndDate == expected
