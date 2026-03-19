import pytest
from dateutil.parser import parse

from dsp.udfs import age_band_5_years, age_band_10_years
from dsp.udfs.age_bands import _band_age, _date_as_float, age_in_years_at_event


@pytest.mark.parametrize("date, expected", [
    ('1988-02-28', 1988.1584699453551),
    ('1988-02-29', 1988.1612021857923),
    ('1988-03-01', 1988.1639344262296),
    ('2018-02-28', 2018.1584699453551),
])
def test__date_as_float(date, expected):
    assert expected == _date_as_float(parse(date))


@pytest.mark.parametrize("dob, diagnostic_test_date, expected", [
    (None, None, None),
    (None, '1989-2-28', None),
    ('1988-02-29', None, None),
    ('1988-02-29', '1989-2-28', 0),
    ('1988-02-29', '1989-03-01', 1),
    ('1988-02-29', '2018-02-28', 29),
    ('1988-02-29', '2018-03-01', 30),
])
def test_age_in_years_at_event(dob, diagnostic_test_date, expected):
    assert expected == age_in_years_at_event(dob, diagnostic_test_date)


def test_band_age_in_bottom_band():

    assert '0-18' == _band_age(2, [-1, 18, 60])


def test_band_age_in_zero_band():

    assert '0' == _band_age(0, [0, 18, 60])


def test_band_age_middle_band():

    assert '19-60' == _band_age(45, [-1, 18, 60])


def test_band_age_above_ceil():

    assert '61 and over' == _band_age(61, [-1, 18, 60])


def test_band_no_age():

    assert 'Age not known' == _band_age(None, [-1, 18])


def test_band_neg_age():

    assert 'Age not known' == _band_age(-1, [-1, 18])


def test_age_band_5_years_zero():

    assert '0-4' == age_band_5_years(0)


def test_age_band_5_years_middle():

    assert '50-54' == age_band_5_years(51)


def test_age_band_5_years_over_90():

    assert '90 and over' == age_band_5_years(90)


def test_age_band_5_years_not_known():

    assert 'Age not known' == age_band_5_years(None)


def test_age_band_10_years_zero():

    assert '0-9' == age_band_10_years(0)


def test_age_band_10_years_middle():

    assert '50-59' == age_band_10_years(51)


def test_age_band_10_years_over_90():

    assert '90 and over' == age_band_10_years(90)


def test_age_band_10_years_not_known():

    assert 'Age not known' == age_band_10_years(None)