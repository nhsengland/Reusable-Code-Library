import datetime
import pytest

from dsp.udfs.postcodes import icb_at, msoa_at, lsoa_at, ccg_at, active_at, distance_between_postcodes, is_pseudo_country_postcode


@pytest.mark.parametrize("postcode, ts, expected", [
    ('LS14JL', datetime.date(2018, 10, 3), 'E01033015'),
    ('LS1 4JL', datetime.date(2018, 10, 3), 'E01033015'),
    (None, datetime.date(2018, 10, 3), None),
    ('ZZ99 1AB', datetime.date(2018, 10, 3), None),
    ('LS1J', datetime.date(2018, 10, 3), None),
    ('WV99 2BT', datetime.date(2001, 10, 3), None),
    ('WV99 2BT', datetime.date(2018, 10, 3), 'E01014192'),
])
def test_lsoa_at(postcode, ts, expected):
    assert expected == lsoa_at(postcode, ts)


@pytest.mark.parametrize("postcode, ts, expected", [
    ('LS14JL', datetime.date(2018, 10, 3), 'E02006875'),
    ('LS1 4JL', datetime.date(2018, 10, 3), 'E02006875'),
    (None, datetime.date(2018, 10, 3), None),
    ('ZZ99 1AB', datetime.date(2018, 10, 3), None),
    ('LS1J', datetime.date(2018, 10, 3), None),
    ('WV99 2BT', datetime.date(2001, 10, 3), None),
    ('WV99 2BT', datetime.date(2018, 10, 3), 'E02002945'),
])
def test_msoa_at(postcode, ts, expected):
    assert expected == msoa_at(postcode, ts)


def test_can_load_ccg_at():
    assert ccg_at('LS14JL', datetime.date(2018, 10, 3)) is not None


@pytest.mark.parametrize("postcode, ts, expected", [
    ('LS14JL', datetime.date(2018, 10, 3), True),
    ('LS1 4JL', datetime.date(2018, 10, 3), True),
    ('LS999ZZ', datetime.date(2018, 10, 3), False),
    ('TS26 0ZX', datetime.date(2017, 8, 1), False),
    ('ZZ99 3WZ', datetime.date(2017, 8, 1), True),
    ('LS18 8RT', datetime.date(2017, 8, 1), False),
    ('LS188', datetime.date(2017, 8, 1), False),
])
def test_active_at(postcode, ts, expected):
    assert expected == active_at(postcode, ts)


@pytest.mark.parametrize("postcode, ts, expected", [
    ('LS166EH', datetime.date(2020, 5, 31), 'E54000005'),
    ('LS16 6EH', datetime.date(2020, 6, 1), 'QWO'),
    (None, datetime.date(2021, 10, 3), None),
    ('ZZ99 1AB', datetime.date(2021, 10, 3), None),
    ('WV99 2BT', datetime.date(2021, 11, 1), 'QOC'),
])
def test_icb_at(postcode, ts, expected):
    assert expected == icb_at(postcode, ts)


@pytest.mark.parametrize("from_postcode, to_postcode, ts, expected", [
    ('TS181HU', 'AL100AU', datetime.date(2018, 10, 3), 319572),
    ('TS182AW', 'E105NP', datetime.date(2018, 10, 3), 346256)
])
def test_distance_between_postcodes(from_postcode, to_postcode, ts, expected):
    assert expected == round(distance_between_postcodes(from_postcode, to_postcode, ts))


@pytest.mark.parametrize("postcode, mock_active_at, expected", [
    ('LS14JL', None, False),
    ('LS1 4JL', None, False),
    ('ZZ99 3WZ', True, True),
    ('ZZ99 3WX', False, False),
    ('', None, False),
    ('ZZ98', None, False),
    (None, None, False),
    (False, None, False)
])
def test_is_pseudo_country_postcode(mocker: object, postcode: object, mock_active_at: object, expected: object) -> object:
    with mocker.patch('dsp.udfs.postcodes.active_at', return_value=mock_active_at):
        assert expected == is_pseudo_country_postcode(postcode)
