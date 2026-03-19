from datetime import date, datetime
from typing import Tuple

import pytest
from dateutil.relativedelta import relativedelta

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars import submission_calendar
from dsp.shared.submissions.calendars.common import CalendarException

_mhsds_ytd_calendar = submission_calendar(DS.MHSDS_YTD, {})


@pytest.mark.parametrize(
    "reporting_period_start, as_at, expected_result, expected_error", [
        (date(2019, 1, 1), datetime(2019, 3, 22), date(2019, 2, 22), None),
        (date(2019, 5, 1), datetime(2019, 7, 5), date(2019, 7, 4), None),
        (date(2019, 5, 1), datetime(2019, 8, 1), date(2019, 7, 21), None),
        (date(2019, 5, 1), datetime(2025, 1, 1), date(2020, 5, 21), None),
        (date(2019, 8, 1), datetime(2020, 4, 1), date(2020, 3, 19), None),
        (date(2020, 3, 1), datetime(2020, 5, 1), date(2020, 4, 22), None),
        (date(2020, 3, 1), datetime(2020, 6, 1), date(2020, 5, 21), None),
        (date(2020, 3, 1), datetime(2020, 12, 1), date(2020, 5, 21), None),
        (date(2020, 4, 1), datetime(2020, 5, 30), date(2020, 5, 21), None),
        (date(2019, 5, 1), datetime(2023, 1, 1), date(2020, 5, 21), None),
        (date(2019, 6, 1), datetime(2023, 1, 1), date(2020, 5, 21), None),
        (date(2019, 5, 1), datetime(2019, 6, 1), None, 'MHSDS no submission window for 2019-05-01 as at 2019-06-01'),
        (date(2019, 5, 1), datetime(2019, 7, 1), None, 'MHSDS no submission window for 2019-05-01 as at 2019-07-01'),
    ]
)
def test_mhsds_snap_to_completed_window(
        reporting_period_start: datetime, as_at: datetime, expected_result: str, expected_error: str
):
    actual_error = None
    actual_result = None

    try:
        actual_result = _mhsds_ytd_calendar.find_last_closed_submission_window_for_rp(
            reporting_period_start, as_at, fake_historic_windows=False
        ).closes
    except Exception as e:
        actual_error = str(e)

    assert actual_error == expected_error
    assert actual_result == expected_result


@pytest.mark.parametrize(
    "reporting_period_start, as_at, expected_result, expected_error", [
        (date(2019, 1, 1), datetime(2019, 2, 22), 'Primary', None),
        (date(2019, 5, 1), datetime(2019, 7, 4), 'Primary', None),
        (date(2019, 5, 1), datetime(2019, 7, 5), 'Refresh', None),
        (date(2019, 5, 1), datetime(2019, 8, 1), 'Refresh', None),
        (date(2019, 5, 1), datetime(2025, 1, 1), 'Refresh', None),
        (date(2019, 5, 1), datetime(2019, 5, 31), None,
         "the as at date cannot precede the primary reporting period opens, as no submission windows can be selected."),
        (date(2019, 5, 1), datetime(2019, 6, 1), "Primary", None),
        (date(2019, 5, 1), datetime(2019, 7, 1), 'Primary', None),
    ]
)
def test_get_submission_window_type(
        reporting_period_start: datetime, as_at: datetime, expected_result: str, expected_error: str
):
    actual_error = None
    actual_result = None

    try:
        actual_result = _mhsds_ytd_calendar.get_submission_window_type(reporting_period_start, as_at,
                                                                       fake_historic_windows=False, )
    except Exception as e:
        actual_error = str(e)

    assert actual_error == expected_error
    assert actual_result == expected_result


@pytest.mark.parametrize(
    "reporting_period_start, as_at, expected_result, expected_error", [
        (date(1970, 1, 1), datetime(2019, 1, 22), None, "unable to find submission windows for MHSDS 1970-01-01"),
        (date(2018, 4, 1), datetime(2019, 1, 22), 1, None),
        (date(2019, 1, 1), datetime(2019, 2, 22), 1, None),
        (date(2019, 5, 1), datetime(2019, 7, 4), 1, None),
        (date(2019, 5, 1), datetime(2019, 7, 5), 2, None),
        (date(2019, 5, 1), datetime(2019, 8, 1), 3, None),
        (date(2019, 5, 1), datetime(2019, 5, 31), 1, None),
        (date(2019, 5, 1), datetime(2019, 6, 1), 1, None),
        (date(2019, 5, 1), datetime(2019, 7, 1), 1, None),
        (date(2019, 5, 1), datetime(2020, 3, 27), 10, None),
        (date(2019, 5, 1), datetime(2020, 7, 1), 12, None),
        (date(2020, 5, 1), datetime(2020, 6, 19), 1, None),
        (date(2020, 6, 1), datetime(2020, 6, 9), 1, None),
        (date(2020, 6, 1), datetime(2020, 7, 29), 2, None),
        (date(2020, 8, 1), datetime(2021, 3, 1), 7, None),
        (date(2020, 5, 1), datetime(2020, 7, 1), 2, None),
        (date(2021, 3, 1), datetime(2021, 4, 15), 1, None),
    ]
)
def test_get_submission_num_months_since_ytd_rp_start(
        reporting_period_start: datetime, as_at: datetime, expected_result: str, expected_error: str
):
    actual_error = None
    actual_result = None

    try:
        actual_result = _mhsds_ytd_calendar.get_submission_num_sws_since_ytd_rp_start(reporting_period_start, as_at,
                                                                                      fake_historic_windows=False)
    except Exception as e:
        actual_error = str(e)

    assert expected_error == actual_error
    assert expected_result == actual_result


@pytest.mark.parametrize(
    'reporting_period_start, fake_historic_windows, found, submission_window_opens, submission_window_closes, '
    'months_offset',
    [
        # found
        [date(2000, 1, 1), True, True, date(2000, 1, 21), date(2000, 2, 20), 0],
        [date(2018, 11, 1), True, True, date(2018, 11, 21), date(2018, 12, 20), 0],
        # not found, because feature is off
        [date(2000, 1, 1), False, False, None, None, 0],
        # not found, because reporting_period_start date is not on the 1st of the month
        [date(2000, 1, 15), True, False, None, None, 0],
        # offset testing
        [date(2019, 1, 1), True, True, date(2019, 3, 4), date(2019, 3, 22), 1],
        [date(2019, 1, 1), True, True, date(2019, 5, 2), date(2019, 5, 23), 3],
        [date(2019, 1, 1), True, False, None, None, 4],
        [date(2019, 3, 1), True, True, date(2019, 5, 2), date(2019, 5, 23), 1],
        [date(2019, 3, 1), True, False, None, None, 2],
        [date(2019, 4, 1), True, True, date(2020, 4, 23), date(2020, 5, 21), 12],
        [date(2019, 4, 1), True, False, None, None, 13],

    ]
)
def test_mhsds_submission_window_historical(
        reporting_period_start: date,
        fake_historic_windows: bool,
        found: bool,
        submission_window_opens: date,
        submission_window_closes: date,
        months_offset: int
):
    if not found:
        with pytest.raises(CalendarException):
            _mhsds_ytd_calendar.find_submission_window(reporting_period_start,
                                                       fake_historic_windows=fake_historic_windows,
                                                       months_offset=months_offset)
        return

    submission_window = _mhsds_ytd_calendar.find_submission_window(
        reporting_period_start, fake_historic_windows=fake_historic_windows,
        months_offset=months_offset
    )
    assert submission_window.opens == submission_window_opens
    assert submission_window.closes == submission_window_closes


@pytest.mark.parametrize(
    "as_at_date, expected_window_opens, expected_window_closes, expected_error, fake_historic_windows",
    [
        (datetime(2018, 1, 2, 0, 0, 0), None, None,
         "MHSDS no closed submission window found for as_at_date=2018-01-02", False),
        (datetime(2019, 1, 23, 0, 0, 1), date(2019, 1, 2), date(2019, 1, 22), None, False),
        (datetime(2019, 2, 23, 0, 0, 0), date(2019, 1, 30), date(2019, 2, 22), None, False),
        (datetime(2019, 3, 22, 23, 59, 59, 999999), date(2019, 1, 30), date(2019, 2, 22), None, False),
        (datetime(2019, 3, 10, 0, 0, 1), date(2019, 1, 30), date(2019, 2, 22), None, False),
        (datetime(2014, 6, 25, 23, 59, 59), date(2014, 5, 21), date(2014, 6, 20), None, True),
        (datetime(2014, 6, 20, 23, 59, 59), date(2014, 4, 21), date(2014, 5, 20), None, True),
        (datetime(2020, 1, 23, 0, 0, 0), date(2020, 1, 1), date(2020, 1, 21), None, True),
    ])
def test_mhsds_last_submission_window_at_date(
        as_at_date, expected_window_opens, expected_window_closes, expected_error, fake_historic_windows
):
    actual_error = None
    submission_window = None
    try:
        submission_window = _mhsds_ytd_calendar.find_last_closed_submission_window(
            as_at_date, fake_historic_windows=fake_historic_windows,
        )
    except CalendarException as e:
        actual_error = str(e)

    assert expected_error == actual_error

    if not actual_error:
        assert expected_window_opens == submission_window.opens
        assert expected_window_closes == submission_window.closes


@pytest.mark.parametrize(
    ['submission_datetime', 'reporting_period_start_date', 'expected'],
    [
        # Reject - just before primary reporting window open
        (datetime(2019, 5, 1, 23, 59, 59), date(2019, 4, 1), False),
        # Accept - on primary reporting window open
        (datetime(2019, 5, 2, 0, 0, 0), date(2019, 4, 1), True),
        # Accept - comfortably within primary reporting window
        (datetime(2019, 5, 23, 11, 17), date(2019, 4, 1), True),
        # Accept - just before primary reporting window close
        (datetime(2019, 5, 23, 23, 59, 59), date(2019, 4, 1), True),
        # Reject - on primary reporting window close
        (datetime(2019, 5, 24, 0, 0, 0), date(2019, 4, 1), False),
        # Reject - just before first refresh reporting window open
        (datetime(2019, 5, 31, 23, 59, 59), date(2019, 4, 1), False),
        # Accept - on first refresh reporting window open
        (datetime(2019, 6, 1, 0, 0, 0), date(2019, 4, 1), True),
        # Accept - just before first refresh reporting window close
        (datetime(2019, 6, 20, 23, 59, 59), date(2019, 4, 1), True),
        (datetime(2019, 7, 4, 0, 0, 0), date(2019, 4, 1), True),
        # Reject - on first refresh reporting window close
        (datetime(2019, 7, 5, 0, 0, 0), date(2019, 5, 1), True),
        # Accept - YTD APRIL RP should be submittable in July SW of same year
        (datetime(2019, 7, 5, 0, 0, 0), date(2019, 4, 1), True),
        # Accept - YTD - APRIL RP should be submittable in May SW of next year
        (datetime(2020, 5, 5, 0, 0, 0), date(2019, 4, 1), True),
        # Reject - YTD - APRIL RP should not be submittable in June SW of next year
        (datetime(2020, 6, 5, 0, 0, 0), date(2019, 4, 1), False),

    ]
)
def test_submitted_within_submission_window(submission_datetime: datetime, reporting_period_start_date: date,
                                            expected: bool):
    assert _mhsds_ytd_calendar.submitted_within_submission_window(
        submission_datetime, reporting_period_start_date, fake_historic_windows=False,
    ) == expected


def test_find_submission_window_by_submission_date_historic_windows_allowed():
    window = _mhsds_ytd_calendar.find_submission_window_by_submission_date(date(2001, 4, 15),
                                                                           fake_historic_windows=True)

    assert window
    assert window.opens == date(2001, 3, 21)
    assert window.closes == date(2001, 4, 20)
    assert window.reporting_periods[-1].start == date(2001, 3, 1)
    assert window.reporting_periods[-2].start == date(2001, 2, 1)


_mhsds_submission_windows = _mhsds_ytd_calendar.submission_windows


@pytest.mark.parametrize(
    "submission_date, fake_historic_windows, snap_to_last_open_window, expect_raises, expect_refresh_rp_start_date",
    [
        (date(2001, 1, 1), False, False, True, None),
        (date(2001, 1, 1), False, True, True, None),
        (date(2001, 4, 15), True, False, False, date(2001, 2, 1)),
        (date(2019, 1, 1), False, True, True, None),
        (date(2019, 1, 1), False, False, True, None),
        (_mhsds_submission_windows[-1].closes + relativedelta(days=1), False, False, True, None),
        (_mhsds_submission_windows[-1].closes + relativedelta(days=1), False, True, True, None),
        (_mhsds_submission_windows[-2].closes + relativedelta(days=1), False, False, False,
         _mhsds_submission_windows[-2].reporting_periods[-1].start),
        (_mhsds_submission_windows[-2].closes + relativedelta(days=1), False, True, False,
         _mhsds_submission_windows[-2].reporting_periods[-1].start),
        (date(2019, 5, 23), False, False, False, date(2019, 3, 1)),
        (date(2019, 5, 24), False, False, True, None),
        (date(2019, 5, 24), False, True, False, date(2019, 3, 1)),
    ])
def test_find_submission_window_by_submission_date(
        submission_date: date, fake_historic_windows: bool, snap_to_last_open_window: bool,
        expect_raises: bool, expect_refresh_rp_start_date: date
):
    if expect_raises:
        with pytest.raises(CalendarException) as ex:
            _mhsds_ytd_calendar.find_submission_window_by_submission_date(
                submission_date, fake_historic_windows=fake_historic_windows,
                snap_to_last_open_window=snap_to_last_open_window
            )
        assert ex
        return

    window = _mhsds_ytd_calendar.find_submission_window_by_submission_date(
        submission_date, fake_historic_windows=fake_historic_windows, snap_to_last_open_window=snap_to_last_open_window
    )

    assert window
    assert window.reporting_periods[-2].start == expect_refresh_rp_start_date


def test_ensure_submission_windows_are_in_correct_order():
    last = None
    last_refresh_rp = None

    for window in _mhsds_ytd_calendar.submission_windows:
        refresh_rp = window.reporting_periods[0]
        primary_rp = window.reporting_periods[1]
        assert refresh_rp.start.day == 1
        assert primary_rp.start.day == 1
        assert primary_rp.start + relativedelta(months=-1) == refresh_rp.start

        if not last:
            continue

        assert window.opens > last.opens
        assert window.closes > last.closes
        assert refresh_rp.start > last_refresh_rp.start

        last = window
        last_refresh_rp = last_refresh_rp


@pytest.mark.parametrize('as_at', [
    (20190223),
    (20190222),
    (20201212)
])
def test_get_refresh_rp_start_and_unique_month_id_as_at(as_at):
    with pytest.raises(CalendarException) as ex:
        _mhsds_ytd_calendar.get_refresh_unique_month_id_and_rp_start_as_at(
            as_at, fake_historic_windows=False
        )
    assert ex
    assert ex.value.args[0] == 'Calendar for MHSDS is YTD, multiple refresh windows available'


@pytest.mark.parametrize("as_at, expected_result", [
    # After May 2019 SW close, earliest should be Apr of previous year
    (datetime(2019, 5, 30), (1417, date(2018, 4, 1))),
    # After June 2019 SW close, earliest should be Apr of the same year
    (datetime(2019, 7, 5), (1429, date(2019, 4, 1))),
    # After July 2019 SW close, earliest should be Apr of the same year
    (datetime(2019, 7, 30), (1429, date(2019, 4, 1))),
    # After Aug  2019 SW close, earliest should be Apr of the same year
    (datetime(2019, 8, 30), (1429, date(2019, 4, 1))),
    # After Jan  2020 SW close, earliest should be Apr of the previous year year
    (datetime(2020, 1, 30), (1429, date(2019, 4, 1))),

])
def test_get_earliest_unique_month_id_and_rp_start_as_at(as_at: datetime,
                                                         expected_result: Tuple[int, date]):
    actual_result = _mhsds_ytd_calendar.get_earliest_unique_month_id_and_rp_start_as_at(as_at,
                                                                                        fake_historic_windows=False)
    assert actual_result == expected_result


@pytest.mark.parametrize("as_at, expected_result, fake_historic_windows", [
    (datetime(2019, 5, 30), (1429, date(2019, 4, 1)), False),
    (datetime(2019, 7, 5), (1430, date(2019, 5, 1)), False),
    (datetime(2019, 7, 30), (1431, date(2019, 6, 1)), False),
    (datetime(2019, 8, 30), (1432, date(2019, 7, 1)), False),
    (datetime(2020, 1, 30), (1437, date(2019, 12, 1)), False),
    (datetime(2019, 4, 1), (1427, date(2019, 2, 1)), False),
    (datetime(2019, 5, 2), (1428, date(2019, 3, 1)), False),
    (datetime(2019, 6, 1), (1429, date(2019, 4, 1)), False),
    (datetime(2019, 2, 23), (1426, date(2019, 1, 1)), False),
    (datetime(2018, 4, 1), (1415, date(2018, 2, 1)), True),
    (datetime(2018, 4, 23), (1416, date(2018, 3, 1)), True),
])
def test_get_latest_unique_month_id_and_rp_start_as_at(as_at: datetime,
                                                       expected_result: Tuple[int, date],
                                                       fake_historic_windows: bool):
    res = _mhsds_ytd_calendar.get_latest_unique_month_id_and_rp_start_as_at(as_at,
                                                                            fake_historic_windows=fake_historic_windows)
    assert res == expected_result


@pytest.mark.parametrize("as_at, expected_result", [
    # After May 2019 SW close, primary should be Apr of same year
    (datetime(2019, 5, 30), (1429, date(2019, 4, 1))),
    # After June 2019 SW close, primary should be May of the same year
    (datetime(2019, 7, 5), (1430, date(2019, 5, 1))),
    # After July 2019 SW close, primary should be Jun of the same year
    (datetime(2019, 7, 30), (1431, date(2019, 6, 1))),
    # After Aug  2019 SW close, primary should be July of the same year
    (datetime(2019, 8, 30), (1432, date(2019, 7, 1))),
    # After Jan  2020 SW close, primary should be Dec of the previous year
    (datetime(2020, 1, 30), (1437, date(2019, 12, 1))),
])
def test_get_primary_unique_month_id_and_rp_start_as_at(as_at: datetime,
                                                        expected_result: Tuple[int, date]):
    actual_result = _mhsds_ytd_calendar.get_primary_unique_month_id_and_rp_start_as_at(as_at,
                                                                                       fake_historic_windows=False)
    assert actual_result == expected_result


def test_no_overlap_between_rps_and_sws():
    for reporting_period, submission_windows in _mhsds_ytd_calendar.rp_start_maps.items():
        changed_sw_count = 0
        for submission_window in submission_windows:
            assert (reporting_period.day != submission_window.opens.day or
                    reporting_period.month != submission_window.opens.month or
                    reporting_period.year != submission_window.opens.year)
            if submission_window not in _mhsds_submission_windows:
                changed_sw_count += 1
                assert submission_window.opens == reporting_period + relativedelta(months=+1)
                assert changed_sw_count <= 1


@pytest.mark.parametrize(['as_at_date', 'rep_period_count', 'first_rp_start_date', 'last_rp_start_date'], [
    (datetime(2021, 1, 31), 9, date(2020, 4, 1), date(2020, 12, 1)),
    (datetime(2021, 2, 28), 10, date(2020, 4, 1), date(2021, 1, 1)),
    (datetime(2021, 3, 31), 11, date(2020, 4, 1), date(2021, 2, 1)),
    (datetime(2021, 4, 30), 12, date(2020, 4, 1), date(2021, 3, 1)),
    (datetime(2021, 5, 21), 12, date(2020, 4, 1), date(2021, 3, 1)),
    # new finanacial year
    (datetime(2021, 5, 22), 13, date(2020, 4, 1), date(2021, 4, 1)),
    (datetime(2021, 5, 31), 13, date(2020, 4, 1), date(2021, 4, 1)),
    (datetime(2021, 6, 30), 2, date(2021, 4, 1), date(2021, 5, 1)),
    (datetime(2021, 7, 31), 3, date(2021, 4, 1), date(2021, 6, 1)),
])
def test_find_last_closed_submission_window(
        as_at_date: datetime, rep_period_count: int, first_rp_start_date: datetime, last_rp_start_date: datetime
):
    submission_window = _mhsds_ytd_calendar.find_last_closed_submission_window(
        as_at_date, fake_historic_windows=False,
    )
    reporting_periods = submission_window.reporting_periods
    assert len(reporting_periods) == rep_period_count
    assert reporting_periods[0].start == first_rp_start_date
    assert reporting_periods[-1].start == last_rp_start_date
