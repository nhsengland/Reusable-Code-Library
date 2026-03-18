from datetime import date, datetime
from typing import Tuple

import pytest
from dateutil.relativedelta import relativedelta

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars import submission_calendar
from dsp.shared.submissions.calendars.common import CalendarException

_mhsds_v5_calendar = submission_calendar(DS.MHSDS_V5, {})


@pytest.mark.parametrize(
    ["reporting_period_start", "as_at", "expected_result", "expected_error"],
    [
        (date(2021, 10, 1), datetime(2021, 11, 30), date(2021, 11, 18), None),
        (date(2021, 10, 1), datetime(2021, 12, 1), date(2021, 11, 18), None),
        (date(2021, 10, 1), datetime(2021, 12, 20), date(2021, 11, 18), None),
        (date(2021, 10, 1), datetime(2021, 12, 21), date(2021, 12, 20), None),
        (date(2022, 6, 1), datetime(2022, 5, 1), None, "MHSDS_V5 no submission window for 2022-06-01 as at 2022-05-01"),
        (date(2022, 6, 1), datetime(2022, 6, 1), None, "MHSDS_V5 no submission window for 2022-06-01 as at 2022-06-01"),
        (date(2022, 6, 1), datetime(2022, 7, 1), None, "MHSDS_V5 no submission window for 2022-06-01 as at 2022-07-01"),
        (date(2022, 6, 1), datetime(2022, 8, 1), date(2022, 7, 20), None),
        (
                date(2021, 5, 1),
                datetime(2021, 6, 1),
                None,
                "MHSDS_V5 no submission window for 2021-05-01 as at 2021-06-01",
        ),
        (
                date(2030, 4, 1),
                datetime(2032, 7, 1),
                None,
                "MHSDS_V5 no submission window for 2030-04-01 as at 2032-07-01"
        ),
    ],
)
def test_mhsds_snap_to_completed_window(
    reporting_period_start: datetime, as_at: datetime, expected_result: str, expected_error: str
):
    actual_error = None
    actual_result = None

    try:
        actual_result = _mhsds_v5_calendar.find_last_closed_submission_window_for_rp(
            reporting_period_start, as_at, fake_historic_windows=False
        ).closes
    except Exception as e:
        actual_error = str(e)

    assert actual_error == expected_error
    assert actual_result == expected_result


@pytest.mark.parametrize(
    ["reporting_period_start", "as_at", "expected_result", "expected_error"],
    [
        (date(2021, 12, 1), datetime(2022, 1, 21), "Primary", None),
        (date(2021, 12, 1), datetime(2022, 1, 22), "Refresh", None),
        (date(2021, 12, 1), datetime(2022, 2, 1), "Refresh", None),
        (date(2021, 12, 1), datetime(2025, 1, 1), "Refresh", None),
        (
            date(2021, 12, 1),
            datetime(2021, 12, 31),
            None,
            "the as at date cannot precede the primary reporting period opens, as no submission windows can be selected.",
        ),
    ],
)
def test_get_submission_window_type(
    reporting_period_start: datetime, as_at: datetime, expected_result: str, expected_error: str
):
    actual_error = None
    actual_result = None

    try:
        actual_result = _mhsds_v5_calendar.get_submission_window_type(
            reporting_period_start,
            as_at,
            fake_historic_windows=False,
        )
    except Exception as e:
        actual_error = str(e)

    assert actual_error == expected_error
    assert actual_result == expected_result


@pytest.mark.parametrize(
    ["reporting_period_start", "as_at", "expected_result", "expected_error"],
    [
        (date(1970, 1, 1), datetime(2021, 1, 22), None, "unable to find submission windows for MHSDS_V5 1970-01-01"),
        (date(2021, 10, 1), datetime(2021, 10, 22), 1, None),
        (date(2021, 10, 1), datetime(2021, 11, 18), 1, None),
        (date(2021, 10, 1), datetime(2021, 11, 18), 1, None),
        (date(2021, 10, 1), datetime(2021, 11, 19), 2, None),
        (date(2021, 10, 1), datetime(2021, 11, 30), 2, None),
        (date(2021, 10, 1), datetime(2021, 12, 1), 2, None),
        (date(2021, 10, 1), datetime(2021, 12, 20), 2, None),
        (date(2021, 10, 1), datetime(2021, 12, 21), 3, None),
        (date(2021, 10, 1), datetime(2022, 1, 1), 3, None),
        (date(2021, 10, 1), datetime(2022, 2, 1), 4, None),
        (date(2021, 10, 1), datetime(2022, 3, 1), 5, None),
        (date(2021, 10, 1), datetime(2022, 4, 1), 6, None),
    ],
)
def test_get_submission_num_months_since_ytd_rp_start(
    reporting_period_start: datetime, as_at: datetime, expected_result: str, expected_error: str
):
    actual_error = None
    actual_result = None

    try:
        actual_result = _mhsds_v5_calendar.get_submission_num_sws_since_ytd_rp_start(
            reporting_period_start, as_at, fake_historic_windows=False
        )
    except Exception as e:
        actual_error = str(e)

    assert actual_error == expected_error
    assert actual_result == expected_result


@pytest.mark.parametrize(
    ["submission_datetime", "reporting_period_start_date", "expected"],
    [
        # Reject - just before primary reporting window open
        (datetime(2021, 10, 31, 23, 59, 59), date(2021, 10, 1), False),
        # Accept - on primary reporting window open
        (datetime(2021, 11, 1, 0, 0, 0), date(2021, 10, 1), True),
        # Accept - comfortably within primary reporting window
        (datetime(2021, 11, 15, 11, 17), date(2021, 10, 1), True),
        # Accept - just before primary reporting window close
        (datetime(2022, 2, 18, 23, 59, 59), date(2022, 1, 1), True),
        # Reject - on primary reporting window close
        (datetime(2022, 2, 18, 0, 0, 0), date(2022, 2, 1), False),
        # Reject - just before first refresh reporting window open
        (datetime(2022, 2, 18, 23, 59, 59), date(2022, 2, 1), False),
        # Accept - on first refresh reporting window open
        (datetime(2022, 2, 19, 0, 0, 0), date(2022, 2, 1), False),
    ],
)
def test_submitted_within_submission_window(
    submission_datetime: datetime, reporting_period_start_date: date, expected: bool
):
    assert (
            _mhsds_v5_calendar.submitted_within_submission_window(
                submission_datetime,
                reporting_period_start_date,
                fake_historic_windows=False,
            )
            == expected
    )


def test_find_submission_window_by_submission_date_historic_windows_allowed():
    window = _mhsds_v5_calendar.find_submission_window_by_submission_date(
        date(2001, 4, 15), fake_historic_windows=True
    )

    assert window
    assert window.opens == date(2001, 3, 21)
    assert window.closes == date(2001, 4, 20)
    assert window.reporting_periods[-1].start == date(2001, 3, 1)
    assert window.reporting_periods[-2].start == date(2001, 2, 1)


_mhsds_submission_windows = _mhsds_v5_calendar.submission_windows


@pytest.mark.parametrize(
    ["submission_date", "snap_to_last_open_window", "expect_raises", "expect_refresh_rp_start_date"],
    [
        (date(2001, 1, 1), False, True, None),
        (date(2001, 1, 1), True, True, None),
        (date(2019, 1, 1), True, True, None),
        (date(2019, 1, 1), False, True, None),
        (_mhsds_submission_windows[-1].closes + relativedelta(days=1), False, True, None),
        (_mhsds_submission_windows[-1].closes + relativedelta(days=1), True, True, None),
        (
                _mhsds_submission_windows[-2].closes + relativedelta(days=1),
                False,
                False,
                _mhsds_submission_windows[-2].reporting_periods[-1].start,
        ),
        (
                _mhsds_submission_windows[-2].closes + relativedelta(days=1),
                True,
                False,
                _mhsds_submission_windows[-2].reporting_periods[-1].start,
        ),
        (date(2021, 11, 18), False, False, date(2021, 9, 1)),
        (date(2021, 11, 19), True, False, date(2021, 10, 1)),
        (date(2021, 11, 20), True, False, date(2021, 10, 1)),
        (date(2021, 12, 1), True, False, date(2021, 10, 1)),
        (date(2021, 12, 20), True, False, date(2021, 10, 1)),
        (date(2021, 12, 21), True, False, date(2021, 11, 1)),
    ],
)
def test_find_submission_window_by_submission_date(
    submission_date: date, snap_to_last_open_window: bool, expect_raises: bool, expect_refresh_rp_start_date: date
):
    if expect_raises:
        with pytest.raises(CalendarException):
            _mhsds_v5_calendar.find_submission_window_by_submission_date(
                submission_date, fake_historic_windows=False, snap_to_last_open_window=snap_to_last_open_window
            )
        return

    window = _mhsds_v5_calendar.find_submission_window_by_submission_date(
        submission_date, fake_historic_windows=False, snap_to_last_open_window=snap_to_last_open_window
    )

    assert window
    assert window.reporting_periods[-2].start == expect_refresh_rp_start_date


def test_ensure_submission_windows_are_in_correct_order():
    last = None
    last_refresh_rp = None

    for window in _mhsds_v5_calendar.submission_windows:
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


@pytest.mark.parametrize(["as_at"], [(20190223,), (20190222,), (20201212,)])
def test_get_refresh_rp_start_and_unique_month_id_as_at(as_at):
    with pytest.raises(CalendarException) as ex:
        _mhsds_v5_calendar.get_refresh_unique_month_id_and_rp_start_as_at(as_at, fake_historic_windows=False)
    assert ex
    assert ex.value.args[0] == "Calendar for MHSDS_V5 is YTD, multiple refresh windows available"


@pytest.mark.parametrize(
    ["as_at", "expected_result"],
    [
        # After June 2021 SW close, earliest should be Apr of the same year
        (datetime(2021, 7, 5), (1453, date(2021, 4, 1))),
        # After July 2021 SW close, earliest should be Apr of the same year
        (datetime(2021, 7, 30), (1453, date(2021, 4, 1))),
        # After Aug  2021 SW close, earliest should be Apr of the same year
        (datetime(2021, 8, 30), (1453, date(2021, 4, 1))),
        # After Jan  2022 SW close, earliest should be Apr of the previous year year
        (datetime(2022, 1, 30), (1459, date(2021, 10, 1))),
    ],
)
def test_get_earliest_unique_month_id_and_rp_start_as_at(as_at: datetime, expected_result: Tuple[int, date]):
    actual_result = _mhsds_v5_calendar.get_earliest_unique_month_id_and_rp_start_as_at(
        as_at, fake_historic_windows=False
    )
    assert actual_result == expected_result


@pytest.mark.parametrize(
    ["as_at", "expected_result"],
    [
        (datetime(2021, 7, 5), (1454, date(2021, 5, 1))),
        (datetime(2021, 7, 30), (1455, date(2021, 6, 1))),
        (datetime(2021, 8, 30), (1456, date(2021, 7, 1))),
        (datetime(2021, 12, 20), (1459, date(2021, 10, 1))),
        (datetime(2021, 12, 21), (1460, date(2021, 11, 1))),
        (datetime(2022, 1, 30), (1461, date(2021, 12, 1))),
    ],
)
def test_get_latest_unique_month_id_and_rp_start_as_at(
    as_at: datetime,
    expected_result: Tuple[int, date],
):
    res = _mhsds_v5_calendar.get_latest_unique_month_id_and_rp_start_as_at(as_at, fake_historic_windows=False)
    assert res == expected_result


@pytest.mark.parametrize(
    ["as_at", "expected_result"],
    [
        # After June 2021 SW close, primary should be May of the same year
        (datetime(2021, 7, 5), (1454, date(2021, 5, 1))),
        # After July 2021 SW close, primary should be Jun of the same year
        (datetime(2021, 7, 30), (1455, date(2021, 6, 1))),
        # After Aug  2021 SW close, primary should be July of the same year
        (datetime(2021, 8, 30), (1456, date(2021, 7, 1))),
        # After Jan  2022 SW close, primary should be Dec of the previous year
        (datetime(2022, 1, 30), (1461, date(2021, 12, 1))),
    ],
)
def test_get_primary_unique_month_id_and_rp_start_as_at(as_at: datetime, expected_result: Tuple[int, date]):
    actual_result = _mhsds_v5_calendar.get_primary_unique_month_id_and_rp_start_as_at(
        as_at, fake_historic_windows=False
    )
    assert actual_result == expected_result


def test_no_overlap_between_rps_and_sws():
    for reporting_period, submission_windows in _mhsds_v5_calendar.rp_start_maps.items():
        changed_sw_count = 0
        for submission_window in submission_windows:
            assert (
                    reporting_period.day != submission_window.opens.day
                    or reporting_period.month != submission_window.opens.month
                    or reporting_period.year != submission_window.opens.year
            )
            if submission_window not in _mhsds_submission_windows:
                changed_sw_count += 1
                assert submission_window.opens == reporting_period + relativedelta(months=+1)
                assert changed_sw_count <= 1


@pytest.mark.parametrize(
    ["as_at_date", "rep_period_count", "first_rp_start_date", "last_rp_start_date"],
    [
        (datetime(2021, 10, 31), 6, date(2021, 4, 1), date(2021, 9, 1)),
        (datetime(2021, 11, 30), 7, date(2021, 4, 1), date(2021, 10, 1)),
        (datetime(2021, 12, 31), 2, date(2021, 10, 1), date(2021, 11, 1)),
        (datetime(2022, 1, 31), 3, date(2021, 10, 1), date(2021, 12, 1)),
        (datetime(2022, 2, 28), 4, date(2021, 10, 1), date(2022, 1, 1)),
        (datetime(2022, 3, 31), 5, date(2021, 10, 1), date(2022, 2, 1)),
        (datetime(2022, 4, 30), 6, date(2021, 10, 1), date(2022, 3, 1)),
        (datetime(2022, 5, 31), 7, date(2021, 10, 1), date(2022, 4, 1)),
        (datetime(2022, 6, 30), 2, date(2022, 4, 1), date(2022, 5, 1)),
        (datetime(2022, 7, 31), 3, date(2022, 4, 1), date(2022, 6, 1)),
    ],
)
def test_find_last_closed_submission_window(
    as_at_date: datetime, rep_period_count: int, first_rp_start_date: datetime, last_rp_start_date: datetime
):
    submission_window = _mhsds_v5_calendar.find_last_closed_submission_window(
        as_at_date,
        fake_historic_windows=False,
    )
    reporting_periods = submission_window.reporting_periods
    assert len(reporting_periods) == rep_period_count
    assert reporting_periods[0].start == first_rp_start_date
    assert reporting_periods[-1].start == last_rp_start_date
