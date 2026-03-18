from datetime import date, timedelta, datetime

import pytest
from dateutil.relativedelta import relativedelta

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars import CalendarException, submission_calendar

_msds_calendar = submission_calendar(DS.MSDS, {})

@pytest.mark.parametrize(
    'reporting_period_start, fake_historic_windows, found, submission_window_opens, submission_window_closes',
    [
        # found
        [date(2000, 1, 1), True, True, date(2000, 2, 1), date(2000, 3, 31)],
        # not found, because feature is off
        [date(2000, 1, 1), False, False, None, None],
        # not found, because reporting_period_start date is not on the 1st of the month
        [date(2000, 1, 15), True, False, None, None]
    ]
)
def test_msds_submission_window_historical(
        reporting_period_start: date,
        fake_historic_windows: bool,
        found: bool,
        submission_window_opens: date,
        submission_window_closes: date
):
    if not found:
        with pytest.raises(CalendarException):
            _msds_calendar.find_submission_window(reporting_period_start, fake_historic_windows)
        return

    submission_window = _msds_calendar.find_submission_window(reporting_period_start, fake_historic_windows)
    assert submission_window.opens == submission_window_opens
    assert submission_window.closes == submission_window_closes


@pytest.mark.parametrize(
    "as_at_date, expected_rp_start, fake_historic_windows",
    [
        (datetime(2019, 9, 30, 0, 0), date(2019, 6, 1), False),
        (datetime(2019, 10, 1, 0, 0), date(2019, 7, 1), False),
        (datetime(2019, 11, 10, 0, 0), date(2019, 8, 1), False),
        (datetime(2014, 11, 10, 0, 0), date(2014, 8, 1), True),
        (datetime(2022, 11, 1, 0, 0), date(2022, 8, 1), False),
    ]
)
def test_find_last_closed_submission_window(as_at_date, expected_rp_start, fake_historic_windows):
    sw = _msds_calendar.find_last_closed_submission_window(as_at_date, fake_historic_windows)
    assert expected_rp_start == sw.reporting_periods[0].start


@pytest.mark.parametrize(
    "as_at_date, expected_rp_start, fake_historic_windows",
    [
        (datetime(2014, 11, 1, 0, 0), date(2014, 9, 1), True),
        (datetime(2022, 11, 1, 0, 0), date(2022, 9, 1), False),
        (datetime(2022, 11, 30, 0, 0), date(2022, 9, 1), False),
        (datetime(2023, 2, 1, 0, 0), date(2022, 12, 1), False),
        (datetime(2023, 2, 28, 0, 0), date(2022, 12, 1), False),
        (datetime(2023, 3, 1, 0, 0), date(2023, 1, 1), False),
        (datetime(2023, 3, 16, 0, 0), date(2023, 1, 1), False),
    ]
)
def test_find_mid_submission_window(as_at_date, expected_rp_start, fake_historic_windows):
    sw = _msds_calendar.find_msds_earliest_active_submission_window(as_at_date, fake_historic_windows)
    assert expected_rp_start == sw.reporting_periods[0].start


@pytest.mark.parametrize(
    ['submission_datetime', 'reporting_period_start_date', 'expected'],
    [
        # Reject - just before primary reporting period open
        (datetime(2019, 4, 30, 23, 59, 59), date(2019, 4, 1), False),
        # Accept - on primary reporting period open
        (datetime(2019, 5, 1, 0, 0, 0), date(2019, 4, 1), True),
        # Accept - comfortably within primary reporting period
        (datetime(2019, 5, 23, 11, 17), date(2019, 4, 1), True),
        (datetime(2019, 5, 23, 23, 59, 59), date(2019, 4, 1), True),
        (datetime(2019, 5, 24, 0, 0, 0), date(2019, 4, 1), True),
        # Accept - just before primary reporting period close
        (datetime(2019, 5, 31, 23, 59, 59), date(2019, 4, 1), True),
        # Accept - on refresh reporting period open
        (datetime(2019, 6, 1, 0, 0, 0), date(2019, 4, 1), True),
        # Accept - just before refresh reporting period close
        (datetime(2019, 6, 30, 23, 59, 59), date(2019, 4, 1), True),
        # April window extended
        (datetime(2019, 7, 12, 0, 0, 0), date(2019, 4, 1), True),
        # Reject - on refresh reporting period close
        (datetime(2019, 7, 13, 0, 0, 0), date(2019, 4, 1), False)
    ]
)
def test_submitted_within_submission_window(
        submission_datetime: datetime, reporting_period_start_date: date, expected: bool
):
    assert _msds_calendar.submitted_within_submission_window(submission_datetime,
                                                             reporting_period_start_date, False) == expected


def test_ensure_submission_windows_are_in_correct_order():
    last = None

    for window in _msds_calendar.submission_windows:

        assert window.reporting_periods[0].start.day == 1
        assert window.opens == window.reporting_periods[0].start + relativedelta(months=1)
        # setting day 31 sets to last day of month, regardless of how many days in month
        assert window.closes >= window.opens + relativedelta(months=1, day=31)
        assert window.opens < window.closes
        assert window.reporting_periods[0].start < window.opens

        if not last:
            continue

        assert window.opens > last.opens
        assert window.closes > last.closes
        assert window.reporting_periods[0].start > last.reporting_periods[0].start
        last = window


@pytest.mark.parametrize(
    "reporting_period_start, as_at, expected_result,  fake_window, expected_error",
    [
        (date(2023, 1, 1), datetime(2023, 4, 1), date(2023, 3, 31), False, None),  # Refresh
        (date(2023, 2, 1), datetime(2023, 4, 1), date(2023, 4, 30), False, None),  # Provisional
        (date(2022, 12, 1), datetime(2023, 4, 1), date(2023, 2, 28), False, None),  # Refresh
        (date(2023, 3, 1), datetime(2023, 4, 1), None, False, 'MSDS no valid submission window found for 2023-03-01 as_at=2023-04-01'),  # Invalid
        (date(2023, 3, 1), datetime(2023, 9, 1), date(2023, 5, 31), False, None),  # Refresh
        (date(2023, 5, 1), datetime(2023, 5, 1), None, False, 'MSDS no valid submission window found for 2023-05-01 as_at=2023-05-01'),  # Invalid
        (date(2016, 5, 1), datetime(2019, 7, 1), None, False, 'MSDS no valid submission window found for 2016-05-01 as_at=2019-07-01'),  # Invalid
        (date(2016, 5, 1), datetime(2019, 7, 1), date(2019, 7, 31), True, None),  # fake_historic_windows
    ]
)
def test_msds_find_possible_submission_window_at_rp(
        reporting_period_start: datetime, as_at: datetime, expected_result: str, fake_window: bool, expected_error: str
):
    actual_error = None
    actual_result = None

    try:
        actual_result = _msds_calendar.find_msds_submission_window_at_rp(
            reporting_period_start, as_at, fake_historic_windows=fake_window
        ).closes
    except Exception as e:
        actual_error = str(e)

    assert actual_error == expected_error
    assert actual_result == expected_result