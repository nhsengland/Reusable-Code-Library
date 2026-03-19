from datetime import date, datetime

import pytest
from dateutil.relativedelta import relativedelta

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars import submission_calendar
from dsp.shared.submissions.calendars.common import CalendarException

_iapt_calendar = submission_calendar(DS.IAPT, {})


@pytest.mark.parametrize(
    "submission_date, fake_historic_windows, snap_to_last_open_window, expect_raises, expect_refresh_rp_start_date",
    [
        # date outside of submission window range, don't use fake historic
        (date(2001, 1, 1), False, False, True, None),
        # date outside of submission window range, use fake historic
        (date(2001, 4, 15), True, False, False, date(2001, 2, 1)),
        # after close of submission window, don't snap to last open window, does not fall within any submission window
        (_iapt_calendar.submission_windows[-1].closes + relativedelta(days=1), False, False, True, None),
        # after close of submission window, snap to last open window, but the last open submission window is the last
        # in the list of submission windows - there could potentially be a later submission window that hasn't been
        # added to the list yet
        (_iapt_calendar.submission_windows[-1].closes + relativedelta(days=1), False, True, True, None),
        # after close of submission window, don't snap to last open window, does not fall within any submission window
        (_iapt_calendar.submission_windows[-2].closes + relativedelta(days=1), False, False, True, None),
        # after close of submission window, snap to last open window
        (_iapt_calendar.submission_windows[-2].closes + relativedelta(days=1), False, True, False,
         _iapt_calendar.submission_windows[-2].reporting_periods[-2].start),
        # date on the close of submission window range
        (date(2020, 9, 25), False, False, False, date(2020, 7, 1)),
        # day after close of submission window range, don't snap to last open
        (date(2020, 9, 26), False, False, True, None),
        # day after close of submission window range, snap to last open
        (date(2020, 9, 26), False, True, False, date(2020, 7, 1)),
        # test extended refresh periods
        (date(2021, 8, 26), False, False, False, date(2020, 9, 1)),
    ])
def test_find_submission_window_by_submission_date(
        submission_date: date, fake_historic_windows: bool, snap_to_last_open_window: bool,
        expect_raises: bool, expect_refresh_rp_start_date: date
):
    if expect_raises:
        with pytest.raises(CalendarException):
            _iapt_calendar.find_submission_window_by_submission_date(
                submission_date, fake_historic_windows=fake_historic_windows,
                snap_to_last_open_window=snap_to_last_open_window
            )
        return

    window = _iapt_calendar.find_submission_window_by_submission_date(
        submission_date, fake_historic_windows=fake_historic_windows, snap_to_last_open_window=snap_to_last_open_window
    )

    assert window
    assert window.reporting_periods[0].start == expect_refresh_rp_start_date


@pytest.mark.parametrize(
    ['submission_datetime', 'reporting_period_start_date', 'expected'],
    [
        # Reject - just before primary reporting window open
        (datetime(2020, 7, 31, 23, 59, 59), date(2020, 7, 1), False),
        # Accept - on primary reporting window open
        (datetime(2020, 8, 1, 0, 0, 0), date(2020, 7, 1), True),
        # Accept - comfortably within primary reporting window
        (datetime(2020, 8, 15, 0, 0, 0), date(2020, 7, 1), True),
        # Accept - just before primary reporting window close
        (datetime(2020, 8, 27, 23, 59, 59), date(2020, 7, 1), True),
        # Reject - on primary reporting window close
        (datetime(2020, 8, 30, 0, 0, 0), date(2020, 7, 1), False),
        # Reject - just before refresh reporting window open
        (datetime(2020, 8, 31, 23, 59, 59), date(2020, 7, 1), False),
        # Accept - on refresh reporting window open
        (datetime(2020, 9, 1, 0, 0, 0), date(2020, 7, 1), True),
        # Accept - comfortably within primary reporting window
        (datetime(2020, 9, 16, 0, 0, 0), date(2020, 7, 1), True),
        # Accept - just before refresh reporting window close
        (datetime(2020, 9, 25, 23, 59, 59), date(2020, 7, 1), True),
        # Reject - on refresh reporting window close
        (datetime(2020, 9, 26, 0, 0, 0), date(2020, 7, 1), False),
        # Accept - on refresh reporting window open
        (datetime(2021, 3, 2, 0, 0, 0), date(2021, 1, 1), True),
        # Accept - on refresh reporting window open
        (datetime(2021, 4, 2, 0, 0, 0), date(2021, 2, 1), True),
        # Accept - on refresh reporting window open
        (datetime(2021, 5, 2, 0, 0, 0), date(2021, 3, 1), True),
        # Accept - September extended refresh window
        (datetime(2020, 12, 20, 0, 0, 0), date(2020, 9, 1), True),
        (datetime(2020, 11, 20, 0, 0, 0), date(2020, 9, 1), True),
        (datetime(2020, 10, 20, 0, 0, 0), date(2020, 9, 1), True),
        # Accept - August 2021 extended refresh window
        (datetime(2021, 8, 20, 0, 0, 0), date(2020, 8, 1), False),
        (datetime(2021, 8, 20, 0, 0, 0), date(2020, 9, 1), True),
        (datetime(2021, 8, 20, 0, 0, 0), date(2021, 7, 1), True),
        (datetime(2021, 8, 20, 0, 0, 0), date(2021, 8, 1), False),
    ]
)
def test_submitted_within_submission_window(
        submission_datetime: datetime, reporting_period_start_date: date, expected: bool
):
    assert _iapt_calendar.submitted_within_submission_window(
        submission_datetime, reporting_period_start_date, fake_historic_windows=False
    ) == expected


def test_find_last_closed_submission_window():
    window = _iapt_calendar.find_last_closed_submission_window("202205140000000000", False)
    assert window.opens == date(2022, 5, 1)
