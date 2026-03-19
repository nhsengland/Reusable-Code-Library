from datetime import date, datetime

import pytest

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars import submission_calendar

_csds_calendar = submission_calendar(DS.CSDS, {})


@pytest.mark.parametrize(
    ['submission_datetime', 'reporting_period_start_date', 'expected'],
    [
        # Reject - no submission window
        (datetime(2020, 10, 12, 23, 59, 59), date(2020, 7, 1), False),
        # Accept - on primary reporting window open
        (datetime(2020, 8, 1, 0, 0, 0), date(2020, 7, 1), True),
        # Accept - comfortably within primary reporting window
        (datetime(2020, 8, 15, 0, 0, 0), date(2020, 7, 1), True),
        # Accept - just before primary reporting window close
        (datetime(2020, 8, 31, 23, 59, 59), date(2020, 7, 1), True),
        # Accept - on refresh reporting window open
        (datetime(2020, 9, 1, 0, 0, 0), date(2020, 7, 1), True),
    ]
)
def test_submitted_within_submission_window(submission_datetime: datetime, reporting_period_start_date: date,
                                            expected: bool):
    assert _csds_calendar.submitted_within_submission_window(
        submission_datetime, reporting_period_start_date, fake_historic_windows=False
    ) == expected
