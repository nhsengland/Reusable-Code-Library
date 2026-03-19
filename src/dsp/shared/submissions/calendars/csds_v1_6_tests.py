from datetime import date, datetime

import pytest

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars import submission_calendar

_csds_v1_6_calendar = submission_calendar(DS.CSDS_V1_6, {})


@pytest.mark.parametrize(
    ['submission_datetime', 'reporting_period_start_date', 'expected'],
    [
        # Reject - no submission window
        (datetime(2023, 2, 14, 23, 59, 59), date(2023, 1, 1), False),

        # Accept - on primary reporting window open
        (datetime(2023, 2, 16, 0, 0, 0), date(2023, 1, 1), False),

        # Accept - on primary reporting window open
        (datetime(2023, 2, 17, 0, 0, 0), date(2023, 1, 1), True),

        # Accept - on refresh reporting window open
        (datetime(2023, 3, 1, 0, 0, 0), date(2023, 1, 1), True),

        # Accept - just before refresh reporting window close
        (datetime(2023, 3, 14, 23, 59, 59), date(2023, 1, 1), True),

        # Accept - just before refresh reporting window close
        (datetime(2023, 4, 18, 23, 59, 59), date(2023, 3, 1), True),

        # Accept - just after refresh reporting window open
        (datetime(2023, 4, 19, 0, 0, 1), date(2023, 4, 1), True),

        # Accept - on reporting window open
        (datetime(2023, 5, 19, 0, 0, 0), date(2023, 5, 1), True),

        # Accept - just before reporting window close
        (datetime(2023, 6, 14, 23, 59, 59), date(2023, 5, 1), True),

        # Accept - on reporting window open
        (datetime(2023, 6, 15, 0, 0, 0), date(2023, 6, 1), True),

        # Accept - just before reporting window close
        (datetime(2023, 7, 14, 23, 59, 59), date(2023, 6, 1), True),

        # Accept - on refresh reporting window open
        (datetime(2023, 7, 17, 0, 0, 0), date(2023, 7, 1), True),

        # Accept - just before refresh reporting window close
        (datetime(2023, 8, 14, 23, 59, 59), date(2023, 7, 1), True),

        # Accept - on refresh reporting window open
        (datetime(2023, 8, 15, 0, 0, 0), date(2023, 8, 1), True),

        # Accept - just before reporting window close
        (datetime(2023, 9, 14, 23, 59, 59), date(2023, 8, 1), True),

        # Accept - on reporting window open
        (datetime(2023, 9, 15, 0, 0, 0), date(2023, 9, 1), True),

        # Accept - just before reporting window close
        (datetime(2023, 10, 13, 23, 59, 59), date(2023, 9, 1), True),
    ]
)
def test_submitted_within_submission_window(submission_datetime: datetime, reporting_period_start_date: date,
                                            expected: bool):
    assert _csds_v1_6_calendar.submitted_within_submission_window(
        submission_datetime, reporting_period_start_date, fake_historic_windows=False
    ) == expected
