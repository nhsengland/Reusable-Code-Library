from datetime import date, datetime

import pytest
from dateutil.relativedelta import relativedelta

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars import submission_calendar
from dsp.shared.submissions.calendars.common import CalendarException

_iapt_v2_1_calendar = submission_calendar(DS.IAPT_V2_1, {})


@pytest.mark.parametrize(
    "submission_date, fake_historic_windows, snap_to_last_open_window, expect_raises, expect_refresh_rp_start_date",
    [
        # date outside of submission window range, don't use fake historic
        (date(2001, 1, 1), False, False, True, None),
        # date outside of submission window range, use fake historic
        (date(2001, 4, 15), True, False, False, date(2001, 2, 1)),
        # after close of submission window, don't snap to last open window, does not fall within any submission window
        (_iapt_v2_1_calendar.submission_windows[-1].closes + relativedelta(days=1), False, False, True, None),
        # after close of submission window, snap to last open window, but the last open submission window is the last
        # in the list of submission windows - there could potentially be a later submission window that hasn't been
        # added to the list yet
        (_iapt_v2_1_calendar.submission_windows[-1].closes + relativedelta(days=1), False, True, True, None),
        # after close of submission window, don't snap to last open window, does not fall within any submission window
        (_iapt_v2_1_calendar.submission_windows[-2].closes + relativedelta(days=1), False, False, True, None),
        # after close of submission window, snap to last open window
        (_iapt_v2_1_calendar.submission_windows[-2].closes + relativedelta(days=1), False, True, False,
         _iapt_v2_1_calendar.submission_windows[-2].reporting_periods[-2].start),
        # date on the close of submission window range
        (date(2022, 5, 27), False, False, False, date(2022, 3, 1)),
        # day after close of submission window range, don't snap to last open
        (date(2022, 5, 28), False, False, True, None),
        # day after close of submission window range, snap to last open
        (date(2022, 5, 28), False, True, False, date(2022, 3, 1)),
        # first day of submission window range, snap to last open
        (date(2022, 6, 1), False, True, False, date(2022, 4, 1)),
    ])
def test_find_submission_window_by_submission_date(
        submission_date: date, fake_historic_windows: bool, snap_to_last_open_window: bool,
        expect_raises: bool, expect_refresh_rp_start_date: date
):
    if expect_raises:
        with pytest.raises(CalendarException):
            _iapt_v2_1_calendar.find_submission_window_by_submission_date(
                submission_date, fake_historic_windows=fake_historic_windows,
                snap_to_last_open_window=snap_to_last_open_window
            )
        return

    window = _iapt_v2_1_calendar.find_submission_window_by_submission_date(
        submission_date, fake_historic_windows=fake_historic_windows, snap_to_last_open_window=snap_to_last_open_window
    )

    assert window
    assert window.reporting_periods[0].start == expect_refresh_rp_start_date


@pytest.mark.parametrize(
    ['submission_datetime', 'reporting_period_start_date', 'expected'],
    [
        # Reject - 2.1 primary window not started yet
        (datetime(2022, 5, 13, 23, 59, 59), date(2022, 3, 1), False),
        # Accept - on primary reporting window open
        (datetime(2022, 5, 27, 23, 59, 59), date(2022, 4, 1), True),
        # # Reject - Just before primary reporting window open
        # (datetime(2022, 5, 27, 23, 59, 59), date(2022, 3, 1), False),
        # Accept - comfortably within primary reporting window
        (datetime(2022, 6, 15, 0, 0, 0), date(2022, 4, 1), True),
        # Accept - refresh within primary reporting window
        (datetime(2022, 6, 29, 0, 0, 0), date(2022, 4, 1), True),
        # Accept - Primary within reporting window
        (datetime(2022, 6, 29, 0, 0, 0), date(2022, 5, 1), True),
        # Reject - Incorrect Refresh reporting window
        (datetime(2022, 7, 27, 0, 0, 0), date(2022, 4, 1), False),
        # Accept - Primary within primary reporting window
        (datetime(2022, 7, 27, 0, 0, 0), date(2022, 6, 1), True),
        # Accept - Refresh within reporting window
        (datetime(2022, 7, 27, 0, 0, 0), date(2022, 5, 1), True),
    ]
)
def test_submitted_within_submission_window(
        submission_datetime: datetime, reporting_period_start_date: date, expected: bool
):
    assert _iapt_v2_1_calendar.submitted_within_submission_window(
        submission_datetime, reporting_period_start_date, fake_historic_windows=False
    ) == expected


@pytest.mark.parametrize(
    ['as_at_date', 'expected_unique_month_id', 'expected_rp_start_date'],
    [
        # May's submission window has finished for the Primary for April
        (date(2022, 5, 28), 1465, date(2022, 4, 1)),
        (date(2022, 6, 1), 1465, date(2022, 4, 1)),
        (date(2022, 6, 29), 1465, date(2022, 4, 1)),

        # June's submission window has finished for the Primary for May
        (date(2022, 6, 30), 1466, date(2022, 5, 1)),
        (date(2022, 7, 1), 1466, date(2022, 5, 1)),
        (date(2022, 7, 27), 1466, date(2022, 5, 1)),

        # July's submission window has finished for the Primary for June
        (date(2022, 7, 28), 1467, date(2022, 6, 1)),

    ]
)
def test_get_primary_unique_month_id_and_rp_start_as_at_no_fake(
        as_at_date: date,
        expected_unique_month_id: int,
        expected_rp_start_date: date
):
    uniq_month_id, rp_start_date = _iapt_v2_1_calendar.get_primary_unique_month_id_and_rp_start_as_at(
        as_at_date,
        fake_historic_windows=False
    )
    assert uniq_month_id == expected_unique_month_id
    assert rp_start_date == expected_rp_start_date


@pytest.mark.parametrize(
    ['as_at_date', 'expected_error'],
    [
        (date(2022, 5, 15), "IAPT_V2_1 no closed submission window found for as_at_date=2022-05-15"),
        (date(2022, 5, 16), "IAPT_V2_1 no closed submission window found for as_at_date=2022-05-16"),
        (date(2022, 5, 17), "IAPT_V2_1 no closed submission window found for as_at_date=2022-05-17"),
        (date(2022, 5, 27), "IAPT_V2_1 no closed submission window found for as_at_date=2022-05-27"),
    ]
)
def test_get_primary_unique_month_id_and_rp_start_as_at_no_fake_pre_may(
        as_at_date: date, expected_error: str
):
    with pytest.raises(CalendarException) as ex:
        _iapt_v2_1_calendar.get_primary_unique_month_id_and_rp_start_as_at(
            as_at_date,
            fake_historic_windows=False
        )

    assert str(ex.value) == expected_error


@pytest.mark.parametrize(
    ['as_at_date', 'expected_unique_month_id', 'expected_rp_start_date'],
    [
        # Faked March to get a hit on a Primary RP - end day 22nd
        (date(2022, 5, 15), 1464, date(2022, 3, 1)),
        (date(2022, 5, 16), 1464, date(2022, 3, 1)),
        (date(2022, 5, 17), 1464, date(2022, 3, 1)),
        (date(2022, 5, 22), 1464, date(2022, 3, 1)),
        (date(2022, 5, 23), 1465, date(2022, 4, 1)),

        # May's submission window has finished for the Primary for April
        (date(2022, 5, 28), 1465, date(2022, 4, 1)),
        (date(2022, 6, 1), 1465, date(2022, 4, 1)),
        (date(2022, 6, 29), 1465, date(2022, 4, 1)),

        # June's submission window has finished for the Primary for May
        (date(2022, 6, 30), 1466, date(2022, 5, 1)),
        (date(2022, 7, 1), 1466, date(2022, 5, 1)),
        (date(2022, 7, 27), 1466, date(2022, 5, 1)),

        # July's submission window has finished for the Primary for June
        (date(2022, 7, 28), 1467, date(2022, 6, 1)),
    ]
)
def test_get_primary_unique_month_id_and_rp_start_as_at_faked_historic_windows(
        as_at_date: date,
        expected_unique_month_id: int,
        expected_rp_start_date: date
):
    uniq_month_id, rp_start_date = _iapt_v2_1_calendar.get_primary_unique_month_id_and_rp_start_as_at(
        as_at_date,
        fake_historic_windows=True
    )
    assert uniq_month_id == expected_unique_month_id
    assert rp_start_date == expected_rp_start_date
