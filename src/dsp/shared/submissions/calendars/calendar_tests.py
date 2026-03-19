from datetime import date, datetime, timedelta
from typing import List, Sequence

import pytest
from dateutil.relativedelta import relativedelta

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars import submission_calendar
from dsp.shared.submissions.calendars.common import (
    calculate_uniq_month_id_from_date,
    SubmissionCalendar,
    SubmissionWindowConfig,
    financial_year_end_date_as_at,
    financial_year_start_date_as_at
)

known_dates = [
    (date(1900, 4, 1), 1),
    (date(1900, 4, 30), 1),
    (date(1901, 4, 1), 13),
    (date(1902, 4, 1), 25),
    (date(1902, 7, 20), 28),
    (date(2014, 5, 1), 1370),
    (date(2015, 5, 1), 1382),
    (date(2019, 1, 2), 1426),
    (date(2019, 6, 1), 1431),
    (datetime(2019, 6, 1, 10, 36, 21), 1431),
]


@pytest.mark.parametrize("month_date, expected_id", known_dates)
def test_known_values_against_calculate_uniq_month_id_from_date(month_date, expected_id):
    unique_month_id = calculate_uniq_month_id_from_date(month_date)
    assert unique_month_id == expected_id


@pytest.mark.parametrize("as_at, config, expected_open, expected_close, expected_rp_starts", [
    (datetime(2019, 2, 21), SubmissionWindowConfig(start_day=3, end_day=22, windows_per_rp=2), date(2019, 1, 3),
     date(2019, 1, 22), [date(2018, 11, 1), date(2018, 12, 1)]),
    (datetime(2019, 4, 21), SubmissionWindowConfig(end_add_months=1), date(2019, 2, 1), date(2019, 3, 31),
     [date(2019, 1, 1)]),
    (datetime(2019, 5, 1), SubmissionWindowConfig(start_day=21, end_day=20, ytd=True), date(2019, 3, 21),
     date(2019, 4, 20), [(date(2018, 4, 1) + relativedelta(months=i)) for i in range(12)]),
    (datetime(2019, 6, 1), SubmissionWindowConfig(start_day=21, end_day=20, ytd=True), date(2019, 4, 21),
     date(2019, 5, 20), [(date(2018, 4, 1) + relativedelta(months=i)) for i in range(13)]),
    (datetime(2019, 7, 1), SubmissionWindowConfig(start_day=21, end_day=20, ytd=True), date(2019, 5, 21),
     date(2019, 6, 20), [date(2019, 4, 1), date(2019, 5, 1)])
])
def test_calendar_fake_last_closed_submission_window_as_at(
        as_at: datetime, config: SubmissionWindowConfig, expected_open: date,
        expected_close: date, expected_rp_starts: List[date]
):
    calendar = SubmissionCalendar('things', [], config)

    sw = calendar.fake_last_closed_window_as_at(as_at)

    assert sw.opens == expected_open
    assert sw.closes == expected_close

    rp_starts = [rp.start for rp in sw.reporting_periods]
    assert rp_starts == expected_rp_starts, (rp_starts, expected_rp_starts)


@pytest.mark.parametrize(
    "dataset_id, rp_start_date, as_at, snap_to_closed_window, expected_result, expected_error, allow_pre_sw",
    [
        (DS.MSDS, date(2021, 10, 1), datetime(2021, 12, 1), True, 'Mid-Window', None, False),
        (DS.MSDS, date(2021, 10, 1), datetime(2022, 11, 1), True, 'Post-Deadline', None, False),
        (DS.MHSDS_V5, date(2021, 10, 1), datetime(2021, 12, 21), False, 'Refresh', None, False),
        (DS.MHSDS_V5, date(2021, 10, 1), datetime(2021, 12, 21, 0, 0, 0, 1), False, 'Refresh', None, False),
        (DS.IAPT, date(2019, 5, 1), datetime(2019, 5, 20), False, None, "the as at date cannot precede the primary "
                                                                        "reporting period opens, as no submission windows can be selected.",
         False),
        (DS.IAPT, date(2019, 5, 1), datetime(2019, 5, 20), False, "undefined", None, True),
        (DS.CSDS, date(2023, 1, 1), datetime(2023, 2, 18), False, "Refresh", None, True),
        (DS.CSDS_V1_6, date(2023, 1, 1), datetime(2023, 2, 18), False, "Primary", None, True)
    ])
def test_get_submission_window_type(
        dataset_id: str, rp_start_date: date, as_at: datetime, snap_to_closed_window: bool, expected_result: str,
        expected_error: str, allow_pre_sw: bool
):
    actual_error = None
    actual_result = None

    calendar = submission_calendar(dataset_id, {})
    try:
        actual_result = calendar.get_submission_window_type(
            rp_start_date, as_at, True, snap_to_closed_window, allow_pre_sw
        )
    except Exception as e:
        actual_error = str(e)

    assert actual_error == expected_error
    assert actual_result == expected_result


@pytest.mark.parametrize("dataset_id, rp_start_date, expected_number", [
    (DS.MSDS, date(2019, 12, 1), 1),
    (DS.MSDS, date(2019, 3, 1), 1),
    (DS.MHSDS_V5, date(2019, 3, 1), 2),
    (DS.MHSDS_V5, date(2019, 7, 1), 10),
    (DS.MHSDS_V5, date(2019, 12, 1), 5),
])
def test_num_sw_for_rp(
        dataset_id: str, rp_start_date: date, expected_number: int
):
    calendar = submission_calendar(dataset_id, {})
    actual = calendar.num_sw_for_rp(rp_start_date)

    assert actual == expected_number


@pytest.mark.parametrize(
    [
        "dataset_id",
        "reporting_period_start",
        "expected_number_sws",
        "expected_first_submission_window_opens",
        "expected_last_submission_window_opens",
        "expected_first_sw_earliest_rp_opens",
        "expected_last_sw_last_rp_opens",
    ],
    [
        (DS.MSDS, date(2010, 1, 1), 1, date(2010, 2, 1), date(2010, 2, 1), date(2010, 1, 1), date(2010, 1, 1)),
        (DS.MSDS, date(2010, 1, 1), 1, date(2010, 2, 1), date(2010, 2, 1), date(2010, 1, 1), date(2010, 1, 1)),
        (DS.MHSDS_V5, date(2010, 1, 1), 4, date(2010, 1, 21), date(2010, 4, 21), date(2009, 4, 1), date(2010, 4, 1)),
        (DS.MHSDS_V5, date(2010, 9, 1), 8, date(2010, 9, 21), date(2011, 4, 21), date(2010, 4, 1), date(2011, 4, 1)),
        (DS.MHSDS_V5, date(2010, 12, 1), 5, date(2010, 12, 21), date(2011, 4, 21), date(2010, 4, 1), date(2011, 4, 1)),
        (DS.MHSDS_V5, date(2010, 3, 1), 2, date(2010, 3, 21), date(2010, 4, 21), date(2009, 4, 1), date(2010, 4, 1)),
    ]
)
def test_fake_historic_windows(dataset_id: str, reporting_period_start: datetime, expected_number_sws: int,
                               expected_first_submission_window_opens: datetime,
                               expected_last_submission_window_opens: datetime,
                               expected_first_sw_earliest_rp_opens: datetime,
                               expected_last_sw_last_rp_opens: datetime):
    calendar = submission_calendar(dataset_id, {})
    rp_windows = calendar.fake_windows_for_rp(reporting_period_start)
    assert len(rp_windows) == expected_number_sws
    assert rp_windows[0].opens == expected_first_submission_window_opens
    assert rp_windows[-1].opens == expected_last_submission_window_opens
    assert rp_windows[0].earliest_rp.start == expected_first_sw_earliest_rp_opens
    assert rp_windows[-1].last_rp.start == expected_last_sw_last_rp_opens


@pytest.mark.parametrize("sw_opens, expected_result", [
    (date(2019, 12, 1), 8),
    (date(2020, 1, 1), 9),
    (date(2020, 2, 1), 10),
    (date(2020, 3, 1), 11),
    (date(2020, 4, 1), 12),
    (date(2020, 5, 1), 13),
    (date(2020, 6, 1), 2),
    (date(2020, 7, 1), 3),

])
def test_ytd_num_rps_for_sw(sw_opens: date,
                            expected_result: int):
    result = SubmissionCalendar.ytd_num_rps_for_sw(sw_opens)
    assert result == expected_result


@pytest.mark.parametrize("sw_closes, expected_result", [
    (date(2020, 3, 31), [date(2019, 4, 1), date(2019, 5, 1), date(2019, 6, 1), date(2019, 7, 1), date(2019, 8, 1),
                         date(2019, 9, 1), date(2019, 10, 1), date(2019, 11, 1), date(2019, 12, 1), date(2020, 1, 1),
                         date(2020, 2, 1)]),
    (date(2020, 4, 30), [date(2019, 4, 1), date(2019, 5, 1), date(2019, 6, 1), date(2019, 7, 1), date(2019, 8, 1),
                         date(2019, 9, 1), date(2019, 10, 1), date(2019, 11, 1), date(2019, 12, 1), date(2020, 1, 1),
                         date(2020, 2, 1), date(2020, 3, 1)]),
    (date(2020, 5, 31), [date(2019, 4, 1), date(2019, 5, 1), date(2019, 6, 1), date(2019, 7, 1), date(2019, 8, 1),
                         date(2019, 9, 1), date(2019, 10, 1), date(2019, 11, 1), date(2019, 12, 1), date(2020, 1, 1),
                         date(2020, 2, 1), date(2020, 3, 1), date(2020, 4, 1)]),
    (date(2020, 6, 30), [date(2020, 4, 1), date(2020, 5, 1)]),
    (date(2020, 7, 31), [date(2020, 4, 1), date(2020, 5, 1), date(2020, 6, 1)])
])
def test_get_mhsds_ytd_rp_starts_from_sw_closes(sw_closes: date, expected_result: Sequence[date]):
    calendar = submission_calendar(DS.MHSDS_V5, {})
    result = calendar.get_ytd_rp_starts_from_sw_closes(sw_closes)
    assert result == expected_result


@pytest.mark.parametrize("as_at_date, expected_result", [
    (datetime(2021, 12, 20), [1459, 1453, date(2021, 4, 1)]),
    (datetime(2022, 1, 2), [1460, 1459, date(2021, 10, 1)]),
    (datetime(2022, 1, 5), [1460, 1459, date(2021, 10, 1)]),
    (datetime(2022, 2, 2), [1461, 1459, date(2021, 10, 1)]),
    (datetime(2022, 3, 2), [1462, 1459, date(2021, 10, 1)]),
    (datetime(2022, 4, 2), [1463, 1459, date(2021, 10, 1)]),
    (datetime(2022, 4, 30), [1464, 1459, date(2021, 10, 1)])
])
def test_correct_dates_and_month_ids_are_returned_from_ytd(as_at_date, expected_result):
    calendar = submission_calendar(DS.MHSDS_V5, feature_toggles={'mhsds_ytd_submissions': True})

    primary_month_id, _ = calendar.get_primary_unique_month_id_and_rp_start_as_at(
        as_at_date, False
    )

    earliest_month_id, earliest_rp_start_date = calendar.get_earliest_unique_month_id_and_rp_start_as_at(
        as_at_date, False
    )
    assert expected_result == [primary_month_id, earliest_month_id, earliest_rp_start_date]


@pytest.mark.parametrize("as_at, expected_fy_start, expected_fy_end", [
    (date(2019, 6, 5), date(2019, 4, 1), date(2020, 4, 1)),
    (date(1993, 1, 12), date(1992, 4, 1), date(1993, 4, 1)),
    (date(2001, 3, 31), date(2000, 4, 1), date(2001, 4, 1)),
    (date(2001, 4, 2), date(2001, 4, 1), date(2002, 4, 1)),
    (date(2001, 4, 1), date(2001, 4, 1), date(2002, 4, 1)),
    (date(2020, 1, 23), date(2019, 4, 1), date(2020, 4, 1)),
])
def test_financial_year_boundaries(as_at, expected_fy_start, expected_fy_end):
    assert financial_year_start_date_as_at(as_at) == expected_fy_start
    assert financial_year_end_date_as_at(as_at) == expected_fy_end


@pytest.mark.parametrize("dataset", [DS.CSDS, DS.CSDS_V1_6, DS.IAPT, DS.MSDS, DS.MHSDS_V5, DS.IAPT_V2_1])
def test_submission_windows_future(dataset: str) -> None:
    """
    Important: If this test fails it is because the hard coded submission dates in SUBMISSION_CALENDAR are running out.
        * MSDS:  src/shared/submissions/calendars/msds.py
        * CSDS:  src/shared/submissions/calendars/csds.py
        * CSDS_V1_6:  src/shared/submissions/calendars/csds_v1_6.py
        * IAPT:  src/shared/submissions/calendars/iapt.py
        * IAPT_V2_1:  src/shared/submissions/calendars/iapt_v2_1.py
        * MHSDS_V5: src/shared/submissions/calendars/mhsds_v5.py


    In this case we urgently need to populate new windows in the SUBMISSION_CALENDAR for given dataset.

    The dates were acquired from a document linked on this page:
        * MSDS
            https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/maternity-services-data-set#what-are-the-submission-dates-
        * CSDS
            https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/community-services-data-set/submitting-data-for-the-csds-to-the-sdcs-cloud#csds-v1-5-submission-dates
        * IAPT
            https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/improving-access-to-psychological-therapies-data-set/submitting-iapt-data#monthly-submission-dates
    """

    if dataset in [DS.MSDS]:
        _LOOK_AHEAD_DAYS = 30
        _submission_calendar = submission_calendar(dataset, {})
    elif dataset in [DS.MHSDS_V5]:
        _LOOK_AHEAD_DAYS = 50
        _submission_calendar = submission_calendar(dataset, feature_toggles={'mhsds_ytd_submissions': True})
    elif dataset == DS.IAPT:
        # Calendar complete - moving onto IAPT 2.1
        return
    elif dataset == DS.CSDS:
        # Calendar complete - moving onto csds v1.6
        return
    else:
        _LOOK_AHEAD_DAYS = 90
        _submission_calendar = submission_calendar(dataset, {})

    last_submission_window_opening_date = _submission_calendar.submission_windows[-1].opens
    exp_date = (datetime.today() + timedelta(days=_LOOK_AHEAD_DAYS)).date()
    assert last_submission_window_opening_date > exp_date, \
        f"The last submission window for {dataset} opens in no more than {_LOOK_AHEAD_DAYS} days, " \
        "add new windows to the calendar"
