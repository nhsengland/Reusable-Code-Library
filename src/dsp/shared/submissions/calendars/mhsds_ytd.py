from datetime import date

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars.common import SubmissionCalendar, SubmissionWindowConfig

_window_config = SubmissionWindowConfig(start_day=21, end_day=20, ytd=True)

SUBMISSION_CALENDAR = SubmissionCalendar.from_dates(
    DS.MHSDS_GENERIC,
    _window_config,
    [
        # sw_opens, sw_closes, rp_start_dates
        (date(2019, 1, 2), date(2019, 1, 22), None),
        (date(2019, 1, 30), date(2019, 2, 22), [date(2018, 4, 1), date(2018, 5, 1), date(2018, 6, 1), date(2018, 7, 1),
                                                date(2018, 8, 1), date(2018, 9, 1), date(2018, 10, 1),
                                                date(2018, 11, 1), date(2018, 12, 1), date(2019, 1, 1)]),
        (date(2019, 3, 4), date(2019, 3, 22), None),
        (date(2019, 4, 1), date(2019, 4, 24), None),
        (date(2019, 5, 2), date(2019, 5, 23), None),
        (date(2019, 6, 1), date(2019, 7, 4), [date(2019, 4, 1), date(2019, 5, 1)]),
        (date(2019, 7, 5), date(2019, 7, 21), None),
        (date(2019, 8, 1), date(2019, 8, 20), None),
        (date(2019, 9, 1), date(2019, 9, 19), None),
        (date(2019, 10, 1), date(2019, 10, 18), None),
        (date(2019, 11, 1), date(2019, 11, 20), None),
        (date(2019, 12, 1), date(2019, 12, 19), None),
        (date(2020, 1, 1), date(2020, 1, 21), None),
        (date(2020, 2, 1), date(2020, 2, 20), None),
        (date(2020, 3, 1), date(2020, 3, 19), None),
        (date(2020, 4, 1), date(2020, 4, 22), None),
        (date(2020, 4, 23), date(2020, 5, 21), None),

        (date(2020, 6, 4), date(2020, 6, 26), None),
        (date(2020, 6, 27), date(2020, 7, 20), None),
        (date(2020, 7, 21), date(2020, 8, 20), None),
        (date(2020, 8, 21), date(2020, 9, 18), None),
        (date(2020, 9, 19), date(2020, 10, 20), None),
        (date(2020, 10, 21), date(2020, 11, 19), None),
        (date(2020, 11, 20), date(2020, 12, 18), None),
        (date(2020, 12, 19), date(2021, 1, 21), None),
        (date(2021, 1, 22), date(2021, 2, 18), None),
        (date(2021, 2, 19), date(2021, 3, 18), None),
        (date(2021, 3, 19), date(2021, 4, 22), None),
        (date(2021, 4, 23), date(2021, 5, 21), None),

        (date(2021, 5, 22), date(2021, 6, 18), None),
        (date(2021, 6, 19), date(2021, 7, 20), None),
        (date(2021, 7, 21), date(2021, 8, 19), None),
        (date(2021, 8, 20), date(2021, 9, 20), None),
        (date(2021, 9, 21), date(2021, 10, 20), None),
        (date(2021, 10, 21), date(2021, 11, 18), None),
        (date(2021, 11, 19), date(2021, 12, 20), None),
        (date(2021, 12, 21), date(2022, 1, 21), None),
        (date(2022, 1, 22), date(2022, 2, 18), None),
        (date(2022, 2, 19), date(2022, 3, 18), None),
        (date(2022, 3, 19), date(2022, 4, 22), None),
        (date(2022, 4, 23), date(2022, 5, 20), None),

        (date(2022, 5, 21), date(2022, 6, 20), None),
        (date(2022, 6, 21), date(2022, 7, 20), None),
        (date(2022, 7, 21), date(2022, 8, 18), None),
        (date(2022, 8, 19), date(2022, 9, 20), None),
        (date(2022, 9, 21), date(2022, 10, 20), None),

        # NB SOMETIMES PADDING DATES ARE NEEDED because a YTD period expects n RP dates, but it only has < n.
        # If you do add padding dates, please add a comment like this above them.
    ]
)
