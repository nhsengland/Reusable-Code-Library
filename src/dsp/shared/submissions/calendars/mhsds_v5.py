from datetime import date

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars.common import SubmissionCalendar, SubmissionWindowConfig

SUBMISSION_CALENDAR = SubmissionCalendar.from_dates(
    DS.MHSDS_GENERIC,
    SubmissionWindowConfig(start_day=21, end_day=20, ytd=True),
    [
        # sw_opens, sw_closes, rp_start_dates
        (date(2021, 5, 22), date(2021, 6, 18), None),
        (date(2021, 6, 19), date(2021, 7, 20), None),
        (date(2021, 7, 21), date(2021, 8, 19), None),
        (date(2021, 8, 20), date(2021, 9, 20), None),
        (date(2021, 9, 21), date(2021, 10, 20), None),
        (date(2021, 10, 21), date(2021, 11, 18), None),
        (date(2021, 11, 19), date(2021, 12, 20), [date(2021, 10, 1), date(2021, 11, 1)]),
        (date(2021, 12, 21), date(2022, 1, 21), [date(2021, 10, 1), date(2021, 11, 1), date(2021, 12, 1)]),
        (date(2022, 1, 22), date(2022, 2, 18), [date(2021, 10, 1), date(2021, 11, 1), date(2021, 12, 1), date(2022, 1, 1)]),
        (date(2022, 2, 19), date(2022, 3, 18), [date(2021, 10, 1), date(2021, 11, 1), date(2021, 12, 1), date(2022, 1, 1), date(2022, 2, 1)]),
        (date(2022, 3, 19), date(2022, 4, 22), [date(2021, 10, 1), date(2021, 11, 1), date(2021, 12, 1), date(2022, 1, 1), date(2022, 2, 1), date(2022, 3, 1)]),
        (date(2022, 4, 23), date(2022, 5, 20), [date(2021, 10, 1), date(2021, 11, 1), date(2021, 12, 1), date(2022, 1, 1), date(2022, 2, 1), date(2022, 3, 1), date(2022, 4, 1)]),
        (date(2022, 5, 21), date(2022, 6, 22), None),
        (date(2022, 6, 23), date(2022, 7, 20), None),
        (date(2022, 7, 21), date(2022, 8, 18), None),
        (date(2022, 8, 19), date(2022, 9, 20), None),
        (date(2022, 9, 21), date(2022, 10, 20), None),
        (date(2022, 10, 21), date(2022, 11, 18), None),
        (date(2022, 11, 19), date(2022, 12, 20), None),
        (date(2022, 12, 21), date(2023, 1, 20), None),
        (date(2023, 1, 21), date(2023, 2, 20), None),
        (date(2023, 2, 21), date(2023, 3, 20), None),
        (date(2023, 3, 21), date(2023, 4, 24), None),
        (date(2023, 4, 25), date(2023, 5, 22), None),
        (date(2023, 5, 23), date(2023, 6, 20), None),
        (date(2023, 6, 21), date(2023, 7, 20), None),
        (date(2023, 7, 21), date(2023, 8, 18), None),
        (date(2023, 8, 19), date(2023, 9, 20), None),
        (date(2023, 9, 21), date(2023, 10, 19), None),
        (date(2023, 10, 20), date(2023, 11, 23), None),
        (date(2023, 11, 24), date(2023, 12, 27), None),
        (date(2023, 12, 28), date(2024, 1, 24), None),
        (date(2024, 1, 25), date(2024, 2, 26), None),
        (date(2024, 2, 27), date(2024, 3, 26), None),
        (date(2024, 3, 27), date(2024, 4, 24), None),
        (date(2024, 4, 25), date(2024, 5, 28), None),
    ]
)
# # # #