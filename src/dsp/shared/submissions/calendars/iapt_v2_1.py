from datetime import date

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars.common import SubmissionCalendar, SubmissionWindowConfig

SUBMISSION_CALENDAR = SubmissionCalendar.from_dates(
    DS.IAPT_V2_1,
    SubmissionWindowConfig(end_day=22, windows_per_rp=2),
    [
        # submission_window_opens, submission_window_closes, rp_start_dates (x2)
        # Apr 22 (P) only: (but had to include Mar as our cal logic doesn't think it's Prim/Refresh if there's only one)
        (date(2022, 5, 16), date(2022, 5, 27), [date(2022, 3, 1), date(2022, 4, 1)]),
        # May 22 (P) & Apr 22 (R)
        (date(2022, 6, 1), date(2022, 6, 29), [date(2022, 4, 1), date(2022, 5, 1)]),
        # May 2022 (R) and June 2022 (P)
        (date(2022, 7, 1), date(2022, 7, 27), [date(2022, 5, 1), date(2022, 6, 1)]),
        # June 2022 (R) and July 2022 (P)
        (date(2022, 8, 1), date(2022, 8, 24), [date(2022, 6, 1), date(2022, 7, 1)]),
        # July 2022 (R) and August 2022 (P)
        (date(2022, 9, 1), date(2022, 9, 28), [date(2022, 7, 1), date(2022, 8, 1)]),
        # August 2022 (R) and September 2022 (P)
        (date(2022, 10, 1), date(2022, 10, 26), [date(2022, 8, 1), date(2022, 9, 1)]),
        # September 2022 (R) and October 2022 (P)
        (date(2022, 11, 1), date(2022, 11, 24), [date(2022, 9, 1), date(2022, 10, 1)]),
        # October 2022 (R) and November 2022 (P)
        (date(2022, 12, 1), date(2022, 12, 28), [date(2022, 10, 1), date(2022, 11, 1)]),
        # November 2022 (R) and December 2022 (P)
        (date(2023, 1, 1), date(2023, 1, 25), [date(2022, 11, 1), date(2022, 12, 1)]),
        # December 2022 (R) and January 2023 (P)
        (date(2023, 2, 1), date(2023, 2, 22), [date(2022, 12, 1), date(2023, 1, 1)]),
        # January 2023 (R) and February 2023 (P)
        (date(2023, 3, 1), date(2023, 3, 27), [date(2023, 1, 1), date(2023, 2, 1)]),
        # February 2023 (R) and March 2023 (P)
        (date(2023, 4, 1), date(2023, 4, 26), [date(2023, 2, 1), date(2023, 3, 1)]),
        # March 2023 (R) and April 2023 (P)
        (date(2023, 5, 1), date(2023, 5, 25), [date(2023, 3, 1), date(2023, 4, 1)]),
        # April 2023 (R) and May 2023 (P)
        (date(2023, 6, 1), date(2023, 6, 28), [date(2023, 4, 1), date(2023, 5, 1)]),
        # May 2023 (R) and June 2023 (P)
        (date(2023, 7, 1), date(2023, 7, 27), [date(2023, 5, 1), date(2023, 6, 1)]),
        # June 2023 (R) and July 2023 (P)
        (date(2023, 8, 1), date(2023, 8, 29), [date(2023, 6, 1), date(2023, 7, 1)]),
        # July 2023 (R) and August 2023 (P)
        (date(2023, 9, 1), date(2023, 9, 28), [date(2023, 7, 1), date(2023, 8, 1)]),
        # August 2023 (R) and September 2023 (P)
        (date(2023, 10, 1), date(2023, 10, 26), [date(2023, 8, 1), date(2023, 9, 1)]),
        # September 2023 (R) and October 2023 (P)
        (date(2023, 11, 1), date(2023, 11, 27), [date(2023, 9, 1), date(2023, 10, 1)]),
        # October 2023 (R) and November 2023 (P)
        (date(2023, 12, 1), date(2023, 12, 28), [date(2023, 10, 1), date(2023, 11, 1)]),
        # November 2023 (R) and December 2023 (P)
        (date(2024, 1, 1), date(2024, 1, 25), [date(2023, 11, 1), date(2023, 12, 1)]),
        # December 2023 (R) and January 2024 (P)
        (date(2024, 2, 1), date(2024, 2, 27), [date(2023, 12, 1), date(2024, 1, 1)]),
        # January 2024 (R) and February 2024 (P)
        (date(2024, 3, 1), date(2024, 3, 27), [date(2024, 1, 1), date(2024, 2, 1)]),
        # February 2024 (R) and March 2024 (P)
        (date(2024, 4, 1), date(2024, 4, 25), [date(2024, 2, 1), date(2024, 3, 1)]),
        # March 2024 (R) and April 2024 (P)
        (date(2024, 5, 1), date(2024, 5, 29), [date(2024, 3, 1), date(2024, 4, 1)]),
        # April 2024 (R) and May 2024 (P)
        (date(2024, 6, 1), date(2024, 6, 27), [date(2024, 4, 1), date(2024, 5, 1)]),
        # May 2024 (R) and June 2024 (P)
        (date(2024, 7, 1), date(2024, 7, 25), [date(2024, 5, 1), date(2024, 6, 1)]),
        # June 2024 (R) and July 2024 (P)
        (date(2024, 8, 1), date(2024, 8, 28), [date(2024, 6, 1), date(2024, 7, 1)]),
        # July 2024 (R) and August 2024 (P)
        (date(2024, 9, 1), date(2024, 9, 26), [date(2024, 7, 1), date(2024, 8, 1)]),
        # August 2024 (R) and September 2024 (P)
        (date(2024, 10, 1), date(2024, 10, 28), [date(2024, 8, 1), date(2024, 9, 1)]),
        # September 2024 (R) and October 2024 (P)
        (date(2024, 11, 1), date(2024, 11, 28), [date(2024, 9, 1), date(2024, 10, 1)]),
        # October 2024 (R) and November 2024 (P)
        (date(2024, 12, 1), date(2024, 12, 23), [date(2024, 10, 1), date(2024, 11, 1)]),
        # November 2024 (R) and December 2024 (P)
        (date(2025, 1, 1), date(2025, 1, 29), [date(2024, 11, 1), date(2024, 12, 1)]),
        # December 2024 (R) and January 2025 (P)
        (date(2025, 2, 1), date(2025, 2, 27), [date(2024, 12, 1), date(2025, 1, 1)]),
        # January 2025 (R) and February 2025 (P)
        (date(2025, 3, 1), date(2025, 3, 27), [date(2025, 1, 1), date(2025, 2, 1)]),
    ]
)
