from datetime import date

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars.common import SubmissionCalendar, SubmissionWindowConfig

SUBMISSION_CALENDAR = SubmissionCalendar.from_dates(
    DS.CSDS_V1_6,
    SubmissionWindowConfig(end_day=14, windows_per_rp=2),
    [
        # sw_opens, sw_closes, rp_start_dates

        # To support transition to version 1.6, the submission windows for {Jan primary} and {Jan refresh / Feb primary}
        # have been designed to allow an opportunity for Jan primary data to be submitted
        # and providers to be able to get post deadline extract reports to help data quality improvements.

        # January 2023 Primary (P) **First v1.6 submission**
        # (but had to include Dec Refresh as our cal logic doesn't think it's Prim/Refresh if there's only one)
        (date(2023, 2, 17), date(2023, 2, 28), [date(2022, 12, 1), date(2023, 1, 1)]),

        # February 2023 (P) & January 2023 Refresh (R)
        (date(2023, 3, 1), date(2023, 3, 14), [date(2023, 1, 1), date(2023, 2, 1)]),

        # March 2023 (P) & February 2023 Refresh (R)
        (date(2023, 3, 15), date(2023, 4, 18), [date(2023, 2, 1), date(2023, 3, 1)]),

        # Actual published dates added
        #  March 2023 Refresh (R) April 2023 Primary (P)
        (date(2023, 4, 19), date(2023, 5, 18), [date(2023, 3, 1), date(2023, 4, 1)]),
        #  April 2023 Refresh (R) May 2023 Primary (P)
        (date(2023, 5, 19), date(2023, 6, 14), [date(2023, 4, 1), date(2023, 5, 1)]),
        # May 2023 Refresh (R) June 2023 Primary (P)
        (date(2023, 6, 15), date(2023, 7, 14), [date(2023, 5, 1), date(2023, 6, 1)]),
        # June 2023 Refresh (R) July 2023 Primary (P)
        (date(2023, 7, 17), date(2023, 8, 14), [date(2023, 6, 1), date(2023, 7, 1)]),
        # July 2023 Refresh (R) August 2023 Primary (P)
        (date(2023, 8, 15), date(2023, 9, 14), [date(2023, 7, 1), date(2023, 8, 1)]),
        # August 2023 Refresh (R) September 2023 Primary (P)
        (date(2023, 9, 15), date(2023, 10, 13), [date(2023, 8, 1), date(2023, 9, 1)]),
        # September 2023 Refresh (R) October 2023 Primary (P)
        (date(2023, 10, 16), date(2023, 11, 14), [date(2023, 9, 1), date(2023, 10, 1)]),
        # October 2023 Refresh (R) November 2023 Primary (P)
        (date(2023, 11, 15), date(2023, 12, 14), [date(2023, 10, 1), date(2023, 11, 1)]),
        # November 2023 Refresh (R) December 2023 Primary (P)
        (date(2023, 12, 15), date(2024, 1, 15), [date(2023, 11, 1), date(2023, 12, 1)]),
        # December 2023 Refresh (R) January 2023 Primary (P)
        (date(2024, 1, 16), date(2024, 2, 14), [date(2023, 12, 1), date(2024, 1, 1)]),
        # January 2023 Refresh (R) February 2023 Primary (P)
        (date(2024, 2, 15), date(2024, 3, 14), [date(2024, 1, 1), date(2024, 2, 1)]),
        # February 2023 Refresh (R) March 2023 Primary (P)
        (date(2024, 3, 15), date(2024, 4, 15), [date(2024, 2, 1), date(2024, 3, 1)]),
        # March 2024 Refresh (R) April 2024 Primary (P)
        (date(2024, 4, 16), date(2024, 5, 15), [date(2024, 3, 1), date(2024, 4, 1)]),
        # April 2024 Refresh (R) May 2024 Primary (P)
        (date(2024, 5, 16), date(2024, 6, 14), [date(2024, 4, 1), date(2024, 5, 1)]),
        # May 2024 Refresh (R) June 2024 Primary (P)
        (date(2024, 6, 17), date(2024, 7, 12), [date(2024, 5, 1), date(2024, 6, 1)]),
        # June 2024 Refresh (R) July 2024 Primary (P)
        (date(2024, 7, 15), date(2024, 8, 14), [date(2024, 6, 1), date(2024, 7, 1)]),
        # July 2024 Refresh (R) August 2024 Primary (P)
        (date(2024, 8, 15), date(2024, 9, 13), [date(2024, 7, 1), date(2024, 8, 1)]),
        # August 2024 Refresh (R) September 2024 Primary (P)
        (date(2024, 9, 16), date(2024, 10, 14), [date(2024, 8, 1), date(2024, 9, 1)]),
        # September 2024 Refresh (R) October 2024 Primary (P)
        (date(2024, 10, 15), date(2024, 11, 14), [date(2024, 9, 1), date(2024, 10, 1)]),
        # October 2024 Refresh (R) November 2024 Primary (P)
        (date(2024, 11, 15), date(2024, 12, 13), [date(2024, 10, 1), date(2024, 11, 1)]),
        # November 2024 Refresh (R) December 2024 Primary (P)
        (date(2024, 12, 16), date(2025, 1, 15), [date(2024, 11, 1), date(2024, 12, 1)]),
        # December 2024 Refresh (R) January 2025 Primary (P)
        (date(2025, 1, 16), date(2025, 2, 14), [date(2024, 12, 1), date(2025, 1, 1)]),
        # January 2025 Refresh (R) February 2025 Primary (P)
        (date(2025, 2, 17), date(2025, 3, 14), [date(2025, 1, 1), date(2025, 2, 1)]),
        # February 2025 Refresh (R) March 2025 Primary (P)
        (date(2025, 3, 17), date(2025, 4, 14), [date(2025, 2, 1), date(2025, 3, 1)]),
    ]
)
