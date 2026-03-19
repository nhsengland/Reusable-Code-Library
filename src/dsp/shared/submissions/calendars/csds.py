from datetime import date

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars.common import SubmissionCalendar, SubmissionWindowConfig

SUBMISSION_CALENDAR = SubmissionCalendar.from_dates(
    DS.CSDS_GENERIC,
    SubmissionWindowConfig(end_day=22, windows_per_rp=2),
    [
        # sw_opens, sw_closes, rp_start_dates
        # Use a historic date for fake pilot primary submission window
        (date(2020, 4, 13), date(2020, 5, 9), [date(2019, 10, 1), date(2019, 11, 1)]),
        # Pilot submission window December 19
        (date(2020, 5, 13), date(2020, 6, 9), [date(2019, 11, 1), date(2019, 12, 1)]),
        # June 20 (P) & May 20 (R)  - Fake for Portal Testing
        (date(2020, 7, 1), date(2020, 7, 31), [date(2020, 5, 1), date(2020, 6, 1)]),
        # July 20 (P) & June 20 (R)
        (date(2020, 8, 1), date(2020, 8, 31), [date(2020, 6, 1), date(2020, 7, 1)]),
        # August 20 (P) & July 20 (R)
        (date(2020, 9, 1), date(2020, 9, 30), [date(2020, 7, 1), date(2020, 8, 1)]),
        # September 20 (P) & August 20 (R)
        (date(2020, 10, 1), date(2020, 10, 31), [date(2020, 8, 1), date(2020, 9, 1)]),
        # October 20 (P) & September 20 (R)
        (date(2020, 11, 1), date(2020, 11, 30), [date(2020, 9, 1), date(2020, 10, 1)]),
        # November 20 (P) & October 20 (R)
        (date(2020, 12, 1), date(2020, 12, 31), [date(2020, 10, 1), date(2020, 11, 1)]),
        # December 20 (P) & November 20 (R)
        (date(2021, 1, 1), date(2021, 1, 31), [date(2020, 11, 1), date(2020, 12, 1)]),
        # January 21 (P) & December 20 (R)
        (date(2021, 2, 1), date(2021, 2, 28), [date(2020, 12, 1), date(2021, 1, 1)]),
        # February 21 (P) & January 21 (R)
        (date(2021, 3, 1), date(2021, 3, 31), [date(2021, 1, 1), date(2021, 2, 1)]),
        # March 21 (P) & February 21 (R)
        (date(2021, 4, 1), date(2021, 4, 30), [date(2021, 2, 1), date(2021, 3, 1)]),
        # April 21 (P) & March 21 (R)
        (date(2021, 5, 1), date(2021, 5, 31), [date(2021, 3, 1), date(2021, 4, 1)]),
        # May 2021 Primary (P) & April 2021  Refresh (R)
        (date(2021, 6, 1), date(2021, 6, 30), [date(2021, 4, 1), date(2021, 5, 1)]),
        # June 2021 Primary (P) & May 2021 Refresh (R)
        (date(2021, 7, 1), date(2021, 7, 31), [date(2021, 5, 1), date(2021, 6, 1)]),
        # July 2021 (P) & June 2021 Refresh (R)
        (date(2021, 8, 1), date(2021, 8, 31), [date(2021, 6, 1), date(2021, 7, 1)]),
        # August 2021 (P) & July 2021 Refresh (R)
        (date(2021, 9, 1), date(2021, 9, 22), [date(2021, 7, 1), date(2021, 8, 1)]),
        # September 2021 (P) & August 2021 Refresh (R)
        (date(2021, 9, 23), date(2021, 10, 22), [date(2021, 8, 1), date(2021, 9, 1)]),
        # October 2021 (P) & September 2021 Refresh (R)
        (date(2021, 10, 23), date(2021, 11, 22), [date(2021, 9, 1), date(2021, 10, 1)]),
        # November 2021 (P) & October 2021 Refresh (R)
        (date(2021, 11, 23), date(2021, 12, 22), [date(2021, 10, 1), date(2021, 11, 1)]),
        # December 2021 (P) & November 2021 Refresh (R)
        (date(2021, 12, 23), date(2022, 1, 25), [date(2021, 11, 1), date(2021, 12, 1)]),
        # January 2022 (P) & December 2021 Refresh (R)
        (date(2022, 1, 26), date(2022, 2, 22), [date(2021, 12, 1), date(2022, 1, 1)]),
        # February 2022 (P) & January 2022 Refresh (R)
        (date(2022, 2, 23), date(2022, 3, 22), [date(2022, 1, 1), date(2022, 2, 1)]),
        # March 2022 (P) & February 2022 Refresh (R)
        (date(2022, 3, 23), date(2022, 4, 26), [date(2022, 2, 1), date(2022, 3, 1)]),
        # April 2022 (P) & March 2022 Refresh (R)
        (date(2022, 4, 27), date(2022, 5, 24), [date(2022, 3, 1), date(2022, 4, 1)]),
        # May 2022 (P) & April 2022 Refresh (R)
        (date(2022, 5, 25), date(2022, 6, 16), [date(2022, 4, 1), date(2022, 5, 1)]),
        # June 2022 (P) & May 2022 Refresh (R)
        (date(2022, 6, 17), date(2022, 7, 14), [date(2022, 5, 1), date(2022, 6, 1)]),

        # July 2022 (P) & June 2022 Refresh (R)
        (date(2022, 7, 15), date(2022, 8, 12), [date(2022, 6, 1), date(2022, 7, 1)]),

        # August 2022 (P) & July 2022 Refresh (R)
        (date(2022, 8, 13), date(2022, 9, 14), [date(2022, 7, 1), date(2022, 8, 1)]),

        # September 2022 (P) & August 2022 Refresh (R)
        (date(2022, 9, 15), date(2022, 10, 14), [date(2022, 8, 1), date(2022, 9, 1)]),

        # October 2022 (P) & September 2022 Refresh (R)
        (date(2022, 10, 15), date(2022, 11, 14), [date(2022, 9, 1), date(2022, 10, 1)]),

        # November 2022 (P) & October 2022 Refresh (R)
        (date(2022, 11, 15), date(2022, 12, 14), [date(2022, 10, 1), date(2022, 11, 1)]),

        # December 2022 (P) & November 2022 Refresh (R)
        (date(2022, 12, 15), date(2023, 1, 16), [date(2022, 11, 1), date(2022, 12, 1)]),

        # December 2022 Refresh (R) **Last v1.5 submission**
        # (but had to include Jan Primary as our cal logic doesn't think it's Prim/Refresh if there's only one)
        (date(2023, 1, 17), date(2023, 2, 14), [date(2022, 12, 1), date(2023, 1, 1)]),

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

    ]
)
