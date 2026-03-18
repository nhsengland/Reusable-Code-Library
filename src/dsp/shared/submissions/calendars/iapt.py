from datetime import date

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars.common import SubmissionCalendar, SubmissionWindowConfig

SUBMISSION_CALENDAR = SubmissionCalendar.from_dates(
    DS.IAPT_GENERIC,
    SubmissionWindowConfig(end_day=22, windows_per_rp=2),
    [
        # sw_opens, sw_closes, rp_start_dates
        # Use a historic date for fake pilot primary submission window
        (date(2020, 4, 13), date(2020, 5, 9), [date(2019, 11, 1), date(2019, 12, 1)]),
        # Pilot submission window December 19
        (date(2020, 5, 13), date(2020, 6, 9), [date(2019, 12, 1), date(2020, 1, 1)]),
        # June 20 (P) & May 20 (R) - Fake for Portal Testing
        (date(2020, 7, 1), date(2020, 7, 31), [date(2020, 5, 1), date(2020, 6, 1)]),
        # July 20 (P) & June 20 (R)
        (date(2020, 8, 1), date(2020, 8, 27), [date(2020, 6, 1), date(2020, 7, 1)]),
        # August 20 (P) & July 20 (R)
        (date(2020, 9, 1), date(2020, 9, 25), [date(2020, 7, 1), date(2020, 8, 1)]),
        # September 20 (P) & August 20 (R)
        (date(2020, 10, 1), date(2020, 10, 27), [date(2020, 8, 1), date(2020, 9, 1)]),
        # October 20 (P) & September 20 (R)
        (date(2020, 11, 1), date(2020, 11, 26), [date(2020, 9, 1), date(2020, 10, 1)]),
        # November 20 (P) & October 20 (R)
        (date(2020, 11, 27), date(2020, 12, 29), [date(2020, 9, 1), date(2020, 10, 1), date(2020, 11, 1)]),
        # December 20 (P) & November 20 (R)
        (date(2021, 1, 1), date(2021, 1, 28), [date(2020, 11, 1), date(2020, 12, 1)]),
        # January 21 (P) & December 20 (R)
        (date(2021, 2, 1), date(2021, 2, 25), [date(2020, 12, 1), date(2021, 1, 1)]),
        # February 21 (P) & January 21 (R)
        (date(2021, 3, 1), date(2021, 3, 25), [date(2021, 1, 1), date(2021, 2, 1)]),
        # March 21 (P) & February 21 (R)
        (date(2021, 4, 1), date(2021, 4, 29), [date(2021, 2, 1), date(2021, 3, 1)]),
        # April 21 (P) & March 21 (R)
        (date(2021, 5, 1), date(2021, 5, 28), [date(2021, 3, 1), date(2021, 4, 1)]),
        # May 21 (P) & April 21 (R)
        (date(2021, 6, 1), date(2021, 6, 25), [date(2021, 4, 1), date(2021, 5, 1)]),
        # June 21 (P) & May 21 (R)
        (date(2021, 7, 1), date(2021, 7, 27), [date(2021, 5, 1), date(2021, 6, 1)]),
        # July 21 (P) & September 20 (R) **SPECIAL SUBMISSION WINDOW**
        (date(2021, 8, 1), date(2021, 8, 26), [
            date(2020, 9, 1),
            date(2020, 10, 1),
            date(2020, 11, 1),
            date(2020, 12, 1),
            date(2021, 1, 1),
            date(2021, 2, 1),
            date(2021, 3, 1),
            date(2021, 4, 1),
            date(2021, 5, 1),
            date(2021, 6, 1),
            date(2021, 7, 1),
        ]),
        # Aug 21 (P) & July 21 (R)
        (date(2021, 9, 1), date(2021, 9, 27), [date(2021, 7, 1), date(2021, 8, 1)]),
        # Sept 21 (P) & Aug 21 (R)
        (date(2021, 10, 1), date(2021, 10, 27), [date(2021, 8, 1), date(2021, 9, 1)]),
        # Oct 21 (P) & Sept 21 (R)
        (date(2021, 11, 1), date(2021, 11, 25), [date(2021, 9, 1), date(2021, 10, 1)]),
        # Nov 21 (P) & Oct 21 (R)
        (date(2021, 12, 1), date(2021, 12, 29), [date(2021, 10, 1), date(2021, 11, 1)]),
        # Dec 21 (P) & Nov 21 (R)
        (date(2022, 1, 1), date(2022, 1, 28), [date(2021, 11, 1), date(2021, 12, 1)]),
        # Jan 22 (P) & Dec 21 (R)
        (date(2022, 2, 1), date(2022, 2, 25), [date(2021, 12, 1), date(2022, 1, 1)]),
        # Feb 22 (P) & Jan 22 (R)
        (date(2022, 3, 1), date(2022, 3, 25), [date(2022, 1, 1), date(2022, 2, 1)]),
        # Mar 22 (P) & Feb 22 (R)
        (date(2022, 4, 1), date(2022, 4, 29), [date(2022, 2, 1), date(2022, 3, 1)]),
        # Mar 22 (R) only: (open for first half of month)
        (date(2022, 5, 1), date(2022, 5, 13), [date(2022, 3, 1), date(2022, 4, 1)]),
        # Dummy submission window to allow May monthend processing to complete
        (date(2022, 6, 1), date(2022, 6, 16), [date(2022, 4, 1), date(2022, 5, 1)]),
        ### end of IAPT v2.0 submissions ###
    ]
)
