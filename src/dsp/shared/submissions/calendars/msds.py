from datetime import date

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars.common import SubmissionCalendar, SubmissionWindowConfig

SUBMISSION_CALENDAR = SubmissionCalendar.from_dates(
    DS.MSDS,
    SubmissionWindowConfig(end_add_months=1, mid_window = True),
    [
        # sw_opens, sw_closes, rp_start_dates
        # September 2018
        (date(2018, 10, 1), date(2018, 11, 30), [date(2018, 9, 1)]),
        # October 2018
        (date(2018, 11, 1), date(2018, 12, 31), [date(2018, 10, 1)]),
        # November 2018
        (date(2018, 12, 1), date(2019, 1, 31), [date(2018, 11, 1)]),
        # December 2018
        (date(2019, 1, 1), date(2019, 2, 28), [date(2018, 12, 1)]),
        # January 2019
        (date(2019, 2, 1), date(2019, 3, 31), [date(2019, 1, 1)]),
        # February 2019
        (date(2019, 3, 1), date(2019, 4, 30), [date(2019, 2, 1)]),
        # March 2019
        (date(2019, 4, 1), date(2019, 5, 31), [date(2019, 3, 1)]),
        # April 2019
        (date(2019, 5, 1), date(2019, 7, 12), [date(2019, 4, 1)]),
        # May 2019
        (date(2019, 6, 1), date(2019, 7, 31), [date(2019, 5, 1)]),
        # June 2019
        (date(2019, 7, 1), date(2019, 8, 31), [date(2019, 6, 1)]),
        # July 2019
        (date(2019, 8, 1), date(2019, 9, 30), [date(2019, 7, 1)]),
        # August 2019
        (date(2019, 9, 1), date(2019, 10, 31), [date(2019, 8, 1)]),
        # September 2019
        (date(2019, 10, 1), date(2019, 11, 30), [date(2019, 9, 1)]),
        # October 2019
        (date(2019, 11, 1), date(2019, 12, 31), [date(2019, 10, 1)]),
        # November 2019
        (date(2019, 12, 1), date(2020, 1, 31), [date(2019, 11, 1)]),
        # December 2019
        (date(2020, 1, 1), date(2020, 2, 29), [date(2019, 12, 1)]),
        # January 2020
        (date(2020, 2, 1), date(2020, 3, 31), [date(2020, 1, 1)]),
        # February 2020
        (date(2020, 3, 1), date(2020, 4, 30), [date(2020, 2, 1)]),
        # March 2020
        (date(2020, 4, 1), date(2020, 5, 31), [date(2020, 3, 1)]),
        # April 2020
        (date(2020, 5, 1), date(2020, 6, 30), [date(2020, 4, 1)]),
        # May 2020
        (date(2020, 6, 1), date(2020, 7, 31), [date(2020, 5, 1)]),
        # June 2020
        (date(2020, 7, 1), date(2020, 8, 31), [date(2020, 6, 1)]),
        # July 2020
        (date(2020, 8, 1), date(2020, 9, 30), [date(2020, 7, 1)]),
        # August 2020
        (date(2020, 9, 1), date(2020, 10, 31), [date(2020, 8, 1)]),
        # September 2020
        (date(2020, 10, 1), date(2020, 11, 30), [date(2020, 9, 1)]),
        # October 2020
        (date(2020, 11, 1), date(2020, 12, 31), [date(2020, 10, 1)]),
        # November 2020
        (date(2020, 12, 1), date(2021, 1, 31), [date(2020, 11, 1)]),
        # December 2020
        (date(2021, 1, 1), date(2021, 2, 28), [date(2020, 12, 1)]),
        # January 2021
        (date(2021, 2, 1), date(2021, 3, 31), [date(2021, 1, 1)]),
        # February 2021
        (date(2021, 3, 1), date(2021, 4, 30), [date(2021, 2, 1)]),
        # March 2021
        (date(2021, 4, 1), date(2021, 5, 31), [date(2021, 3, 1)]),
        # April 2021
        (date(2021, 5, 1), date(2021, 6, 30), [date(2021, 4, 1)]),
        # May 2021
        (date(2021, 6, 1), date(2021, 7, 31), [date(2021, 5, 1)]),
        # June 2021
        (date(2021, 7, 1), date(2021, 8, 31), [date(2021, 6, 1)]),
        # July 2021
        (date(2021, 8, 1), date(2021, 9, 30), [date(2021, 7, 1)]),
        # August 2021
        (date(2021, 9, 1), date(2021, 10, 31), [date(2021, 8, 1)]),
        # September 2021
        (date(2021, 10, 1), date(2021, 11, 30), [date(2021, 9, 1)]),
        # October 2021
        (date(2021, 11, 1), date(2021, 12, 31), [date(2021, 10, 1)]),
        # November 2021
        (date(2021, 12, 1), date(2022, 1, 31), [date(2021, 11, 1)]),
        # December 2021
        (date(2022, 1, 1), date(2022, 2, 28), [date(2021, 12, 1)]),
        # January 2021
        (date(2022, 2, 1), date(2022, 3, 31), [date(2022, 1, 1)]),
        # February 2022
        (date(2022, 3, 1), date(2022, 4, 30), [date(2022, 2, 1)]),
        # March 2022
        (date(2022, 4, 1), date(2022, 5, 31), [date(2022, 3, 1)]),
        # April 2022
        (date(2022, 5, 1), date(2022, 6, 30), [date(2022, 4, 1)]),
        # May 2022
        (date(2022, 6, 1), date(2022, 7, 31), [date(2022, 5, 1)]),
        # June 2022
        (date(2022, 7, 1), date(2022, 8, 31), [date(2022, 6, 1)]),
        # July 2022
        (date(2022, 8, 1), date(2022, 10, 7), [date(2022, 7, 1)]),

        # Now some dummy dates as padding, though when calendar website is updated it will likely say exactly these.

        # Aug 2022
        (date(2022, 9, 1), date(2022, 10, 31), [date(2022, 8, 1)]),
        # Sep 2022
        (date(2022, 10, 1), date(2022, 11, 30), [date(2022, 9, 1)]),
        # Oct 2022
        (date(2022, 11, 1), date(2022, 12, 31), [date(2022, 10, 1)]),
        # Nov 2022
        (date(2022, 12, 1), date(2023, 1, 31), [date(2022, 11, 1)]),
        # Dec 2022
        (date(2023, 1, 1), date(2023, 2, 28), [date(2022, 12, 1)]),
        # Jan 2023
        (date(2023, 2, 1), date(2023, 3, 31), [date(2023, 1, 1)]),
        # Feb 2023
        (date(2023, 3, 1), date(2023, 4, 30), [date(2023, 2, 1)]),
        # Mar 2023
        (date(2023, 4, 1), date(2023, 5, 31), [date(2023, 3, 1)]),
        # Apr 2023
        (date(2023, 5, 1), date(2023, 6, 30), [date(2023, 4, 1)]),
        # May 2023
        (date(2023, 6, 1), date(2023, 7, 31), [date(2023, 5, 1)]),
        # Jun 2023
        (date(2023, 7, 1), date(2023, 8, 31), [date(2023, 6, 1)]),
        # Jul 2023
        (date(2023, 8, 1), date(2023, 9, 30), [date(2023, 7, 1)]),
        # Aug 2023
        (date(2023, 9, 1), date(2023, 10, 31), [date(2023, 8, 1)]),
        # Sep 2023
        (date(2023, 10, 1), date(2023, 11, 30), [date(2023, 9, 1)]),
        # Oct 2023
        (date(2023, 11, 1), date(2023, 12, 31), [date(2023, 10, 1)]),
        # Nov 2023
        (date(2023, 12, 1), date(2024, 1, 31), [date(2023, 11, 1)]),
        # Dec 2023
        (date(2024, 1, 1), date(2024, 2, 29), [date(2023, 12, 1)]),
        # Jan 2024
        (date(2024, 2, 1), date(2024, 3, 31), [date(2024, 1, 1)]),
        # Feb 2024
        (date(2024, 3, 1), date(2024, 4, 30), [date(2024, 2, 1)]),
        # Mar 2024
        (date(2024, 4, 1), date(2024, 5, 31), [date(2024, 3, 1)]),
        # Apr 2024
        (date(2024, 5, 1), date(2024, 6, 30), [date(2024, 4, 1)]),
        # May 2024
        (date(2024, 6, 1), date(2024, 7, 31), [date(2024, 5, 1)]),
        # Jun 2024
        (date(2024, 7, 1), date(2024, 8, 31), [date(2024, 6, 1)]),
        # Jul 2024
        (date(2024, 8, 1), date(2024, 9, 30), [date(2024, 7, 1)]),
        # Aug 2024
        (date(2024, 9, 1), date(2024, 10, 31), [date(2024, 8, 1)]),
        # Sep 2024
        (date(2024, 10, 1), date(2024, 11, 30), [date(2024, 9, 1)]),
        # Oct 2024
        (date(2024, 11, 1), date(2024, 12, 31), [date(2024, 10, 1)]),
        # Nov 2024
        (date(2024, 12, 1), date(2025, 1, 31), [date(2024, 11, 1)]),
        # Dec 2024
        (date(2025, 1, 1), date(2025, 2, 28), [date(2024, 12, 1)]),
    ]
)
