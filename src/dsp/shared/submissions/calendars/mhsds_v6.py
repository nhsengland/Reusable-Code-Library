# these are provisional dates awaiting the publication of the actual dates.
# real dates to be picked up in https://nhsd-jira.digital.nhs.uk/browse/DEP003-187 once the publication timetable is known

from datetime import date

from dsp.shared.constants import DS
from dsp.shared.submissions.calendars.common import SubmissionCalendar, SubmissionWindowConfig

SUBMISSION_CALENDAR = SubmissionCalendar.from_dates(
    DS.MHSDS_V6,
    SubmissionWindowConfig(start_day=21, end_day=20, ytd=True),
    [
    # These are test dates for testers
    #(date(2023, 12, 28), date(2024, 1, 24), None),
    #(date(2024, 1, 25), date(2024, 2, 26), None),
    #(date(2024, 2, 27), date(2024, 3, 26), None),
    #(date(2024, 3, 27), date(2024, 4, 24), None),
    #(date(2024, 4, 25), date(2024, 5, 28), None),
    # sw_opens, sw_closes, rp_start_dates
    (date(2024, 6, 1), date(2024, 6, 20), None), # Only April Activity will be accepted
    (date(2024, 6, 21), date(2024, 7, 20), None), # Only April & May Activity will be accepted
    (date(2024, 7, 21), date(2024, 8, 18), None), # Only April May and June  Activity will be accepted
    (date(2024, 8, 19), date(2024, 9, 20), None), # Only April May Jun and July Activity will be accepted
    (date(2024, 9, 21), date(2024, 10, 19), None), # and so on....
    (date(2024, 10, 20), date(2024, 11, 23), None),
    (date(2024, 11, 24), date(2024, 12, 27), None),
    (date(2024, 12, 28), date(2025, 1, 24), None),
    (date(2025, 1, 25), date(2025, 2, 26), None),
    (date(2025, 2, 27), date(2025, 3, 26), None),
    (date(2025, 3, 27), date(2025, 4, 24), None),
    (date(2025, 4, 25), date(2025, 5, 28), None)
    ]
)
# # # #