from typing import Dict, Any

# from dsp.shared.submissions.calendars import csds
# import dsp.shared.submissions.calendars.csds_v1_6
# import dsp.shared.submissions.calendars.iapt
# import dsp.shared.submissions.calendars.iapt_v2_1
# import dsp.shared.submissions.calendars.mhsds_v5
# import dsp.shared.submissions.calendars.mhsds_v6
# import dsp.shared.submissions.calendars.msds
# import dsp.shared.submissions.calendars.mhsds_ytd

from dsp.shared.submissions.calendars import csds
from dsp.shared.submissions.calendars import csds_v1_6
from dsp.shared.submissions.calendars import iapt
from dsp.shared.submissions.calendars import iapt_v2_1
from dsp.shared.submissions.calendars import mhsds_v5
from dsp.shared.submissions.calendars import mhsds_v6
from dsp.shared.submissions.calendars import mhsds_ytd
from dsp.shared.submissions.calendars import msds

from dsp.shared.constants import DS, FeatureToggles, DatasetVersions
from dsp.shared.logger import log_action
from dsp.shared.submissions.calendars.common import ReportingPeriod, SubmissionWindow, CalendarException, SubmissionCalendar

_CALENDAR_MODULES = [
    (DS.CSDS_GENERIC, csds),
    (DS.CSDS_V1_6, csds_v1_6),
    (DS.IAPT_GENERIC, iapt),
    (f'{DS.IAPT_GENERIC}:{DatasetVersions.IAPT_V2_0}', iapt),
    (DS.IAPT_V2_1, iapt_v2_1),
    (f'{DS.IAPT_GENERIC}:{DatasetVersions.IAPT_V2_1}', iapt_v2_1),
    (DS.MHSDS_GENERIC, mhsds_ytd),
    (DS.MHSDS_V1_TO_V5_AS_V6, mhsds_v5),
    (DS.MHSDS_V6, mhsds_v6),
    (DS.MSDS, msds),
    (f'{DS.CSDS_GENERIC}:{DatasetVersions.CSDS_V1_6}', csds),
    (f'{DS.CSDS_V1_6}:{DatasetVersions.CSDS_V1_6}', csds_v1_6),

]

_SUBMISSION_CALENDARS = {
    name: module.SUBMISSION_CALENDAR for name, module in _CALENDAR_MODULES
}


@log_action(log_args=['dataset_id'])
def submission_calendar(
        dataset_id: str, feature_toggles: Dict[str, Any], version: str = None
) -> SubmissionCalendar:
    version = version or ''
    feature_toggles = feature_toggles or {}

    calendar = _SUBMISSION_CALENDARS.get(f'{dataset_id}:{version}', _SUBMISSION_CALENDARS.get(f'{dataset_id}'))
    if not calendar:
        raise CalendarException(f'submission calendar not found for {dataset_id} {version}')

    return calendar
