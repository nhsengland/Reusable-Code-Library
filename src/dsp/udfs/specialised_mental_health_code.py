import sys
from datetime import datetime, date
from typing import Union

from dsp.model.ods_record_codes import ODSRelType, ODSRoleScope, ODSOrgRole
from testdata.ref_data.providers import ODSProvider, SchoolsProvider

def specialised_mental_health_exists(
    code: str, point_in_time: Union[str, int, date, datetime] = None, default_org_list=None, follow_successions=True
):
    """
    A function that queries corporate ref data. Returns True if the organisation was live at the given point in time.
    By default follows succession for organisations
    Args:
        code: The code for the organisation to query
        point_in_time: The optional point in time the query is for
        default_org_list: An optional list of default orgs to accept as valid
        follow_successions: An optional parameter whether or not to follow organisation succession where this exists

    Returns:
        True if the organisation was live at the point in time, or in the default list

    """

    if not code:
        return False

    # if default_org_list:
    #     return is_default(code, default_org_list)

    if default_org_list and code in default_org_list:
        return True

    provider = ODSProvider()

    site_org = provider.get_org(code, point_in_time, follow_successions=follow_successions)
    if not site_org:
        return False

    return site_org.active_at(point_in_time)
