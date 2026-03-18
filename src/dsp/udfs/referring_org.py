from datetime import datetime, date
from typing import Union

from dsp.model.ods_record_codes import ODSReferringRoleType
from dsp.udfs.organisation import is_valid_org_role, DefaultOrgCodes
from dsp.shared import common
from testdata.ref_data.providers import ODSProvider


def is_empty_or_valid_referring_org(code: str, point_in_time: Union[str, int, date, datetime] = None) -> bool:
    """
    validates if referring organisation is none or one of the DefaultOrgCodes or present in reference data.

    Args:
        code: org code to check in ref data
        point_in_time: point in time to evaluate
    Returns:
        True if org code is present in ref data or none
        False if not present in ref data
    """

    if not code:
        return True

    code = (code or '').strip().upper()
    if (not code) or (code in common.enumlike_values(DefaultOrgCodes)):
        return True

    return is_valid_org_role(code, point_in_time, *common.enumlike_values(ODSReferringRoleType))


def is_valid_ref_org_active(code, point_in_time: Union[str, int, date, datetime]):
    """
    check if the referring organisation code is valid in point in time, validates only if it is valid referring organisation code
    :param code: referring organisation code to validate against
    :param point_in_time: date to evaluate against
    :return: False if valid referring organisation code not active in point in time, any invalid referring organisation code returns True
    """
    code = (code or '').strip().upper()
    if not code or not point_in_time:
        return False

    if code in common.enumlike_values(DefaultOrgCodes):
        return True

    provider = ODSProvider()

    org = provider.get_org(code)

    if not org:
        return True

    if not org.has_role(*common.enumlike_values(ODSReferringRoleType)):
        return True

    return org.has_role_at(point_in_time, *common.enumlike_values(ODSReferringRoleType))
