import sys
from datetime import datetime, date
from typing import List, Union

from pyspark.sql.functions import lit, array

from dsp.model.ods_record_codes import ODSRelType, ODSRoleScope, ODSOrgRole
from testdata.ref_data.providers import ODSProvider, SchoolsProvider


class DefaultOrgCodes:
    NOT_APPLICABLE = 'X99998'
    NOT_KNOWN = 'X99999'
    # Commissioner Code for Ministry of Defence (MoD) Healthcare
    MOD_HEALTHCARE_COMMISSIONER = 'XMD00'
    # Non-NHS UK provider where no ORGANISATION CODE / ORGANISATION IDENTIFIER has been issued
    NON_NHS_UK_PROVIDER_WITH_NO_ORG_CODE = '89999'
    # Non-UK provider where no ORGANISATION CODE / ORGANISATION IDENTIFIER has been issued
    NON_UK_PROVIDER_WITH_NO_ORG_CODE = '89997'
    # Primary Care Organisation Not Applicable (Overseas Visitors)
    PRIMARY_CARE_ORG_NA_OVERSEAS_VISITORS = 'X98'
    # High Level Health Geography/Primary Care Organisation of Residence Not Known
    PRIMARY_CARE_ORG_OF_RESIDENCE_NOT_KNOWN = 'Q99'


def organisation_name(code: str):
    code = (code or '').strip().upper()
    if not code:
        return None

    provider = ODSProvider()

    org = provider.get_org(code)

    if not org:
        return None

    return org.name()


_gp_name_default_codes = {
    # Valid Scottish legacy codes. The truncated descriptions match those present in the reference data.
    'S30063': 'Grampian Out Of Hours Service',
    'S30152': 'Virtual Practice Only',
    'S99942': 'PATIENTS REGISTERED WITH',
    'S99957': 'PATIENTS NOT REGISTERED',
    'S99961': 'PATIENTS WHERE PRACTICE C',
    'S99976': 'BRITISH ARMED FORCES PATIENT',
    'S99995': 'PATIENTS REGISTERED WITH',
    'S99999': 'Unknown',
    # Currently valid codes
    'V81997': 'Not Registered',
    'V81998': 'Not Applicable',
    'V81999': 'Not Known',
}


def gp_practice_name(code):
    # type: (str) -> str

    code = (code or '').strip().upper()

    default_desc = _gp_name_default_codes.get(code)
    if default_desc:
        return default_desc

    return organisation_name(code)


def is_valid_gp_practice_active(code, point_in_time: Union[str, int, date, datetime]):
    """
    check if the gp code is valid in point in time, validates only if it is valid GP
    :param code: GP Code to validate against
    :param point_in_time: date to evaluate against
    :return: False if valid GP code not active in point in time any invalid GP returns True
    """

    code = (code or '').strip().upper()
    if not code or not point_in_time:
        return False

    if code in _gp_name_default_codes.keys():
        return True

    provider = ODSProvider()

    org = provider.get_org(code)

    if not org:
        return True

    if not org.has_role(ODSOrgRole.GP_PRACTICE, ODSOrgRole.PRESCRIBING_COST_CENTRE):
        return True

    return org.has_role_at(point_in_time, ODSOrgRole.GP_PRACTICE, ODSOrgRole.PRESCRIBING_COST_CENTRE)


def is_practice_code_valid(code, point_in_time: Union[str, int, date, datetime] = None, case_sensitive: bool = False):
    """
    check if the gp code is valid
    :param code: GP Code
    :param point_in_time: diagnostic test date
    :param case_sensitive: validate exactly what has been submitted, without amending to uppercase first?
    :return: bool
    """

    if not case_sensitive:
        code = (code or '').strip().upper()
    if not code:
        return False

    if code in _gp_name_default_codes.keys():
        return True

    provider = ODSProvider()

    org = provider.get_org(code, point_in_time)

    if not org:
        return False

    return org.has_role_at(point_in_time, ODSOrgRole.GP_PRACTICE, ODSOrgRole.PRESCRIBING_COST_CENTRE) if point_in_time \
        else org.has_role(ODSOrgRole.GP_PRACTICE, ODSOrgRole.PRESCRIBING_COST_CENTRE)


_site_code_description_defaults = {
    '89999': 'Non-NHS UK Provider',
    '89997': 'Non-UK Provider',
    'R9998': 'Not a Hospital Site'
}


def site_code_description(code):
    # type: (str) -> str

    code = (code or '').strip().upper()

    default_desc = _site_code_description_defaults.get(code)
    if default_desc:
        return default_desc

    return organisation_name(code)


def is_site_code_valid(
    code: str, point_in_time: Union[str, int, date, datetime] = None, follow_successions: bool = True
) -> bool:
    """
    Return true if the site code is part of a default list or exists in the reference database
    :param code: Provider site code
    :param point_in_time: diagnostic test date
    :param follow_successions: if set to true validator will follow successors, returning true when successor was active in the point_in_time
    :return: bool
    """

    if code in _site_code_description_defaults:
        return True

    return organisation_exists(code, point_in_time, follow_successions=follow_successions)


def is_valid_site_code_active(code, point_in_time: Union[str, int, date, datetime]):
    """
    check if site code is valid in point in time, validates only if it is valid site
    :param code: site Code to validate against
    :param point_in_time: date to evaluate against
    :return: False if valid site code not active in point in time, True if valid site code active at point in time any
     invalid site code returns True
    """

    code = (code or '').strip().upper()
    if not code or not point_in_time:
        return False

    if code in _site_code_description_defaults:
        return True

    provider = ODSProvider()

    org = provider.get_org(code)

    if not org:
        return True

    return org.active_at(point_in_time)


def postcode_from_org_code(code, point_in_time: Union[str, int, date, datetime] = None) -> Union[str, None]:
    """Get the postcode of an org code at a particular time.

    Args:
        code: The org code

    Keyword Args:
        point_in_time: Has two effects:
            - (recursively) pick successor organisation if ``point_in_time`` is after an organisation has closed but has a successor.
            - scans through the contact information history to pick out the postcode that was valid at the time.
                - ``point_in_time`` earlier than the earliest valid postcode will cause this function to return ``None``.
                - ``point_in_time`` later than the last valid postcode will cause this function to return the last valid postcode.

    Returns:
        The relevant postcode or None.
    """

    code = (code or '').strip().upper()
    provider = ODSProvider()

    # Handles the finding successor information
    org = provider.get_org(code, point_in_time)

    if not org:
        return None

    if not point_in_time:
        return org.postcode()

    point_in_time = org.get_point_in_time(point_in_time)

    if point_in_time < min([hp["from"] for hp in org.historical_postcodes()], default=sys.maxsize):
        # earlier than than the earliest postcode
        return None

    # If time inside any historical postcode bounds, return that
    # postcode
    for hp in org.historical_postcodes():
        # hp["to"] = None -> current postcode
        if point_in_time >= hp["from"] and (hp["to"] is None or point_in_time <= hp["to"]):
            return hp["postcode"]

    # If we get here then point_in_time is later than the last
    # postcode. This should only be the case if the org has closed
    # down and the final postcode had an end date earlier than
    # point_in_time.  In this case, return the most recent postcode.
    return org.postcode()


DEFAULT_SHA = None

_SHA_ROLE_IDS = [
    ODSRoleScope.HEALTH_AUTHORITY_HA,
    ODSOrgRole.STRATEGIC_HEALTH_AUTHORITY_SITE
]


def sha_code_from_site_code(site_code: str, point_in_time: Union[str, int, date, datetime] = None) -> Union[str, None]:
    site_code = (site_code or '').strip().upper()

    provider_code = provider_code_from_site_code(site_code, point_in_time)

    return sha_code_from_org_code(provider_code, point_in_time)


def sha_code_from_org_code(org_code: str, point_in_time: Union[str, int, date, datetime] = None) -> Union[str, None]:
    org_code = (org_code or '').strip().upper()

    if not org_code:
        return DEFAULT_SHA

    ods_provider = ODSProvider()

    ods_record = ods_provider.get_org(org_code, point_in_time)

    if not ods_record:
        return DEFAULT_SHA

    sha_org = ods_record.rel_org_at_of_type(
        point_in_time=point_in_time,
        rel_type=ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF,
        role_ids=_SHA_ROLE_IDS
    )

    return sha_org or DEFAULT_SHA


def ccg_code_from_gp_practice_code(code: str, point_in_time: Union[str, int, date, datetime] = None):
    ods_org = ODSProvider().get_org(code, point_in_time)
    if not ods_org:
        return None

    return ods_org.rel_org_at_of_type(
        point_in_time=point_in_time,
        rel_type=ODSRelType.IS_COMMISSIONED_BY,
        role_ids=ODSOrgRole.CLINICAL_COMMISSIONING_GROUP
    )


def pct_code_from_gp_practice_code(code: str, point_in_time: Union[str, int, date, datetime] = None):
    ods_org = ODSProvider().get_org(code, point_in_time)
    if not ods_org:
        return None

    return ods_org.latest_rel_org_at_of_type(
        point_in_time=point_in_time,
        rel_type=ODSRelType.IS_COMMISSIONED_BY,
        role_ids=ODSOrgRole.PRIMARY_CARE_TRUST
    )


def sub_icb_code_from_gp_practice_code(code: str, point_in_time: Union[str, int, date, datetime] = None):
    """
        Org Relationship Lineage to get ICB code from GP Practice code
    """
    ods_org = ODSProvider().get_org(code, point_in_time)
    if not ods_org:
        return None

    return ods_org.rel_org_at_of_type(
        point_in_time=point_in_time,
        rel_type=ODSRelType.IS_COMMISSIONED_BY,
        role_ids=ODSOrgRole.CLINICAL_COMMISSIONING_GROUP
    )


def provider_code_from_site_code(code: str, point_in_time: Union[str, int, date, datetime] = None):
    if not code:
        return None

    provider = ODSProvider()

    site_org = provider.get_org(code, point_in_time)
    if not site_org:
        return None

    if not site_org.nhs_organisation_at(point_in_time):
        return code

    return code[:3]


def organisation_exists(
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


def organisation_exists_anytime(
    code: str, follow_successions=True
):
    """
    A function that queries corporate ref data. Returns True if the organisation exists in data.
    By default follows succession for organisations
    Args:
        code: The code for the organisation to query
        follow_successions: An optional parameter whether or not to follow organisation succession where this exists

    Returns:
        True if the organisation was live at the point in time, or in the default list

    """

    if not code:
        return False

    if code in _site_code_description_defaults:
        return True

    provider = ODSProvider()

    site_org = provider.get_org(code, follow_successions=follow_successions)
    if site_org:
        return True

    return False


def organisation_exists_anytime_without_successor(code: str, default_org_list=None):
    if not code:
        return False

    if default_org_list and code in default_org_list:
        return True

    provider = ODSProvider()
    org_code = provider.get(code)
    if org_code:
        return True

    return False


def get_org_role_types_without_successor(org_code: str, point_in_time: Union[str, int, date, datetime] = None,
                       follow_successions: bool = False):
    """
    Returns list of organisation role types for the given org_code
    Args:
        org_code: org_code of org for which roles need to be returned
        point_in_time: Optional, point in time to evaluate
        follow_successions is set to False as required for IAPT validation

    Returns:
        List of org roles
    """
    if not org_code:
        return []

    provider = ODSProvider()
    org = provider.get_org(org_code, point_in_time, follow_successions=follow_successions)

    if not org:
        return []
    return [role["type"] for role in org.roles()]


def get_org_role_types(org_code: str, point_in_time: Union[str, int, date, datetime] = None):
    """
    Returns list of organisation role types for the given org_code
    Args:
        org_code: org_code of org for which roles need to be returned
        point_in_time: Optional, point in time to evaluate

    Returns:
        List of org roles
    """
    if not org_code:
        return []

    provider = ODSProvider()
    org = provider.get_org(org_code, point_in_time)

    if not org:
        return []
    return [role["type"] for role in org.roles()]


def is_valid_org_role(org_code: str, point_in_time: Union[str, int, date, datetime], *role_types,
                      follow_successions: bool = True) -> bool:
    """
    validates if org for the given org_code had role present from given role or role list at the given point in time
    Args:
        org_code: org_code of org for which role needs to be check
        point_in_time: point in time to evaluate
        *role_types: list of roles to check for the org
        follow_successions (bool): whether to attempt to resolve successor relationships if valid and found ?

    Returns:
        True: if org has a role from the given role list

    """
    if not org_code:
        return False

    provider = ODSProvider()

    org = provider.get_org(org_code, point_in_time, follow_successions=follow_successions)

    if not org:
        return False

    return org.has_role_at(point_in_time, *role_types) if point_in_time else org.has_role(*role_types)


def is_valid_ccg(ccg_code: str, point_in_time: Union[str, int, date, datetime]) -> bool:
    return is_valid_org_role(ccg_code, point_in_time, ODSOrgRole.CLINICAL_COMMISSIONING_GROUP)


def is_valid_school(code: str, point_in_time: Union[str, int, date, datetime] = None) -> bool:
    if not code:
        return False

    provider = SchoolsProvider()

    return provider.active_at(code, point_in_time)


def find_related_org_with_role(code: str, rel_type, role_ids: Union[List[str], None],
                               from_org_type: str = None, point_in_time: Union[str, int, date, datetime] = None,
                               follow_successions: bool = True):
    code = (code or '').strip().upper()
    ods_org = ODSProvider().get_org(code, point_in_time, follow_successions=follow_successions)
    if not ods_org:
        return None
    return ods_org.rel_org_at_of_type(point_in_time, rel_type, from_org_type, role_ids)


def stp_code_from_ccg_code(code: str, point_in_time: Union[str, int, date, datetime] = None) -> str:
    """
        Org Relationship Lineage to get stp code from ccg code:
            Use CCST
    """
    return find_related_org_with_role(
        code=code,
        rel_type=ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF,
        role_ids=[ODSRoleScope.STRATEGIC_PARTNERSHIP],
        from_org_type='CC',
        point_in_time=point_in_time
    )


def icb_code_from_sub_icb_code(code: str, point_in_time: Union[str, int, date, datetime] = None) -> str:
    """
        Org Relationship Lineage to get ICB code from sub-ICB code
    """
    return find_related_org_with_role(
        code=code,
        rel_type=ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF,
        role_ids=[ODSRoleScope.REGION_GEOGRAPHY, ODSRoleScope.STRATEGIC_PARTNERSHIP],
        from_org_type='CC',
        point_in_time=point_in_time
    )


def region_code_from_ccg_code(code: str, point_in_time: Union[str, int, date, datetime] = None) -> str:
    """
        Org Relationship Lineage to get region code from ccg code:
            Use CCST, STCE if lineage exists
            Else CCCE if lineage exists
            Else CCOA if lineage exists
    """
    # Try CCST, STCE
    stp_code = stp_code_from_ccg_code(code, point_in_time)
    region_code = find_related_org_with_role(
        code=stp_code,
        rel_type=ODSRelType.IS_A_SUB_DIVISION_OF,
        role_ids=[ODSRoleScope.NHS_ENGLAND_COMMISSIONING_REGION],
        from_org_type='ST',
        point_in_time=point_in_time
    )
    if region_code:
        return region_code

    # Try CCCE
    region_code = find_related_org_with_role(
        code=code,
        rel_type=ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF,
        role_ids=[ODSRoleScope.NHS_ENGLAND_COMMISSIONING_REGION],
        from_org_type='CC',
        point_in_time=point_in_time
    )
    if region_code:
        return region_code

    # Try CCOA
    region_code = find_related_org_with_role(
        code=code,
        rel_type=ODSRelType.IS_DIRECTED_BY,
        role_ids=[ODSRoleScope.OTHER_STATUTORY_AUTHORITY_OSA],
        from_org_type='CC',
        point_in_time=point_in_time
    )
    return region_code


def region_code_from_sub_icb_code(code: str, point_in_time: Union[str, int, date, datetime] = None) -> str:
    """
        Org Relationship Lineage to get region code from subICB code.
    """
    icb_code = icb_code_from_sub_icb_code(code, point_in_time)
    region_code = find_related_org_with_role(
        code=icb_code,
        rel_type=ODSRelType.IS_A_SUB_DIVISION_OF,
        role_ids=[ODSRoleScope.NHS_ENGLAND_COMMISSIONING_REGION],
        from_org_type='ST',
        point_in_time=point_in_time
    )
    if region_code:
        return region_code

    region_code = find_related_org_with_role(
        code=code,
        rel_type=ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF,
        role_ids=[ODSRoleScope.NHS_ENGLAND_COMMISSIONING_REGION],
        from_org_type='CC',
        point_in_time=point_in_time
    )
    if region_code:
        return region_code

    region_code = find_related_org_with_role(
        code=code,
        rel_type=ODSRelType.IS_DIRECTED_BY,
        role_ids=[ODSRoleScope.OTHER_STATUTORY_AUTHORITY_OSA],
        from_org_type='CC',
        point_in_time=point_in_time
    )

    return region_code
