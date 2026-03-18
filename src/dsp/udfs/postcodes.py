import datetime
from math import hypot
from typing import Optional

from dsp.model.ons_record import ONSRecord
from testdata.ref_data.providers import ONSProvider


def msoa_at(postcode, event_date=None):

    if not postcode:
        return None

    provider = ONSProvider()

    return provider.get_msoa_at(postcode, event_date)


def lsoa_at(postcode, event_date=None):

    if not postcode:
        return None

    provider = ONSProvider()

    return provider.get_lsoa_at(postcode, event_date)


def country_at(postcode, event_date):

    if not postcode:
        return None

    provider = ONSProvider()

    return provider.get_country_at(postcode, event_date)


def unitary_authority_at(postcode, event_date):

    if not postcode:
        return None

    provider = ONSProvider()

    return provider.get_unitary_authority_at(postcode, event_date)


def ccg_at(postcode, event_date):

    if not postcode:
        return None

    provider = ONSProvider()

    return provider.get_ccg_code_at(postcode, event_date)


def icb_at(postcode, event_date):

    if not postcode:
        return None

    provider = ONSProvider()

    return provider.get_icb_code_at(postcode, event_date)


def eastings_at(postcode, point_in_time=None):

    if not postcode:
        return None

    provider = ONSProvider()

    return provider.get_eastings_at(postcode, point_in_time)


def northings_at(postcode, point_in_time=None):

    if not postcode:
        return None

    provider = ONSProvider()

    return provider.get_northings_at(postcode, point_in_time)


def active_at(postcode: str, point_in_time: datetime.date = None):
    """
    Validates that a postcode was active in the reference data at the given point in time.
    If no point in time is specified the current date & time is used.

    Args:
        postcode: Postcode, spaces optional
        point_in_time:
    Returns:
         False if postcode is false
         True if the postcode is valid for the specified point in time
         False if the postcode is invalid for the specified point in time
    """

    if not postcode:
        return False

    provider = ONSProvider()

    return provider.active_at(postcode, point_in_time)


def county_at(postcode, point_in_time=None):

    if not postcode:
        return None

    provider = ONSProvider()

    return provider.get_county_at(postcode, point_in_time)


def pcds_at(postcode, point_in_time=None):

    if not postcode:
        return None

    provider = ONSProvider()

    return provider.get_pcds_at(postcode, point_in_time)


def electoral_ward_at(postcode, point_in_time=None):

    if not postcode:
        return None

    provider = ONSProvider()

    return provider.get_electoral_ward_at(postcode, point_in_time)


def distance_between_postcodes(from_postcode: str, to_postcode: str,
                               point_in_time: datetime.date = None) -> Optional[float]:

    if None in [from_postcode, to_postcode]:
        return None

    from_northings_at = northings_at(from_postcode, point_in_time)
    from_eastings_at = eastings_at(from_postcode, point_in_time)

    to_northings_at = northings_at(to_postcode, point_in_time)
    to_eastings_at = eastings_at(to_postcode, point_in_time)

    if None in [from_northings_at, from_eastings_at, to_northings_at, to_eastings_at]:
        return None

    return hypot(int(from_northings_at) - int(to_northings_at), int(from_eastings_at) - int(to_eastings_at))


def is_pseudo_country_postcode(postcode: str) -> bool:
    if not postcode or not postcode.startswith('ZZ99'):
        return False
    return active_at(postcode)


def pcds_from_postcode_at(postcode: str, point_in_time: datetime.date) -> Optional[str]:

    postcode = (postcode or '').upper().strip()
    rec = ONSProvider().get(postcode.replace(' ', ''))  # type: ONSRecord
    if not rec:
        return None
    return rec.get_pcds_at(point_in_time)
