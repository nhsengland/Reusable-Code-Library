import datetime
import math
from calendar import isleap
from typing import Optional, List

from dsp.shared import common


def _date_as_float(the_date: datetime.date) -> float:
    """
    Converts a date into a float in the format year.seconds

    This algorithm effectively smears the number of seconds in a year evenly over leap years and non-leap years,
    allowing for date comparisons that conform to UK government requirements around leap years.

    params:
        the_date: A timestamp as a datetime.datetime object

    returns:
        A float in the format 'year.seconds'
    """
    size_of_day = 1. / 366.
    size_of_second = size_of_day / (24. * 60. * 60.)

    days_from_jan1 = the_date - datetime.datetime(the_date.year, 1, 1)
    if not isleap(the_date.year) and days_from_jan1.days >= 31 + 28:
        days_from_jan1 += datetime.timedelta(1)

    return the_date.year + days_from_jan1.days * size_of_day + days_from_jan1.seconds * size_of_second


def age_in_years_at_event(dob: str, event_date: str, return_none_if_error=False) -> Optional[int]:
    """
    Calculates the age of a person on a given date.

    params:
        dob: A string that can be parsed as a date by dateutil.parser.parse
        event_date: A string that can be parsed as a date by dateutil.parser.parse

    returns:
        For invalid inputs, None
        For valid inputs, the person's age in years as an integer.
    """
    default_value = None

    if not dob or not event_date:
        return default_value

    try:
        dob_dt = common.parse_date(dob)
        event_dt = common.parse_date(event_date)
    except ValueError as e:
        if return_none_if_error:
            return None
        else:
            raise e

    try:
        return int(math.floor(_date_as_float(event_dt) - _date_as_float(dob_dt)))
    except ValueError:
        return default_value


def age_band_5_years(age):
    # type: (int) -> str
    if age is None or age < 0:
        return 'Age not known'

    if age > 89:
        return '90 and over'

    lbnd = 5 * int(math.floor(age / 5))
    ubnd = lbnd + 4
    return '{}-{}'.format(lbnd, ubnd)


def age_band_10_years(age):
    # type: (int) -> str
    if age is None or age < 0:
        return 'Age not known'

    if age > 89:
        return '90 and over'

    lbnd = 10 * int(math.floor(age / 10))
    ubnd = lbnd + 9
    return '{}-{}'.format(lbnd, ubnd)


def _band_age(age: Optional[int], bands: List[int]) -> str:
    if age is None or age < 0:
        return 'Age not known'

    last_ceil = 0
    for ceil in bands:
        if age <= ceil:
            return '{}-{}'.format(last_ceil + 1, ceil) if last_ceil != ceil else str(ceil)
        last_ceil = ceil

    return '{} and over'.format(bands[-1] + 1)


def age_band_children_inc0(age):
    # type: (int) -> str
    return _band_age(age, [0, 4, 9, 15])


def age_band_inc_50_74(age):
    # type: (int) -> str
    return _band_age(age, [-1, 18, 49, 74])


def age_band_inc_working_age_16_60(age):
    # type: (int) -> str
    return _band_age(age, [-1, 15, 60])


def age_band_inc_working_age_16_64(age):
    # type: (int) -> str
    return _band_age(age, [-1, 15, 64])
