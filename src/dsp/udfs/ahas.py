import binascii
import os
import re
from datetime import date, datetime, timedelta
from typing import Any, List, Optional, Union

import pytz
from pyspark.sql import Column
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType, TimestampType
from typing_extensions import TypedDict


def generate_audit_id(fmt: str = "%Y%m%d%H") -> int:
    return int(datetime.now().strftime(fmt))


class InclusionRow(TypedDict):
    partyear: str
    year: int
    period: int
    as_at_date: str
    activity_from: str
    activity_to: str
    activity_quarter_end: str
    fyear: str


def inclusion_date_str(year: int, month: int, day: int) -> str:
    """
    Automatically returns the correct timestamp for legacy oracles Derivation of inclusion
    date cutoff.

    Args:
        year (int): Year of the inclusion date
        month (int): Month of the inclusion date
        day (int): Day of the inclusion date

    Returns:
        str: Datetime formatted string in the format of: %Y-%m-%d %H:%M:%S
    Raises:
        ValueError: When the supplied integers cannot get converted to a datetime
    """
    local_tz = pytz.timezone("Europe/London")
    utc_dt: datetime = datetime(year, month, day, 17, 30)
    offset: Union[timedelta, None] = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz).utcoffset()
    if offset is not None:
        offset_dt = utc_dt - offset
        return offset_dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        raise ValueError(f"{year}-{month}-{day} is not a valid set of arguments")


"""
In accordance with British Summertime Legacy HES uses a timestamp of 16:30 for the winter
months and a timestamp of 17:30 in the summer months. As AHAS uses UTC there is no such
difference but in order to reconcile the two, the summer periods will change to reflect this

2019 	31 March -	27 October
2020 	29 March -	25 October
2021 	28 March -	31 October

Daylight Savings:
https://www.gov.uk/when-do-the-clocks-change
Inclusion Calendar:
https://digital.nhs.uk/services/secondary-uses-service-sus/payment-by-results-guidance

"""
# This should not change
inclusion_calendar_header = [
    "partyear",
    "year",
    "period",
    "as_at_date",
    "activity_from",
    "activity_to",
    "activity_quarter_end",
    "fyear",
]
inclusion_calendar_rows = [
    # partyear,year,period, as_at_date,                   activity_from, activity_to,  quarter_end, fyear
    ["201813", 2018, 13, inclusion_date_str(2019, 5, 20), "2018-04-01", "2019-03-31", "2019-03-31", "1819"],
    # 2019/20 calendar
    ["201901", 2019, 1, inclusion_date_str(2019, 5, 20), "2019-04-01", "2019-04-30", "2019-06-30", "1920"],
    ["201902", 2019, 2, inclusion_date_str(2019, 6, 19), "2019-04-01", "2019-05-31", "2019-06-30", "1920"],
    ["201903", 2019, 3, inclusion_date_str(2019, 7, 17), "2019-04-01", "2019-06-30", "2019-06-30", "1920"],
    ["201904", 2019, 4, inclusion_date_str(2019, 8, 19), "2019-04-01", "2019-07-31", "2019-09-30", "1920"],
    ["201905", 2019, 5, inclusion_date_str(2019, 9, 18), "2019-04-01", "2019-08-31", "2019-09-30", "1920"],
    ["201906", 2019, 6, inclusion_date_str(2019, 10, 17), "2019-04-01", "2019-09-30", "2019-09-30", "1920"],
    ["201907", 2019, 7, inclusion_date_str(2019, 11, 19), "2019-04-01", "2019-10-31", "2019-12-31", "1920"],
    ["201908", 2019, 8, inclusion_date_str(2019, 12, 17), "2019-04-01", "2019-11-30", "2019-12-31", "1920"],
    ["201909", 2019, 9, inclusion_date_str(2020, 1, 20), "2019-04-01", "2019-12-31", "2019-12-31", "1920"],
    ["201910", 2019, 10, inclusion_date_str(2020, 2, 19), "2019-04-01", "2020-01-31", "2020-03-31", "1920"],
    ["201911", 2019, 11, inclusion_date_str(2020, 3, 18), "2019-04-01", "2020-02-29", "2020-03-31", "1920"],
    ["201912", 2019, 12, inclusion_date_str(2020, 4, 21), "2019-04-01", "2020-03-31", "2020-03-31", "1920"],
    ["201913", 2019, 13, inclusion_date_str(2020, 5, 20), "2019-04-01", "2020-03-31", "2020-03-31", "1920"],
    # 2020/21 calendar
    ["202001", 2020, 1, inclusion_date_str(2020, 5, 20), "2020-04-01", "2020-04-30", "2020-06-30", "2021"],
    ["202002", 2020, 2, inclusion_date_str(2020, 6, 17), "2020-04-01", "2020-05-31", "2020-06-30", "2021"],
    ["202003", 2020, 3, inclusion_date_str(2020, 7, 17), "2020-04-01", "2020-06-30", "2020-06-30", "2021"],
    ["202004", 2020, 4, inclusion_date_str(2020, 8, 19), "2020-04-01", "2020-07-31", "2020-09-30", "2021"],
    ["202005", 2020, 5, inclusion_date_str(2020, 9, 17), "2020-04-01", "2020-08-31", "2020-09-30", "2021"],
    ["202006", 2020, 6, inclusion_date_str(2020, 10, 19), "2020-04-01", "2020-09-30", "2020-09-30", "2021"],
    ["202007", 2020, 7, inclusion_date_str(2020, 11, 18), "2020-04-01", "2020-10-31", "2020-12-31", "2021"],
    ["202008", 2020, 8, inclusion_date_str(2020, 12, 16), "2020-04-01", "2020-11-30", "2020-12-31", "2021"],
    ["202009", 2020, 9, inclusion_date_str(2021, 1, 20), "2020-04-01", "2020-12-31", "2020-12-31", "2021"],
    ["202010", 2020, 10, inclusion_date_str(2021, 2, 17), "2020-04-01", "2021-01-31", "2021-03-31", "2021"],
    ["202011", 2020, 11, inclusion_date_str(2021, 3, 17), "2020-04-01", "2021-02-28", "2021-03-31", "2021"],
    ["202012", 2020, 12, inclusion_date_str(2021, 4, 21), "2020-04-01", "2021-03-31", "2021-03-31", "2021"],
    ["202013", 2020, 13, inclusion_date_str(2021, 5, 20), "2020-04-01", "2021-03-31", "2021-03-31", "2021"],
    ["202013", 2020, 14, inclusion_date_str(2021, 5, 20), "2020-04-01", "2021-03-31", "2021-03-31", "2021"],
    # 2021/22 calendar
    ["202101", 2021, 1, inclusion_date_str(2021, 5, 20), "2021-04-01", "2021-04-30", "2021-06-30", "2122"],
    ["202102", 2021, 2, inclusion_date_str(2021, 6, 17), "2021-04-01", "2021-05-31", "2021-06-30", "2122"],
    ["202103", 2021, 3, inclusion_date_str(2021, 7, 19), "2021-04-01", "2021-06-30", "2021-06-30", "2122"],
    ["202104", 2021, 4, inclusion_date_str(2021, 8, 18), "2021-04-01", "2021-07-31", "2021-09-30", "2122"],
    ["202105", 2021, 5, inclusion_date_str(2021, 9, 17), "2021-04-01", "2021-08-31", "2021-09-30", "2122"],
    ["202106", 2021, 6, inclusion_date_str(2021, 10, 19), "2021-04-01", "2021-09-30", "2021-09-30", "2122"],
    ["202107", 2021, 7, inclusion_date_str(2021, 11, 17), "2021-04-01", "2021-10-31", "2021-12-31", "2122"],
    ["202108", 2021, 8, inclusion_date_str(2021, 12, 16), "2021-04-01", "2021-11-30", "2021-12-31", "2122"],
    ["202109", 2021, 9, inclusion_date_str(2022, 1, 20), "2021-04-01", "2021-12-31", "2021-12-31", "2122"],
    ["202110", 2021, 10, inclusion_date_str(2022, 2, 17), "2021-04-01", "2022-01-31", "2022-03-31", "2122"],
    ["202111", 2021, 11, inclusion_date_str(2022, 3, 17), "2021-04-01", "2022-02-28", "2022-03-31", "2122"],
    ["202112", 2021, 12, inclusion_date_str(2022, 4, 21), "2021-04-01", "2022-03-31", "2022-03-31", "2122"],
    ["202113", 2021, 13, inclusion_date_str(2022, 5, 19), "2021-04-01", "2022-03-31", "2022-03-31", "2122"],
    ["202114", 2021, 14, inclusion_date_str(2022, 5, 19), "2021-04-01", "2022-03-31", "2022-03-31", "2122"],
    # 2022/23 calendar
    ["202201", 2022, 1, inclusion_date_str(2022, 5, 19), "2022-04-01", "2022-04-30", "2022-06-30", "2223"],
    ["202202", 2022, 2, inclusion_date_str(2022, 6, 21), "2022-04-01", "2022-05-31", "2022-06-30", "2223"],
    ["202203", 2022, 3, inclusion_date_str(2022, 7, 19), "2022-04-01", "2022-06-30", "2022-06-30", "2223"],
    ["202204", 2022, 4, inclusion_date_str(2022, 8, 17), "2022-04-01", "2022-07-31", "2022-09-30", "2223"],
    # Queens funeral caused shift in date
    # ["202205", 2022, 5, inclusion_date_str(2022, 9, 19), "2022-04-01", "2022-08-31", "2022-09-30", "2223"],
    ["202205", 2022, 5, inclusion_date_str(2022, 9, 20), "2022-04-01", "2022-08-31", "2022-09-30", "2223"],
    ["202206", 2022, 6, inclusion_date_str(2022, 10, 19), "2022-04-01", "2022-09-30", "2022-09-30", "2223"],
    ["202207", 2022, 7, inclusion_date_str(2022, 11, 17), "2022-04-01", "2022-10-31", "2022-12-31", "2223"],
    ["202208", 2022, 8, inclusion_date_str(2022, 12, 16), "2022-04-01", "2022-11-30", "2022-12-31", "2223"],
    ["202209", 2022, 9, inclusion_date_str(2023, 1, 19), "2022-04-01", "2022-12-31", "2022-12-31", "2223"],
    ["202210", 2022, 10, inclusion_date_str(2023, 2, 17), "2022-04-01", "2023-01-31", "2023-03-31", "2223"],
    ["202211", 2022, 11, inclusion_date_str(2023, 3, 17), "2022-04-01", "2023-02-28", "2023-03-31", "2223"],
    ["202212", 2022, 12, inclusion_date_str(2023, 4, 21), "2022-04-01", "2023-03-31", "2023-03-31", "2223"],
    ["202213", 2022, 13, inclusion_date_str(2023, 5, 18), "2022-04-01", "2023-03-31", "2023-03-31", "2223"],
    ["202214", 2022, 14, inclusion_date_str(2023, 5, 18), "2022-04-01", "2023-03-31", "2023-03-31", "2223"],
    # 2023/24 calendar
    ["202301", 2023, 1, inclusion_date_str(2023, 5, 18), "2023-04-01", "2023-04-30", "2023-06-30", "2324"],
    ["202302", 2023, 2, inclusion_date_str(2023, 6, 19), "2023-04-01", "2023-05-31", "2023-06-30", "2324"],
    ["202303", 2023, 3, inclusion_date_str(2023, 7, 19), "2023-04-01", "2023-06-30", "2023-06-30", "2324"],
    ["202304", 2023, 4, inclusion_date_str(2023, 8, 17), "2023-04-01", "2023-07-31", "2023-09-30", "2324"],
    ["202305", 2023, 5, inclusion_date_str(2023, 9, 19), "2023-04-01", "2023-08-31", "2023-09-30", "2324"],
    ["202306", 2023, 6, inclusion_date_str(2023, 10, 18), "2023-04-01", "2023-09-30", "2023-09-30", "2324"],
    ["202307", 2023, 7, inclusion_date_str(2023, 11, 17), "2023-04-01", "2023-10-31", "2023-12-31", "2324"],
    ["202308", 2023, 8, inclusion_date_str(2023, 12, 18), "2023-04-01", "2023-11-30", "2023-12-31", "2324"],
    ["202309", 2023, 9, inclusion_date_str(2024, 1, 18), "2023-04-01", "2023-12-31", "2023-12-31", "2324"],
    ["202310", 2023, 10, inclusion_date_str(2024, 2, 19), "2023-04-01", "2024-01-31", "2024-03-31", "2324"],
    ["202311", 2023, 11, inclusion_date_str(2024, 3, 19), "2023-04-01", "2024-02-29", "2024-03-31", "2324"],
    ["202312", 2023, 12, inclusion_date_str(2024, 4, 18), "2023-04-01", "2024-03-31", "2024-03-31", "2324"],
    ["202313", 2023, 13, inclusion_date_str(2024, 5, 20), "2023-04-01", "2024-03-31", "2024-03-31", "2324"],
    ["202314", 2023, 14, inclusion_date_str(2024, 5, 20), "2023-04-01", "2024-03-31", "2024-03-31", "2324"],
    # 2024/25 calendar
    ["202401", 2024, 1, inclusion_date_str(2024, 5, 20), "2024-04-01", "2024-04-30", "2024-06-30", "2425"],
    ["202402", 2024, 2, inclusion_date_str(2024, 6, 19), "2024-04-01", "2024-05-31", "2024-06-30", "2425"],
    ["202403", 2024, 3, inclusion_date_str(2024, 7, 17), "2024-04-01", "2024-06-30", "2024-06-30", "2425"],
    ["202404", 2024, 4, inclusion_date_str(2024, 8, 19), "2024-04-01", "2024-07-31", "2024-09-30", "2425"],
    ["202405", 2024, 5, inclusion_date_str(2024, 9, 18), "2024-04-01", "2024-08-31", "2024-09-30", "2425"],
    ["202406", 2024, 6, inclusion_date_str(2024, 10, 17), "2024-04-01", "2024-09-30", "2024-09-30", "2425"],
    ["202407", 2024, 7, inclusion_date_str(2024, 11, 19), "2024-04-01", "2024-10-31", "2024-12-31", "2425"],
    ["202408", 2024, 8, inclusion_date_str(2024, 12, 17), "2024-04-01", "2024-11-30", "2024-12-31", "2425"],
    ["202409", 2024, 9, inclusion_date_str(2025, 1, 20), "2024-04-01", "2024-12-31", "2024-12-31", "2425"],
    ["202410", 2024, 10, inclusion_date_str(2025, 2, 19), "2024-04-01", "2025-01-31", "2025-03-31", "2425"],
    ["202411", 2024, 11, inclusion_date_str(2025, 3, 19), "2024-04-01", "2025-02-28", "2025-03-31", "2425"],
    ["202412", 2024, 12, inclusion_date_str(2025, 4, 17), "2024-04-01", "2025-03-31", "2025-03-31", "2425"],
    ["202413", 2024, 13, inclusion_date_str(2025, 5, 20), "2024-04-01", "2025-03-31", "2025-03-31", "2425"],
    ["202414", 2024, 14, inclusion_date_str(2025, 5, 20), "2024-04-01", "2025-03-31", "2025-03-31", "2425"],
]
inclusion_calendar: List[InclusionRow] = []
for row in inclusion_calendar_rows:
    row_dict: InclusionRow = {
        header: row[header_index] for header_index, header in enumerate(inclusion_calendar_header)
    }
    inclusion_calendar.append(row_dict)

DateValue = Union[str, date, datetime, DateType, TimestampType]


def get_inclusion_calendar_row(calendar_year: Union[int, str], month: Union[int, str]) -> InclusionRow:
    """
    Pass in a year and month to get the correct values for that inclusion year and period

    Args:
        calendar_year (int): Month of the inclusion year you want (1-14)
        year (int): Year of the inclusion calendar to search

    Returns:
        InclusionRow: A dict containing various inclusion calendar values

    """

    calendar_year = int(calendar_year)
    month = int(month)
    inclusion_years = {x["year"] for x in inclusion_calendar}
    pretty_calendar = " Year | Month | Inclusion Date\n------+-------+--------------------\n"
    for row in inclusion_calendar:
        pretty_calendar += f" {row['year']} |  {str(row['period']).ljust(2)}   | {row['as_at_date']}\n"
        if row["year"] == calendar_year and row["period"] == month:
            return row
    else:
        print(pretty_calendar)
        if calendar_year not in inclusion_years:
            raise ValueError("The year you provided is not in the inclusion calendar and must be added")
        elif not (1 <= month <= 14):
            raise ValueError("The month you provided is wrong and must be 1 to 14")
        raise ValueError(f"No combination found for Year: {calendar_year} Month: {month}\n{pretty_calendar}")


def date_as_string(date_value: DateValue, can_be_none: bool = False, local_time: bool = False) -> Optional[str]:
    """
    Converts a date, datetime to a string format

    Args:
        date_value (str, date, datetime, DateType, TimestampType): The value to convert.
        can_be_none (bool): Whether the date value can be None.
        local_time (bool): If true, the provided date value is passed through
            inclusion_date_str function which will adjust the timestamp to be the correct
            timezone for the inclusion calendar.

    Returns:
         str: A string representation of the date value.
    """

    if isinstance(date_value, (datetime, TimestampType)):
        if local_time:
            return inclusion_date_str(date_value.year, date_value.month, date_value.day)
        return date_value.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(date_value, (date, DateType)):
        if local_time:
            return inclusion_date_str(date_value.year, date_value.month, date_value.day)
        return date_value.strftime("%Y-%m-%d")
    elif date_value is None and can_be_none:
        return None
    elif isinstance(date_value, str):
        date_value = date_value.replace("T", " ").replace("Z", "")
        date_regex_string = r"^((19|20)\d\d[- \/.](0[1-9]|1[012])[- \/.](0[1-9]|[12][0-9]|3[01])( \d{2}:\d{2}:\d{2})?)"
        exception_string = f"""Incorrect date format: {date_value}; should start with YYYY-MM-DD
                            and optionally have a timestamp"""
        if not date_value:
            return date_value
        # search for a date part in the string
        match = re.search(date_regex_string, date_value)
        if not match:
            # assert match, f"timestamp is '{date_value}'"
            raise ValueError(exception_string + f"\ntimestamp is '{date_value}'")

        try:
            if len(match.group()) == 19:
                date_value = datetime.strptime(match.group(), "%Y-%m-%d %H:%M:%S")
                if local_time:
                    return inclusion_date_str(date_value.year, date_value.month, date_value.day)
                return date_value.strftime("%Y-%m-%d %H:%M:%S")
            else:
                date_value = datetime.strptime(match.group(), "%Y-%m-%d")
                if local_time:
                    return inclusion_date_str(date_value.year, date_value.month, date_value.day)
                return date_value.strftime("%Y-%m-%d")
        except ValueError:
            raise ValueError(exception_string)

    raise TypeError(f"Expecting str, date or datetime but got {type(date_value)}")


def get_inclusion_row(
    activity_date: DateValue,
    effective_from: DateValue,
    effective_to: DateValue,
    as_at_date: DateValue,
    year: int,
    raw_filter: bool = False,
) -> InclusionRow:
    """
    Gets the row from the inclusion calendar given the activity_date, effective_from and effective_to fields

    1. Activity date must be between activity from and activity to (inclusive? )
    2. Effective from must be before inclusion date (what if it is the same?)
    3. Effective to must be null or after the inclusion date (what if it is the same?)
    Args:
        activity_date: The date of the activity for the record.
        effective_from: The date of the effective_from, when the record is valid from.
        effective_to: The date of the effective_to, when the record is valid to.
        as_at_date: The date for when the inclusion is to be run for, this should be a valid inclusion date.
        year: The year for this to be run for.
        raw_filter: Flag for capturing raw data by ignoring upper bound of activity date
    Returns:
        dict: Dictionary containing the matched inclusion data or an empty dictionary.

    """
    activity_date = date_as_string(activity_date)
    effective_from = date_as_string(effective_from)
    effective_to = date_as_string(effective_to, can_be_none=True)
    as_at_date = date_as_string(as_at_date, can_be_none=True, local_time=True)
    if as_at_date is None:
        as_at_date = ""
    if as_at_date not in [r.get("as_at_date")[: len(as_at_date)] for r in inclusion_calendar]:
        raise Exception(f"as_at_date must be an inclusion date: {as_at_date}")
    # try and match to a row in the inclusion data
    for inclusion_row in inclusion_calendar:
        if inclusion_row.get("as_at_date")[: len(as_at_date)] == as_at_date and inclusion_row.get("year") == year:
            if effective_from < inclusion_row.get("as_at_date"):
                if effective_to is None or effective_to > inclusion_row.get("as_at_date"):
                    if not raw_filter and inclusion_row.get("activity_to") >= activity_date >= inclusion_row.get(
                        "activity_from"
                    ):
                        return inclusion_row
                    elif raw_filter and activity_date >= inclusion_row.get("activity_from"):
                        return inclusion_row
    # Return an empty dict if no match found.
    return {}


def derive_month_period(
    activity_date: DateValue,
    effective_from: DateValue,
    effective_to: DateValue,
    as_at_date: DateValue,
    year: int,
    raw_filter: bool = False,
) -> Optional[int]:
    """
    Helper to derive the Month Period that matches HES functionality

    Args:
        activity_date (DateValue): the activity date for a record
        effective_from (DateValue): the date from where the record is live or effective from
        effective_to (DateValue): the date from where the record is live or effective to
        as_at_date: The date for when the inclusion is to be run for.
        year: The year for this to be run for.
        raw_filter: Flag for capturing raw data by ignoring upper bound of activity date
    Returns:
        Int: The month period for the record
    """
    inclusion_row = get_inclusion_row(activity_date, effective_from, effective_to, as_at_date, year, raw_filter)
    return inclusion_row.get("period")


def derive_year_period(
    activity_date: DateValue,
    effective_from: DateValue,
    effective_to: DateValue,
    as_at_date: DateValue,
    year: int,
    raw_filter: bool = False,
) -> Optional[int]:
    """
    Helper to derive the Month Period that matches HES functionality

    Args:
        activity_date (DateValue): the activity date for a record
        effective_from (DateValue): the date from where the record is live or effective from
        effective_to (DateValue): the date from where the record is live or effective to
        as_at_date: The date for when the inclusion is to be run for.
        year: The year for this to be run for.
        raw_filter: Flag for capturing raw data by ignoring upper bound of activity date
    Returns:
        Int: The month period for the record
    """
    inclusion_row = get_inclusion_row(activity_date, effective_from, effective_to, as_at_date, year, raw_filter)
    return inclusion_row.get("year")


def derive_part_year(
    activity_date: DateValue,
    effective_from: DateValue,
    effective_to: DateValue,
    as_at_date: DateValue,
    year: int,
    raw_filter: bool = False,
) -> Optional[str]:
    """
    Helper to derive the Year that matches HES functionality

    Args:
        activity_date (DateValue): the activity date for a record
        effective_from (DateValue): the date from where the record is live or effective from
        effective_to (DateValue): the date from where the record is live or effective to
        as_at_date: The date for when the inclusion is to be run for.
        year: The year for this to be run for.
        raw_filter: Flag for capturing raw data by ignoring upper bound of activity date
    Returns:
        Str: The formatted year and month period
    """
    inclusion_row = get_inclusion_row(activity_date, effective_from, effective_to, as_at_date, year, raw_filter)
    return inclusion_row.get("partyear")


def derive_submission_period(col: Column, as_at_date: DateValue, year: int) -> Optional[int]:
    """
    Helper to derive the submission period from as_at_date and year

    Args:
        col: For large datasets, the UDF needs to be passed a column
        as_at_date: The date for when the inclusion is to be run for.
        year: The year for this to be run for.

    Returns: Optional[int]

    """
    as_at_date = date_as_string(as_at_date, can_be_none=True, local_time=True)
    if as_at_date is None:
        as_at_date = ""
    if as_at_date not in [r.get("as_at_date")[: len(as_at_date)] for r in inclusion_calendar]:
        raise Exception(f"as_at_date must be an inclusion date: {as_at_date}")
    # try and match to a row in the inclusion data
    for inclusion_row in inclusion_calendar:
        if inclusion_row.get("as_at_date")[: len(as_at_date)] == as_at_date and inclusion_row.get("year") == year:
            return inclusion_row.get("period")
    return None


def derive_partyear(col: Column, as_at_date: DateValue, year: int) -> Optional[str]:
    """
    Helper to derive part year without mininal required arguments.
    See derive_part_year() for more argument options

    Args:
        col: For large datasets, the UDF needs to be passed a column
        as_at_date: The date for when the inclusion is to be run for.
        year: The year for this to be run

    Returns: Dict[str, Any]

    """
    as_at_date = date_as_string(as_at_date, can_be_none=True, local_time=True)
    if as_at_date is None:
        as_at_date = ""
    if as_at_date not in [r.get("as_at_date")[: len(as_at_date)] for r in inclusion_calendar]:
        raise Exception(f"as_at_date must be an inclusion date: {as_at_date}")
    # try and match to a row in the inclusion data
    for inclusion_row in inclusion_calendar:
        if inclusion_row.get("as_at_date")[: len(as_at_date)] == as_at_date and inclusion_row.get("year") == year:
            return inclusion_row.get("partyear")
    return None


def derive_period_start(col: Column, as_at_date: DateValue, year: int) -> Optional[str]:
    """
    Helper to derive the period start from as_at_date and year

    Args:
        col: For large datasets, the UDF needs to be passed a column
        as_at_date: The date for when the inclusion is to be run for.
        year: The year for this to be run for.

    Returns: Dict[str, Any]
    """
    as_at_date = date_as_string(as_at_date, can_be_none=True, local_time=True)
    if as_at_date is None:
        as_at_date = ""
    if as_at_date not in [r.get("as_at_date")[: len(as_at_date)] for r in inclusion_calendar]:
        raise Exception(f"as_at_date must be an inclusion date: {as_at_date}")
    # try and match to a row in the inclusion data
    for inclusion_row in inclusion_calendar:
        if inclusion_row.get("as_at_date")[: len(as_at_date)] == as_at_date and inclusion_row.get("year") == year:
            return inclusion_row.get("activity_from")
    return None


def derive_period_end(col: Column, as_at_date: DateValue, year: int) -> Optional[str]:
    """
    Helper to derive the period end from as_at_date and year

    Args:
        col: For large datasets, the UDF needs to be passed a column
        as_at_date: The date for when the inclusion is to be run for.
        year: The year for this to be run for.

    """
    as_at_date = date_as_string(as_at_date, can_be_none=True, local_time=True)
    if as_at_date is None:
        as_at_date = ""
    if as_at_date not in [r.get("as_at_date")[: len(as_at_date)] for r in inclusion_calendar]:
        raise Exception(f"as_at_date must be an inclusion date: {as_at_date}")
    # try and match to a row in the inclusion data
    for inclusion_row in inclusion_calendar:
        if inclusion_row.get("as_at_date")[: len(as_at_date)] == as_at_date and inclusion_row.get("year") == year:
            return inclusion_row.get("activity_to")
    return None


def derive_financial_end_year(as_at_date: str) -> str:
    input_dt = datetime.datetime.strptime(as_at_date, "%Y-%m-%d")
    date = datetime.datetime.strptime(f"{input_dt.year}-03-31", "%Y-%m-%d")
    if input_dt > date:
        date = datetime.datetime.strptime(f"{input_dt.year + 1}-03-31", "%Y-%m-%d")
    return date


def format_boolean_to_value(column: Union[bool, str], true_value: Any, false_value: Any) -> Optional[Any]:
    """
    Helper to format boolean values, returns the true_value if true and false_value if false.

    Args:
        column (bool, str): the value to convert
        true_value: The value to return if True
        false_value:  The value to return if False
    Returns:
        Any: The values passed in as args or None
    Raises:
        TypeError: When the column is ot of type bool or str
    """
    if isinstance(column, str):
        if column.lower() in ("true", "y"):
            return true_value
        elif column.lower() in ("false", "n"):
            return false_value
        else:
            return None

    elif isinstance(column, bool):
        if column:
            return true_value
        return false_value

    elif column is None:
        return None

    else:
        raise TypeError(f"Expected types bool or str, not {type(column)}")


def format_boolean_to_integer(column: bool) -> int:
    """
    Helper to format boolean values, i.e. True/False into Integer values 1/0

    Args:
        column (bool, str): the boolean value to convert

    Returns:
        Int: The formatted boolean
    """
    true_value, false_value = 1, 0
    return format_boolean_to_value(column, true_value, false_value)


def format_boolean_to_character(column: bool, true_value="Y", false_value="N") -> str:
    """
    Helper to format boolean values, i.e. True/False into Character values Y/N

    Args:
        column (bool): the boolean value to convert

    Returns:
        Str: The formatted boolean
    """

    return format_boolean_to_value(column, true_value, false_value)


def format_null_to_type(column, return_val, return_type: str = None) -> Any:
    """
    Helper to replace nulls with an empty value of a type for not null columns

    Args:
        column (Any): The value we want to convert
        return_val: The value to return if the column is null
        return_type: The value type to cast as if you wish to.

    Returns:
        Any: The converted null value
    """
    if column is None:
        return return_val
    else:
        if return_type:
            types = {"str": str, "int": int, "float": float}
            if return_type in types:

                return types[return_type](column)

        return column


def format_null_to_str(column, return_val) -> str:
    """
    Helper to replace nulls with an empty value of a type for not null columns

    Args:
        column (Any): The value we want to convert
        return_val: The value to return if the column is null

    Returns:
        Any: The converted null value
    """

    return format_null_to_type(column, return_val, "str")


def format_null_to_int(column, return_val) -> int:
    """
    Helper to replace nulls with an empty value of a type for not null columns

    Args:
        column (Any): The value we want to convert
        return_val: The value to return if the column is null

    Returns:
        Any: The converted null value
    """

    return format_null_to_type(column, return_val, "int")


def format_null_to_numeric(column, return_val) -> float:
    """
    Helper to replace nulls with an empty value of a type for not null columns

    Args:
        column (Any): The value we want to convert
        return_val: The value to return if the column is null

    Returns:
        Any: The converted null value
    """
    return format_null_to_type(column, return_val, "float")


def smart_cast_int(column: Any, null_value: Any = None) -> Optional[int]:
    """
    Helper to cast some strings as integer data types that may
    have issues converting because of a space in the string.

    Args:
        column (Any): The value we want to convert
        null_value (Any): The value o return if the column is not a digit.
    Returns:
        int, Any: The converted string to an integer type or the null_value
    """
    column = str(column).strip()
    if column.isdigit():
        return int(column)

    else:
        return null_value


@udf
def generate_pseudo_value(length: int = 16) -> str:
    """
    UDF to generate a random alphanumeric pseudo value

    Args:
        length: The length of the pseudo value we want

    Return:
        str: The random pseudo value of length {length}
    """

    def int_to_string(number, alphabet, padding=None):
        """
        Convert a number to a string, using the given alphabet.
        The output has the most significant digit first.
        See:  https://github.com/skorokithakis/shortuuid/blob/master/shortuuid/main.py
        """
        output = ""
        alpha_len = len(alphabet)
        while number:
            number, digit = divmod(number, alpha_len)
            output += alphabet[digit]
        if padding:
            remainder = max(padding - len(output), 0)
            output = output + alphabet[0] * remainder
        return output[::-1]

    random_num = int(binascii.b2a_hex(os.urandom(length)), length)
    return int_to_string(random_num, "23456789ABCDEFGHJKLMNPQRSTUVWXYZ", padding=length)[:length]
