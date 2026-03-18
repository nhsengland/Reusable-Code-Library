from datetime import date, datetime

import pytest
from dsp.common.utils import get_chunk_dates
from dsp.udfs.ahas import (
    date_as_string,
    derive_month_period,
    derive_part_year,
    derive_partyear,
    derive_period_end,
    derive_period_start,
    derive_submission_period,
    derive_year_period,
    format_boolean_to_character,
    format_boolean_to_integer,
    format_boolean_to_value,
    format_null_to_int,
    format_null_to_numeric,
    format_null_to_str,
    get_inclusion_calendar_row,
    get_inclusion_row,
    inclusion_calendar,
    inclusion_date_str,
    smart_cast_int,
)


def test_get_inclusion_calendar_row():
    assert get_inclusion_calendar_row(2020, 2) == {
        "partyear": "202002",
        "year": 2020,
        "period": 2,
        "as_at_date": "2020-06-17 16:30:00",
        "activity_from": "2020-04-01",
        "activity_to": "2020-05-31",
        "activity_quarter_end": "2020-06-30",
        "fyear": "2021",
    }


strptime_string = "%Y-%m-%d"
date_as_string_tests = [
    ("2020-04-20", False, "2020-04-20"),
    (datetime.strptime("2020-04-20", strptime_string), False, "2020-04-20 00:00:00"),
    (datetime.strptime("2020-04-20", strptime_string).date(), False, "2020-04-20"),
    (None, True, None),
    (date(2020, 4, 20), False, "2020-04-20"),
    (datetime(2020, 4, 20, 12, 12, 12), False, "2020-04-20 12:12:12"),
    ("2020-04-20T12:01:04Z", False, "2020-04-20 12:01:04"),
    ("2020-04-20 12:01:04", False, "2020-04-20 12:01:04"),
    (inclusion_date_str(2020, 5, 20), False, "2020-05-20 16:30:00"),
]
date_ids = [str(x) for x, _, _ in date_as_string_tests]


@pytest.mark.parametrize(["date_value", "can_be_none", "expected"], date_as_string_tests, ids=date_ids)
def test_date_as_string(date_value, can_be_none, expected):
    assert date_as_string(date_value, can_be_none=can_be_none) == expected


date_as_string_raises_tests = [("not a date", ValueError), ("02-11-1990", ValueError), (1000000, TypeError)]


@pytest.mark.parametrize(["date_value", "exception"], date_as_string_raises_tests)
def test_date_as_string_raises(date_value, exception):
    with pytest.raises(exception):
        date_as_string(date_value)


inclusion_calendar_tests = [
    ("2020-04-20", "2020-04-20", None, "2020-05-20 12:31:12", 2020, inclusion_calendar[14]),
    ("2020-04-20", "2020-04-20", "2020-05-25", "2020-05-20", 2020, inclusion_calendar[14]),
    ("2020-04-20", "2020-04-20", None, "2020-06-17", 2020, inclusion_calendar[15]),
    ("2020-04-20", "2020-04-20", "2020-06-18", "2020-06-17", 2020, inclusion_calendar[15]),
    ("2020-05-31", "2020-04-20", "2020-06-18", "2020-06-17", 2020, inclusion_calendar[15]),
    # ("2019-05-24", "2019-05-25T15:26:10.999+0000", "2019-06-26T15:26:10.999+0000", "2019-06-19", 2019, inclusion_calendar[2]),
    # Add other types (datetimes and dates)
    (
        datetime.strptime("2020-05-20", strptime_string),
        datetime.strptime("2020-04-20", strptime_string),
        None,
        "2020-06-17",
        2020,
        inclusion_calendar[15],
    ),
    (
        datetime.strptime("2020-04-20", strptime_string).date(),
        datetime.strptime("2020-04-20", strptime_string).date(),
        None,
        "2020-05-20",
        2020,
        inclusion_calendar[14],
    ),
]

inclusion_calendar_raw_filter_tests = [
    #   Make sure the effective_to col in inclusion calendar is not being used
    ("2020-05-01", "2020-04-20", "2020-05-25", "2020-05-20", 2020, inclusion_calendar[14]),
    ("2020-06-01", "2020-04-20", "2020-06-18", "2020-06-17", 2020, inclusion_calendar[15]),
    ("2020-07-20", "2020-04-20", "2020-06-18", "2020-06-17", 2020, inclusion_calendar[15]),
]


@pytest.mark.parametrize(
    ["activity_date", "effective_from", "effective_to", "as_at_date", "year", "expected_row"], inclusion_calendar_tests
)
def test_get_inclusion_row(activity_date, effective_from, effective_to, as_at_date, year, expected_row):
    assert get_inclusion_row(activity_date, effective_from, effective_to, as_at_date, year) == expected_row, (
        f"Failed with activity_date: {activity_date}, effective_from: {effective_from}, "
        f"effective_to: {effective_to}, as_at_date: {as_at_date}"
    )


@pytest.mark.parametrize(
    ["activity_date", "effective_from", "effective_to", "as_at_date", "year", "expected_row"],
    inclusion_calendar_raw_filter_tests,
)
def test_get_inclusion_row_with_raw_filter(activity_date, effective_from, effective_to, as_at_date, year, expected_row):
    assert get_inclusion_row(activity_date, effective_from, effective_to, as_at_date, year, True) == expected_row, (
        f"Failed with activity_date: {activity_date}, effective_from: {effective_from}, "
        f"effective_to: {effective_to}, as_at_date: {as_at_date}"
    )


@pytest.mark.parametrize(
    ["activity_date", "effective_from", "effective_to", "as_at_date", "year", "expected_row"], inclusion_calendar_tests
)
def test_derive_month_period(activity_date, effective_from, effective_to, as_at_date, year, expected_row):
    assert expected_row.get("period") == derive_month_period(
        activity_date, effective_from, effective_to, as_at_date, year
    )


@pytest.mark.parametrize(
    ["activity_date", "effective_from", "effective_to", "as_at_date", "year", "expected_row"],
    inclusion_calendar_raw_filter_tests,
)
def test_derive_month_period_with_raw_filter(
    activity_date, effective_from, effective_to, as_at_date, year, expected_row
):
    assert expected_row.get("period") == derive_month_period(
        activity_date, effective_from, effective_to, as_at_date, year, True
    )


@pytest.mark.parametrize(
    ["activity_date", "effective_from", "effective_to", "as_at_date", "year", "expected_row"], inclusion_calendar_tests
)
def test_derive_part_year(activity_date, effective_from, effective_to, as_at_date, year, expected_row):
    assert expected_row.get("partyear") == derive_part_year(
        activity_date, effective_from, effective_to, as_at_date, year
    )


@pytest.mark.parametrize(
    ["activity_date", "effective_from", "effective_to", "as_at_date", "year", "expected_row"],
    inclusion_calendar_raw_filter_tests,
)
def test_derive_part_year_with_raw_filter(activity_date, effective_from, effective_to, as_at_date, year, expected_row):
    assert expected_row.get("partyear") == derive_part_year(
        activity_date, effective_from, effective_to, as_at_date, year, True
    )


@pytest.mark.parametrize(
    ["activity_date", "effective_from", "effective_to", "as_at_date", "year", "expected_row"], inclusion_calendar_tests
)
def test_derive_year_period(activity_date, effective_from, effective_to, as_at_date, year, expected_row):
    assert expected_row.get("year") == derive_year_period(activity_date, effective_from, effective_to, as_at_date, year)


@pytest.mark.parametrize(
    ["activity_date", "effective_from", "effective_to", "as_at_date", "year", "expected_row"],
    inclusion_calendar_raw_filter_tests,
)
def test_derive_year_period_with_raw_filter(
    activity_date, effective_from, effective_to, as_at_date, year, expected_row
):
    assert expected_row.get("year") == derive_year_period(
        activity_date, effective_from, effective_to, as_at_date, year, True
    )


@pytest.mark.parametrize(
    ["activity_date", "effective_from", "effective_to", "as_at_date", "year", "expected_row"], inclusion_calendar_tests
)
def test_derive_submission_period(activity_date, effective_from, effective_to, as_at_date, year, expected_row):
    assert expected_row.get("period") == derive_submission_period("placeholder", as_at_date, year)


@pytest.mark.parametrize(
    ["activity_date", "effective_from", "effective_to", "as_at_date", "year", "expected_row"], inclusion_calendar_tests
)
def test_derive_period_start(activity_date, effective_from, effective_to, as_at_date, year, expected_row):
    assert expected_row.get("activity_from") == derive_period_start("placeholder", as_at_date, year)


@pytest.mark.parametrize(
    ["activity_date", "effective_from", "effective_to", "as_at_date", "year", "expected_row"], inclusion_calendar_tests
)
def test_derive_period_end(activity_date, effective_from, effective_to, as_at_date, year, expected_row):
    assert expected_row.get("activity_to") == derive_period_end("placeholder", as_at_date, year)


inclusion_calendar_type_error_tests = [
    (100, "2020-04-20", "2020-05-25"),
    ("2020-04-20", 100, "2020-05-25"),
    ("2020-04-20", "2020-04-20", 100),
]


@pytest.mark.parametrize(["activity_date", "effective_from", "effective_to"], inclusion_calendar_type_error_tests)
def test_get_inclusion_row_raises_type_error(activity_date, effective_from, effective_to):
    with pytest.raises(TypeError):
        get_inclusion_row(activity_date, effective_from, effective_to)


def test_format_boolean_to_value_raises_type_error():
    with pytest.raises(TypeError):
        format_boolean_to_value(1, True, False)


format_boolean_to_integer_tests = [
    (False, 0),
    (True, 1),
    ("false", 0),
    ("true", 1),
    ("FALSE", 0),
    ("TRUE", 1),
    ("foo", None),
    ("Y", 1),
    ("N", 0),
    ("y", 1),
    ("n", 0),
]


@pytest.mark.parametrize(["column", "expected"], format_boolean_to_integer_tests)
def test_format_boolean_to_integer(column, expected):
    formatted = format_boolean_to_integer(column)
    assert formatted == expected


format_boolean_to_character_tests = [
    (False, "N"),
    (True, "Y"),
    ("false", "N"),
    ("true", "Y"),
    ("FALSE", "N"),
    ("TRUE", "Y"),
    ("foo", None),
]


@pytest.mark.parametrize(["column", "expected"], format_boolean_to_character_tests)
def test_format_boolean_to_character(column, expected):
    formatted = format_boolean_to_character(column)
    assert formatted == expected


smart_cast_tests = [
    ("2019", None, 2019),
    ("2019", 0, 2019),
    ("skdjf", 2, 2),
    (2019, 1, 2019),
    ("155638763600099636445", 0, 155638763600099636445),
]


@pytest.mark.parametrize(["column", "null_value", "expected"], smart_cast_tests)
def test_smart_cast_int(column: str, null_value: int, expected: int):
    assert expected == smart_cast_int(column, null_value)


format_null_to_numeric_tests = [(None, 2019, float(2019)), (None, 0, float(0)), (2019.5, None, 2019.5)]


@pytest.mark.parametrize(["column", "return_val", "expected"], format_null_to_numeric_tests)
def test_format_null_to_numeric(column: str, return_val: int, expected: int):
    assert expected == format_null_to_numeric(column, return_val)


format_null_to_int_tests = [(None, 1, 1), (2019, 0, 2019), (2020, 2, 2020), (2019, 1, 2019)]


@pytest.mark.parametrize(["column", "return_val", "expected"], format_null_to_int_tests)
def test_format_null_to_int(column: str, return_val: int, expected: int):
    assert expected == format_null_to_int(column, return_val)


format_null_to_str_tests = [(None, "", ""), ("Whatever", "0", "Whatever"), (None, "2", "2")]


@pytest.mark.parametrize(["column", "return_val", "expected"], format_null_to_str_tests)
def test_format_null_to_str(column: str, return_val: int, expected: int):
    assert expected == format_null_to_str(column, return_val)


@pytest.mark.parametrize(
    ["activity_date", "effective_from", "effective_to", "as_at_date", "year", "expected_row"], inclusion_calendar_tests
)
def test_derive_partyear(activity_date, effective_from, effective_to, as_at_date, year, expected_row):
    assert expected_row.get("partyear") == derive_partyear("", as_at_date, year)


@pytest.mark.parametrize(["year", "month", "chunks", "expected"], [(2020, 1, 5, "2020-05-01"), (2020, 13, 10, "2021-04-01"), (2020, 12, 9, "2021-04-01")])
def test_get_chunk_dates(year, month, chunks, expected):
    inc_row = get_inclusion_calendar_row(year, month)
    chunks = get_chunk_dates(chunks, inc_row)
    assert chunks[-1].to_date == expected
