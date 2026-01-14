"""Domain specific type definitions for use in validators."""

# pylint: disable=too-few-public-methods
import datetime as dt
import itertools
import re
import warnings
from collections.abc import Iterator, Sequence
from functools import lru_cache
from typing import ClassVar, Optional, TypeVar, Union

from pydantic import fields, types, validate_arguments
from typing_extensions import Literal

from dve.metadata_parser import exc

T = TypeVar("T")


NULL_POSTCODES = ["tba", "tbc", "na", "n/a", "no valid"]
# The regex contains the following groups to ensure a postcode fits into the
# available formats:
# \A                  :   Signifies the start of the string
# ([a-zA-Z]{1,2})     :   Matches a single alphabetic character once or twice,
#                         of either case
# (\d)                :   Matches a single digit
# ([a-zA-Z]?|\d?)     :   Optionally matches either a single alphabetic character
#                         or digit
# (\s\d[a-zA-Z]{2})   :   Matches a space, followed by a digit and then two
#                         alphabetic characters
# \Z                  :   Signifies the end of the string
POSTCODE_REGEX = re.compile(r"\A[a-zA-Z]{1,2}\d([a-zA-Z]?|\d?)\s\d[a-zA-Z]{2}\Z")


class _SimpleRegexValidator(types.ConstrainedStr):
    """A basic regex-validated type."""

    regex: re.Pattern
    """A regex pattern used to validate the string."""
    strip_whitespace: bool = True
    """Whether to strip the whitespace from the string."""


class NHSNumber(types.ConstrainedStr):
    """A constrained string which validates an NHS number.

    ### Validation criteria

    The following criteria are used for validation (after separators are removed).
    An NHS number:
     - Is 10 characters long
     - Is a number
     - Has the correct check digit.

    ### Check digit calculation

    The check digit (the last digit of the NHS number) should match the
    'checksum' calculated from the number. This checksum is calculated as follows:
     1. The first 9 digits of the number are multiplied by factors from 10 to 2
        (i.e. the first is multiplied by 10, the second is multiplied by 9, ...)
     2. The sum of the products from stage 1 is taken
     3. The checksum is calculated by applying the following logic to the mod-11
        (remainder after dividing by 11) of the sum from stage 2. This checksum
        must match the last digit of the NHS number.

            * If 0: the checksum is 0
            * If 1: the NHS number is invalid
            * If between 2 and 11: the checksum is 11 minus the mod-11

    These rules are taken from the data dictionary definition:
        https://datadictionary.nhs.uk/attributes/nhs_number.html

    ### Warning Emission

    Warnings will be emitted in the following circumstances:

     - The NHS number is a sentinel value, commonly used for a specific purpose.
     - The NHS number starts with '9'; this indicates a test number.
     - The NHS number is a palindrome; this indicates a test number.

    """

    SENTINEL_VALUES: ClassVar[dict[str, str]] = {
        "0000000000": "returned by MPS to indicate no match",
        "1111111111": "common example value given for patient-facing forms",
        "9999999999": "returned by MPS to indicate multiple matches",
        "0123456789": "common example value given for patient-facing forms",
    }
    """
    Sentinel NHS number values, which are normally used for specific purposes
    and annoyingly happen to pass checksum validation.

    """

    _FACTORS: ClassVar[tuple[int, ...]] = (10, 9, 8, 7, 6, 5, 4, 3, 2)
    """Weights for the NHS number digits in the checksum."""

    warn_on_test_numbers = True

    @classmethod
    def _warn_for_possible_invalid_number(cls, nhs_number: str, loc: str) -> None:
        """Emit warnings for possible invalid NHS numbers."""
        reason = None

        sentinel_reason = cls.SENTINEL_VALUES.get(nhs_number)
        if cls.warn_on_test_numbers and sentinel_reason:
            reason = sentinel_reason
        elif cls.warn_on_test_numbers and nhs_number.startswith("9"):
            reason = "NHS number starts with '9': this indicates a test number"
        elif cls.warn_on_test_numbers and nhs_number == nhs_number[::-1]:
            reason = "NHS number is a palindrome: this indicates a test number"

        if reason:
            warnings.warn(exc.LocWarning(f"NHS number possibly invalid ({reason})", loc))

    @staticmethod
    def ensure_format(nhs_number: Optional[str]) -> str:
        """Coerce an NHS number string to the correct format, raising an error if
        coersion fails.

        """
        if nhs_number is None:
            raise ValueError("NHS number not provided")

        nhs_number = str(nhs_number).replace(" ", "").replace("-", "")
        if len(nhs_number) != 10 or not nhs_number.isnumeric():
            raise ValueError("NHS number invalid (not a 10 digit number after separator removal)")
        return nhs_number

    @staticmethod
    def confirm_checksum_validates(nhs_number: str) -> bool:
        """Does the mod 11 check on the NHSnumber. This requires a 10 digit numeric string."""
        *digits, check_digit = iter(nhs_number)
        # weighted multiplication
        factors = (10, 9, 8, 7, 6, 5, 4, 3, 2)
        total = sum(int(digit) * factor for digit, factor in zip(digits, factors))
        remainder = total % 11
        check = 11 - (remainder or 11)
        return check == int(check_digit)

    @classmethod
    def check_validates(cls, value: Optional[str]) -> bool:
        """Check whether an NHS number is valid, returning `True` for valid numbers
        and `False` for invalid numbers.

        """
        try:
            nhs_number = cls.ensure_format(value)
            is_valid = cls.confirm_checksum_validates(nhs_number)
        except ValueError:
            return False
        return is_valid

    @classmethod
    def validate(cls, value: Optional[str], field: fields.ModelField) -> str:  # type: ignore
        """Validates the given postcode"""
        nhs_number = cls.ensure_format(value)

        if cls.confirm_checksum_validates(nhs_number):
            # TODO: Get a better way to get 'loc' here.
            cls._warn_for_possible_invalid_number(nhs_number, field.name)
            return nhs_number
        raise ValueError("NHS number invalid (incorrect check digit: cannot be a real NHS number)")


@lru_cache()
@validate_arguments
def permissive_nhs_number(warn_on_test_numbers: bool = False):
    """Defaults to not checking for test numbers"""
    dict_ = NHSNumber.__dict__.copy()
    dict_["warn_on_test_numbers"] = warn_on_test_numbers

    return type("NHSNumber", (NHSNumber, *NHSNumber.__bases__), dict_)


# TODO: Make the spacing configurable. Not all downstream consumers want a single space
class Postcode(types.ConstrainedStr):
    """Postcode constrained string"""

    regex: re.Pattern = POSTCODE_REGEX
    strip_whitespace = True

    @staticmethod
    def normalize(postcode: str) -> Optional[str]:
        """Strips internal and external spaces"""
        postcode = postcode.replace(" ", "")
        if not postcode or postcode.lower() in NULL_POSTCODES:
            return None
        postcode = postcode.replace(" ", "")
        return " ".join((postcode[0:-3], postcode[-3:])).upper()

    @classmethod
    def validate(cls, value: str) -> Optional[str]:  # type: ignore
        """Validates the given postcode"""
        stripped = cls.normalize(value)
        if not stripped:
            return None

        if not cls.regex.match(stripped):
            raise ValueError("Invalid Postcode submitted")

        return stripped


class OrgID(_SimpleRegexValidator):
    """A validator for OrgID. This does not validate that the OrgID is
    for a 'real' organisation.

    That should be done using a database reference table.

    """

    regex = re.compile(r"^[A-Z0-9]{3,5}$")
    strip_whitespace = False

    @classmethod
    def validate(cls, value: str) -> str:
        """Validates the given OrgID"""
        if not value:
            raise ValueError("org_id not provided")
        return super().validate(value)


class ConFormattedDate(dt.date):
    """A date, provided as a date or a string in a specific format."""

    DATE_FORMAT: ClassVar[Optional[str]] = None
    """The specific format of the date as a Python 'strptime' string."""
    ge: ClassVar[Optional[dt.date]] = None
    """The earliest date allowed."""
    le: ClassVar[Optional[dt.date]] = None
    """The latest date allowed."""
    gt: ClassVar[Optional[dt.date]] = None
    """The earliest date allowed."""
    lt: ClassVar[Optional[dt.date]] = None
    """The latest date allowed."""

    @classmethod
    def validate(cls, value: Optional[Union[dt.date, str]]) -> Optional[dt.date]:
        """Validate a passed datetime or string."""
        if value is None:
            return value

        if isinstance(value, dt.date):
            date = value
        elif cls.DATE_FORMAT is not None:
            try:
                date = dt.datetime.strptime(value, cls.DATE_FORMAT).date()
            except ValueError as err:
                raise ValueError(
                    f"Unable to parse provided datetime in format {cls.DATE_FORMAT}"
                ) from err  # pylint: disable=line-too-long
        else:
            raise ValueError("No date format provided")

        return date

    @classmethod
    def validate_range(cls, value) -> Optional[dt.date]:
        """Validates that the date falls within any constraints provided"""
        if cls.ge is not None and value < cls.ge:
            raise ValueError(f"Date must be greater than or equal to {cls.ge}")

        if cls.le is not None and value > cls.le:
            raise ValueError(f"Date must be less than or equal to {cls.le}")

        if cls.gt is not None and value <= cls.gt:
            raise ValueError(f"Date must be greater than {cls.gt}")

        if cls.lt is not None and value >= cls.lt:
            raise ValueError(f"Date must be less than {cls.lt}")

        return value

    @classmethod
    def __get_validators__(cls) -> Iterator[classmethod]:
        """Gets all validators"""
        yield cls.validate  # type: ignore
        yield cls.validate_range  # type: ignore


@lru_cache()
@validate_arguments
def conformatteddate(
    date_format: Optional[str] = None,
    ge: Optional[dt.date] = None,  # pylint: disable=invalid-name
    le: Optional[dt.date] = None,  # pylint: disable=invalid-name
    gt: Optional[dt.date] = None,  # pylint: disable=invalid-name
    lt: Optional[dt.date] = None,  # pylint: disable=invalid-name
) -> type[ConFormattedDate]:
    """Return a formatted date class with a set date format
    and timezone treatment.

    """
    if date_format is None:
        return ConFormattedDate

    dict_ = ConFormattedDate.__dict__.copy()
    dict_["DATE_FORMAT"] = date_format
    dict_["ge"] = ge
    dict_["le"] = le
    dict_["gt"] = gt
    dict_["lt"] = lt

    return type("FormattedDatetime", (ConFormattedDate, *ConFormattedDate.__bases__), dict_)


class FormattedDatetime(dt.datetime):
    """A datetime, provided as a datetime or a string in a specific format."""

    DATE_FORMAT: ClassVar[Optional[str]] = None
    """The specific format of the datetime as a Python 'strptime' string."""
    TIMEZONE_TREATMENT: ClassVar[Literal["forbid", "permit", "require"]] = "permit"
    """How to treat the presence of timezone-related information."""
    DEFAULT_PATTERNS: Sequence[str] = list(
        map(
            "".join,
            itertools.product(
                ("%Y-%m-%d", "%Y%m%d"),
                ("T", " ", ""),
                ("%H:%M:%S", "%H%M%S"),
                ("", ".%f"),
                ("%z", ""),
            ),
        )
    )
    """A sequence of datetime format patterns to try if `DATE_FORMAT` is unset."""

    @staticmethod
    def reformat_nhs_string_format(string: str) -> str:
        """Reformat the NHS's preferred string format to something sensible."""
        string = string.replace("T", "")
        return "".join(
            (
                "-".join((string[:4], string[4:6], string[6:8])),
                "T",
                ":".join((string[8:10], string[10:12], string[12:14])),
                "+",
                string[14:16],
                ":00",
            )
        )

    @classmethod
    def parse_datetime(cls, string: str) -> dt.datetime:
        """Attempt to parse a datetime using various formats in sequence."""
        string = string.strip()
        if string.endswith("Z"):  # Convert 'zulu' time to UTC.
            string = string[:-1] + "+00:00"

        if re.match(r"^([0-9]{16}|([0-9]{8}T[0-9]{8}))$", string):
            string = cls.reformat_nhs_string_format(string)

        for pattern in cls.DEFAULT_PATTERNS:
            try:
                datetime = dt.datetime.strptime(string, pattern)
            except ValueError:
                continue

            return datetime  # pragma: no cover
        raise ValueError("Unable to parse provided datetime")

    @classmethod
    def validate(cls, value: Optional[Union[dt.datetime, str]]) -> Optional[dt.datetime]:
        """Validate a passed datetime or string."""
        if value is None:
            return value

        if isinstance(value, dt.datetime):
            datetime = value
        elif cls.DATE_FORMAT is not None:
            try:
                datetime = dt.datetime.strptime(value, cls.DATE_FORMAT)
            except ValueError as err:
                raise ValueError(
                    f"Unable to parse provided datetime in format {cls.DATE_FORMAT}"
                ) from err  # pylint: disable=line-too-long
        else:
            datetime = cls.parse_datetime(value)

        if cls.TIMEZONE_TREATMENT == "forbid" and datetime.tzinfo:
            raise ValueError("Provided datetime has timezone, but this is forbidden for this field")
        if cls.TIMEZONE_TREATMENT == "require" and not datetime.tzinfo:
            raise ValueError(
                "Provided datetime missing timezone, but this is required for this field"
            )  # pylint: disable=line-too-long
        return datetime

    @classmethod
    def __get_validators__(cls) -> Iterator[classmethod]:
        """Gets all validators"""
        yield cls.validate  # type: ignore


class FormattedTime(dt.time):
    """A time, provided as a datetime or a string in a specific format."""

    TIME_FORMAT: ClassVar[Optional[str]] = None
    """The specific format of the time."""
    TIMEZONE_TREATMENT: ClassVar[Literal["forbid", "permit", "require"]] = "permit"
    """How to treat the presence of timezone-related information."""
    DEFAULT_PATTERNS: Sequence[str] = list(
        # 24 hour time pattern combinations
        map(
            "".join,
            itertools.product(
                ("%H:%M:%S", "%H%M%S"),
                ("", ".%f"),
                ("%p", "%P", ""),
                ("%z", ""),
            ),
        )
    ) + list(
        # 12 hour time pattern combinations
        map(
            "".join,
            itertools.product(
                ("%I:%M:%S", "%I%M%S"),
                ("", ".%f"),
                ("%z", ""),
                (" %p", "%p", "%P", " %P", ""),
            ),
        )
    )
    """A sequence of time format patterns to try if `TIME_FORMAT` is unset."""

    @classmethod
    def convert_to_time(cls, value: dt.datetime) -> dt.time:
        """
        Convert `datetime.datetime` to `datetime.time`. If datetime contains timezone info, that
        will be retained.
        """
        if value.tzinfo:
            return value.timetz()

        return value.time()

    @classmethod
    def parse_time(cls, string: str) -> dt.time:
        """Attempt to parse a datetime using various formats in sequence."""
        string = string.strip()
        if string.endswith("Z"):  # Convert 'zulu' time to UTC.
            string = string[:-1] + "+00:00"

        for pattern in cls.DEFAULT_PATTERNS:
            try:
                datetime = dt.datetime.strptime(string, pattern)
            except ValueError:
                continue

            time = cls.convert_to_time(datetime)

            return time  # pragma: no cover
        raise ValueError("Unable to parse provided time")

    @classmethod
    def validate(cls, value: Union[dt.time, dt.datetime, str]) -> dt.time | None:
        """Validate a passed time, datetime or string."""
        if value is None:
            return value

        if isinstance(value, dt.time):
            new_time = value
        elif isinstance(value, dt.datetime):
            new_time = cls.convert_to_time(value)
        else:
            if cls.TIME_FORMAT is not None:
                try:
                    new_time = dt.datetime.strptime(value, cls.TIME_FORMAT)  # type: ignore
                    new_time = cls.convert_to_time(new_time)  # type: ignore
                except ValueError as err:
                    raise ValueError(
                        f"Unable to parse provided time in format {cls.TIME_FORMAT}"
                    ) from err
            else:
                new_time = cls.parse_time(value)

        if cls.TIMEZONE_TREATMENT == "forbid" and new_time.tzinfo:
            raise ValueError("Provided time has timezone, but this is forbidden for this field")
        if cls.TIMEZONE_TREATMENT == "require" and not new_time.tzinfo:
            raise ValueError("Provided time missing timezone, but this is required for this field")

        return new_time


@lru_cache()
@validate_arguments
def formatteddatetime(
    date_format: Optional[str] = None,
    timezone_treatment: Literal["forbid", "permit", "require"] = "permit",
) -> type[FormattedDatetime]:
    """Return a formatted datetime class with a set date format
    and timezone treatment.

    """
    if date_format is None and timezone_treatment == "permit":
        return FormattedDatetime

    dict_ = FormattedDatetime.__dict__.copy()
    dict_["DATE_FORMAT"] = date_format
    dict_["TIMEZONE_TREATMENT"] = timezone_treatment

    return type("FormattedDatetime", (FormattedDatetime, *FormattedDatetime.__bases__), dict_)


@lru_cache()
@validate_arguments
def formattedtime(
    time_format: Optional[str] = None,
    timezone_treatment: Literal["forbid", "permit", "require"] = "permit",
) -> type[FormattedTime]:
    """Return a formatted time class with a set time format and timezone treatment."""
    if time_format is None and timezone_treatment == "permit":
        return FormattedTime

    dict_ = FormattedTime.__dict__.copy()
    dict_["TIME_FORMAT"] = time_format
    dict_["TIMEZONE_TREATMENT"] = timezone_treatment

    return type("FormattedTime", (FormattedTime, *FormattedTime.__bases__), dict_)


class ReportingPeriod(dt.date):
    """A reporting period field, with the type of reporting period supplied"""

    REPORTING_PERIOD_TYPE: ClassVar[Literal["start", "end"]]
    DATE_FORMAT: ClassVar[str] = "%Y-%m-%d"

    @classmethod
    def parse_datetime(cls, value: str) -> dt.date:
        """Attempt to parse string to date"""
        try:
            date = dt.datetime.strptime(value, cls.DATE_FORMAT).date()
            return date
        except ValueError as err:
            raise ValueError(f"Unable to parse provided date in format {cls.DATE_FORMAT}") from err

    @staticmethod
    def start_of_month(value: dt.date) -> bool:
        """Check if the date supplied is the start of the month"""
        return value.day == 1

    @staticmethod
    def end_of_month(value: dt.date) -> bool:
        """Check if the date supplied is the end of the month"""

        def last_day_of_month(value: dt.date):
            """Calculate the last day in the month of the supplied date"""
            if value.month == 12:
                return value.replace(day=31)
            return value.replace(month=value.month + 1, day=1) - dt.timedelta(days=1)

        return value == last_day_of_month(value)

    @classmethod
    def validate(cls, value: Union[dt.date, str]) -> Optional[dt.date]:
        """Validate if the value is a valid reporting period"""
        if isinstance(value, str):
            value = cls.parse_datetime(value)
        if cls.REPORTING_PERIOD_TYPE == "start":
            if not cls.start_of_month(value):
                raise ValueError("Reporting date supplied is not at the start of the month")
        else:
            if not cls.end_of_month(value):
                raise ValueError("Reporting date supplied is not at end of the month")
        return value

    @classmethod
    def __get_validators__(cls) -> Iterator[classmethod]:
        """Gets all validators"""
        yield cls.validate  # type: ignore


# @lru_cache()
@validate_arguments
def reportingperiod(
    reporting_period_type: Literal["start", "end"], date_format: Optional[str] = "%Y-%m-%d"
) -> type[ReportingPeriod]:
    """Return a check on whether a reporting period date is a valid date,
    and is the start/ end of the month supplied depending on reporting period type
    """
    dict_ = ReportingPeriod.__dict__.copy()
    dict_["REPORTING_PERIOD_TYPE"] = reporting_period_type
    dict_["DATE_FORMAT"] = date_format

    return type("ReportingPeriod", (ReportingPeriod, *ReportingPeriod.__bases__), dict_)


@lru_cache()
@validate_arguments
def alphanumeric(
    min_digits: types.NonNegativeInt = 1,
    max_digits: types.PositiveInt = 1,
) -> type[_SimpleRegexValidator]:
    """Return a regex-validated class which will ensure that
    passed numbers are alphanumeric.

    """
    an_group_str = r"[A-Za-z0-9]"
    if max_digits == min_digits:
        type_name = f"AN{max_digits}"
        pattern_str = f"{an_group_str}{{{max_digits}}}"
    else:
        type_name = f"AN{min_digits}_{max_digits}"
        pattern_str = f"{an_group_str}{{{min_digits},{max_digits}}}"

    dict_ = _SimpleRegexValidator.__dict__.copy()
    dict_["regex"] = re.compile(f"^{pattern_str}$")

    return type(
        type_name,
        (_SimpleRegexValidator, *_SimpleRegexValidator.__bases__),
        dict_,
    )


@lru_cache()
@validate_arguments
def identifier(
    min_digits: types.NonNegativeInt = 1,
    max_digits: types.PositiveInt = 1,
) -> type[_SimpleRegexValidator]:
    """
    Return a regex-validated class which will ensure that
    passed strings are alphanumeric or in a fixed set of
    special characters for identifiers.
    """
    id_group_str = r"[A-Za-z0-9_\-=\/\\#:; ().`*!,|+'\^\[\]]"
    if max_digits == min_digits:
        type_name = f"AN{max_digits}"
        pattern_str = rf"{id_group_str}{{{max_digits}}}"
    else:
        type_name = f"AN{min_digits}_{max_digits}"
        pattern_str = rf"{id_group_str}{{{min_digits},{max_digits}}}"

    dict_ = _SimpleRegexValidator.__dict__.copy()
    dict_["regex"] = re.compile(f"^{pattern_str}$")

    return type(
        type_name,
        (_SimpleRegexValidator, *_SimpleRegexValidator.__bases__),
        dict_,
    )
