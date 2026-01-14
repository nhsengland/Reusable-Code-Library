"""Function implementations to be registered as UDFs."""

# pylint: disable=invalid-name,missing-function-docstring
import datetime
from decimal import Decimal
from typing import Optional, Union

from src.dve.metadata_parser.domain_types import NHSNumber


def over_10k(x: float) -> bool:  # noqa: D103
    return x > 10000


def over_1k(x: float) -> bool:  # noqa: D103
    return x > 1000


def under_10k(x: float) -> bool:  # noqa: D103
    return x < 10000


def under_5k(x: float) -> bool:  # noqa: D103
    return x < 5000


def over_5(x: float) -> bool:  # noqa: D103
    return x > 5


def over_10(x: float) -> bool:  # noqa: D103
    return x > 10


def x_not_greater_than_y(x: float, y: float) -> bool:  # noqa: D103
    return x <= y


def date_in_current_financial_year(test_date: datetime.date) -> bool:
    """Checks the date is in the current financial year"""
    current_fiscal_year_start: datetime.date = datetime.date(datetime.date.today().year, 4, 1)
    return test_date >= current_fiscal_year_start


def is_valid_ods_code(check_ods_code: str) -> bool:
    """A test ODS code lookup, contains some actual valid codes"""
    if (not check_ods_code) or (not isinstance(check_ods_code, str)):
        return False
    valid_ods_codes = [
        "EE142976",
        "EE144430",
        "EE143473",
        "EE148112",
        "EE142863",
        "EE147862",
        "EE142472",
        "EE141208",
        "EE143149",
        "EE140862",
        "EE140319",
        "EE144899",
        "EE144475",
        "EE141850",
        "EE147934",
        "EE141068",
        "EE143825",
        "EE147805",
        "EE143489",
        "EE146813",
        "EE145703",
        "EE148295",
        "EE140156",
        "EE145502",
        "EE148396",
        "EE144126",
        "EE145590",
        "EE141566",
        "EE142081",
        "EE143640",
        "EE144911",
        "EE145935",
        "EE145279",
        "EE143156",
        "EE146556",
        "EE140781",
        "EE144734",
        "EE144841",
        "EE140419",
        "EE140040",
        "EE147342",
        "EE143330",
        "EE140926",
        "EE140926",
        "EE146438",
        "EE142137",
        "EE143856",
        "EE141067",
        "EE148534",
        "EE141310",
        "EE146899",
        "EE146996",
        "EE147487",
        "EE148447",
        "EE144311",
        "EE142147",
        "EE147605",
        "EE142117",
        "EE144087",
        "EE147326",
        "EE147614",
        "EE143703",
        "EE146135",
        "EE140782",
        "EE143603",
        "EE143554",
        "EE146659",
        "EE140321",
        "EE141185",
        "EE147648",
        "EE144527",
        "EE142680",
        "EE141620",
        "EE145274",
        "EE146251",
        "EE148209",
        "EE142574",
        "EE148162",
        "EE143118",
        "EE142977",
        "EE147798",
        "EE147902",
        "EE145780",
        "EE146992",
        "EE142916",
        "EE144777",
        "EE146935",
        "EE145586",
        "EE144570",
        "EE147122",
        "EE140874",
        "EE141338",
        "EE143244",
    ]
    return check_ods_code in valid_ods_codes


def is_valid_national_org(check_org_code: str) -> bool:
    """Simple test org code lookup"""
    if (not check_org_code) or (not isinstance(check_org_code, str)):
        return False
    valid_org_codes = ["ORG01", "ORG02"]
    return check_org_code in valid_org_codes


def check_correct_numeric_signage(
    val: Union[float, int, Decimal], expected_sign: str = "+/-"
) -> Optional[bool]:
    """Check the expected sign of a numeric type."""
    if val is None:
        return None
    if expected_sign not in ("+/-", "+", "-"):
        return None

    if expected_sign == "+/-":
        return True
    if expected_sign == "+":
        return val >= 0
    if expected_sign == "-":
        return val <= 0
    return None


def number_matches_within_tolerance(
    comparator: Decimal, number: Decimal, tolerance: Decimal
) -> Optional[bool]:
    """Check that `number` matches `comparator` to within a fixed tolerance."""
    if comparator is None or number is None or tolerance is None:
        return None

    return abs(number - comparator) <= abs(tolerance)


def number_matches_within_percentage(
    comparator: Decimal, number: Decimal, percentage: Decimal
) -> Optional[bool]:
    """Check that `number` matches `comparator` to within a percentage of the value of `comparator`.

    `tolerance` should be a decimal representaion of the percentage (e.g. 0.01 for 1%).

    """
    if percentage is None or comparator is None:
        return None

    tolerance = comparator * percentage
    return number_matches_within_tolerance(comparator, number, tolerance)


def nhsno_mod11_check(nhs_no: str) -> bool:
    """Check that a given nhs number passes mod11 checks."""
    return NHSNumber.check_validates(nhs_no)
