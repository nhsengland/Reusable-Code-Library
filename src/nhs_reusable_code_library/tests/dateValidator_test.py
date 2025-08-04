from nhs_reusable_code_library.standard_data_validations import dateValidator
import pytest


@pytest.mark.parametrize(
    "date_string, expected",
    [
        ("01/01/1900", True),  # Edge date
        ("29/02/1992", True),  # Leap Year
        ("31/05/2020", True),  # 31-day month
        ("29/02/2040", True),  # Leap Year
        ("31/12/2049", True),  # Edge date
    ],
)
def test_validDate_positives(date_string, expected):
    assert expected == dateValidator.validDate(date_string)


@pytest.mark.parametrize(
    "date_string, expected",
    [
        ("31/12/1899", False),  # Edge date
        ("29/02/1990", False),  # Leap Year
        ("31/04/2020", False),  # 31-day month
        ("29/02/2041", False),  # Leap Year
        ("01/01/2050", False),  # Edge date
    ],
)
def test_validDate_negatives(date_string, expected):
    assert expected == dateValidator.validDate(date_string)
