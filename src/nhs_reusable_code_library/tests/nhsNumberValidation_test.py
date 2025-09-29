from src.nhs_reusable_code_library.standard_data_validations.nhsNumberValidation.nhsNumberValidation import mod11_check, nhs_number_format_check, palindromic_nhs_number_check, sensitive_legally_restricted
import pytest


@pytest.mark.parametrize(
    "nhsNumber, expected",
    [
        ("8429141456", True),
    ],
)
def test_mod11_check(nhsNumber, expected):
    assert expected == mod11_check(nhsNumber)


@pytest.mark.parametrize(
    "decision_to_admit,nhs_number_status_indicator, withheld_identity_reason, expected",
    [
        ('360', '07', '01', True),
    ],
)
def test_sensitive_legally_restricted(decision_to_admit,nhs_number_status_indicator, withheld_identity_reason,  expected):
    assert expected == sensitive_legally_restricted(decision_to_admit,nhs_number_status_indicator, withheld_identity_reason)


@pytest.mark.parametrize(
    "nhs_number, expected",
    [
        ("8429141456", True),
        ("842914145", False),
        ("84291414567", False),
        ("84291414A6", False),
        (8429141456, False)
    ],
)
def test_palindromic_nhs_number_check(expected, nhs_number):
    assert expected == nhs_number_format_check(nhs_number)

@pytest.mark.parametrize(
    "nhs_number, expected",
    [
        ("2000000002", True),
        ("1234567890", False),
    ],
)
def test_palindromic_nhs_number_check(nhs_number, expected):
    assert expected == palindromic_nhs_number_check(nhs_number)