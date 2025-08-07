from nhs_reusable_code_library.standard_data_validations import nhsNumberValidation
import pytest


@pytest.mark.parametrize(
    "nhsNumber, expected",
    [
        ("8429141456", True),
    ],
)
def test_mod11_check(nhsNumber, expected):
    assert expected == nhsNumberValidation.mod11_check(nhsNumber)


@pytest.mark.parametrize(
    "decision_to_admit,nhs_number_status_indicator, witheld_identity_reason, expected",
    [
        (True, True, True, True),
    ],
)
def test_sensitive_legally_restricted(decision_to_admit,nhs_number_status_indicator, witheld_identity_reason,  expected):
    assert expected == nhsNumberValidation.sensitive_legally_restricted(decision_to_admit,nhs_number_status_indicator, witheld_identity_reason)
