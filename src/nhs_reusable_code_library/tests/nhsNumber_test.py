from nhs_reusable_code_library.nhsNumber import nhsNumberGenerator, nhsNumberValidator
import pytest
import random


@pytest.mark.parametrize(
    "to_generate, random_state, expected",
    [(3, 42, [8429141456, 2625792787, 8235363119]), (2, 1, [9598980006, 6597925149])],
)
def test_nhsNumberGenerator_BAU(to_generate, random_state, expected):
    assert expected == nhsNumberGenerator(to_generate, random_state=random_state)


@pytest.mark.parametrize("to_generate", [4.2, 1000001, -1])
def test_nhsNumberGenerator_valueErrors(to_generate):
    with pytest.raises(ValueError):
        nhsNumberGenerator(to_generate)


@pytest.mark.parametrize(
    "to_validate, expected", [(9598980006, True), (9598980007, False)]
)
def test_nhsNumberValidator_BAU(to_validate, expected):
    assert expected == nhsNumberValidator(to_validate)


@pytest.mark.parametrize("to_validate", [4.2, 1000001, -1])
def test_nhsNumberValidator_valueErrors(to_validate):
    with pytest.raises(ValueError):
        nhsNumberValidator(to_validate)
