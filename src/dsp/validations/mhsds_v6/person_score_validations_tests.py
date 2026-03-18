from typing import Optional, Dict, Mapping

import pytest

from dsp.datasets.validations.common import is_valid_score
from dsp.datasets.validations.common import PersonScoreInSetValidator, PersonScoreWithinRangeValidator

_ASSESSMENT_SCALES_RANGE = {'123': {
    'value_validator': PersonScoreWithinRangeValidator(lower_bound=0.0, upper_bound=1.0, decimal_digits_number=1)}}

_ASSESSMENT_SCALES_SET = {'123': {'value_validator': PersonScoreInSetValidator(acceptable_values=('A', 'C'))}}

_ASSESSMENT_SCALES_HIGH_RANGE = {'123': {
    'value_validator': PersonScoreWithinRangeValidator(lower_bound=0.0, upper_bound=2.0, decimal_digits_number=2)}}

@pytest.mark.parametrize(['snomed_code', 'score', 'validators_by_code', 'expected'], [
    ('123', None, _ASSESSMENT_SCALES_SET, True),
    (None, 'A', _ASSESSMENT_SCALES_SET, True),
    ('456', 'A', _ASSESSMENT_SCALES_SET, True),
    ('123', 'A', _ASSESSMENT_SCALES_SET, True),
    ('123', 'B', _ASSESSMENT_SCALES_SET, False),
    ('123', 'A', _ASSESSMENT_SCALES_RANGE, False),
    ('123', '0.5', _ASSESSMENT_SCALES_RANGE, True),
    ('123', '1.5', _ASSESSMENT_SCALES_RANGE, False),
    ('123', '-1', _ASSESSMENT_SCALES_RANGE, False),
    ('123', '1.50', _ASSESSMENT_SCALES_HIGH_RANGE, True),
    ('123', '1.51', _ASSESSMENT_SCALES_HIGH_RANGE, True),
])
def test_is_valid_score(snomed_code: Optional[str], score: Optional[str],
                        validators_by_code: Mapping[str, Dict], expected: Optional[bool]):
    assert is_valid_score(snomed_code, score, validators_by_code) == expected

