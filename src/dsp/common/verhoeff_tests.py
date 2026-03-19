import pytest

from dsp.common.verhoeff import checksum, verify


@pytest.mark.parametrize(['digits', 'expected'], [
    ("236", 3),
])
def test_checksum(digits: str, expected: int):
    assert checksum(digits) == expected


@pytest.mark.parametrize(['digits', 'expected'], [
    ("2363", True),
    ("979641000000103", True)
])
def test_verify(digits: str, expected: bool):
    assert verify(digits) == expected
