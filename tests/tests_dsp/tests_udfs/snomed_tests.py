import pytest

from dsp.udfs import snomed


@pytest.mark.parametrize("snomedct_id,lookup_date,expected", [
    ('Invalid', '20140101', None),
    ('10001005', '2002-01-30', None),
    ('10001005', '2002-01-31', 'Bacterial septicemia (disorder)'),
    ('10001005', '2022-01-31', 'Bacterial sepsis (disorder)')
])
def test_snomedct_description_from_snomedct_id(snomedct_id, lookup_date, expected):
    assert snomed.snomedct_description_from_snomedct_id(snomedct_id, lookup_date) == expected
