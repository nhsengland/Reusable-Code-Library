import pytest

from dsp.datasets.models.msds_constants import UnitOfMeasure


class TestUnitOfMeasure(object):
    """ Unit tests for UnitOfMeasure """
    def setup(self):
        self.unit_of_measure = UnitOfMeasure('m', 'meters')

    def test_contains_is_case_insensitive(self):
        assert 'M' in self.unit_of_measure
        assert 'mEterS' in self.unit_of_measure

    def test_contains_ignores_leading_and_trailing_whitespace(self):
        assert ' m' in self.unit_of_measure
        assert 'mEterS ' in self.unit_of_measure
        assert ' M   \t' in self.unit_of_measure
        assert 'ME TERS' not in self.unit_of_measure

    def test_item_access_is_case_insensitive(self):
        assert self.unit_of_measure['M'] == 'm'
        assert self.unit_of_measure['mEterS  '] == 'meters'

    def test_item_access_ignores_leading_and_trailing_whitespace(self):
        assert self.unit_of_measure[' m '] == 'm'
        assert self.unit_of_measure['mEterS '] == 'meters'
        assert self.unit_of_measure[' M   \t'] == 'm'

    def test_unknown_item_access_raises_key_error(self):
        with pytest.raises(KeyError):
            _ = self.unit_of_measure['ME TERS']

    def test_attribute_access_is_case_insensitive(self):
        assert self.unit_of_measure.M == 'm'
        assert self.unit_of_measure.mEterS == 'meters'

    def test_unknown_attribute_access_raises_attribute_error(self):
        with pytest.raises(AttributeError):
            _ = self.unit_of_measure.NOT_AN_ATTRIBUTE

    def test_iterator_protocol(self):
        assert len(self.unit_of_measure) == 2
        assert list(self.unit_of_measure) == ['m', 'meters']

    def test_bool_protocol(self):
        assert bool(self.unit_of_measure) is True

        empty_unit_of_measure = UnitOfMeasure()
        assert bool(empty_unit_of_measure) is False
