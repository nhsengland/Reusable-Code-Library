import pytest

from dsp.udfs import ethnic_category_group, ethnic_category_description


def test_ethnic_category_group_none():
    assert None is ethnic_category_group(None)


def test_ethnic_category_group_white():
    for ethnic_code in ['A', 'B', 'C']:
        assert 'White' == ethnic_category_group(ethnic_code)


def test_ethnic_category_group_mixed():
    for ethnic_code in ['D', 'E', 'F', 'G']:
        assert 'Mixed' == ethnic_category_group(ethnic_code)


def test_ethnic_category_group_asian():
    for ethnic_code in ['H', 'J', 'K', 'L']:
        assert 'Asian or Asian British' == ethnic_category_group(ethnic_code)


def test_ethnic_category_group_black():
    for ethnic_code in ['M', 'N', 'P']:
        assert 'Black or Black British' == ethnic_category_group(ethnic_code)


def test_ethnic_category_group_other():
    for ethnic_code in ['R', 'S']:
        assert 'Other Ethnic Groups' == ethnic_category_group(ethnic_code)


def test_ethnic_category_group_99():
    assert ethnic_category_group('99') is None


def test_ethnic_category_group_not_stated():
    assert 'Not stated' == ethnic_category_group('Z')


def test_ethnic_category_desc_none():
    assert ethnic_category_description(None) is None


def test_ethinic_category_desc_99():
    assert 'Not Known' == ethnic_category_description('99')

