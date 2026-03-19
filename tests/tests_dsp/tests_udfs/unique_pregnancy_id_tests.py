import pytest

from dsp.udfs.unique_pregnancy_id import get_graph, get_distinct_upid_groups, get_first_item_from_distinct_upid_group


@pytest.mark.parametrize("groups, expected", [
    ([['a', 'b'], ['b', 'c'], ['d', 'e', 'f']], {
            'a': {'a', 'b'},
            'b': {'a', 'b', 'c'},
            'c': {'b', 'c'},
            'd': {'d', 'e', 'f'},
            'e': {'d', 'e', 'f'},
            'f': {'d', 'e', 'f'}
        }),
    ([], {}),
])
def test_get_graph(groups, expected):
    graph = get_graph(groups)
    assert graph == expected


@pytest.mark.parametrize("groups, expected", [
    ([['a', 'b'], ['b', 'c'], ['d', 'e', 'f']], [['a', 'b', 'c'], ['d', 'e', 'f']]),
    ([], []),
])
def test_get_connected_components(groups, expected):
    connected_components = get_distinct_upid_groups(groups)
    assert connected_components == expected


def test_get_first_item_from_list_containing_value():
    value = get_first_item_from_distinct_upid_group('b', [['a', 'b', 'c'], ['d', 'e']])
    assert value == 'a'


def test_get_first_item_from_list_containing_value_no_list_containing_value():
    with pytest.raises(ValueError):
        get_first_item_from_distinct_upid_group('x', [['a', 'b', 'c'], ['d', 'e']])


def test_get_first_item_from_list_containing_value_multiple_lists_containing_value():
    with pytest.raises(ValueError):
        get_first_item_from_distinct_upid_group('x', [['a', 'b', 'x'], ['d', 'x']])
