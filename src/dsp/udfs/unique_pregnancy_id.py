from collections import defaultdict
from typing import List, Any, Dict, Set


def get_graph(groups: List[List[str]]) -> Dict[Any, Set[str]]:
    """
        Converts list of lists of UPID to graph
    Args:
        groups: List[List[str]] - list of lists upids e.g. [['a', 'b'], ['b', 'c'], ['d', 'e', 'f']]

    Returns:
        graph: Dict[Any, List[str] - graph representation of input, e.g.
         {
            'a': { 'a', 'b' },
            'b': { 'a', 'b', 'c' },
            'c': { 'b', 'c' },
            'd': { 'd', 'e', 'f' },
            'e': { 'd', 'e', 'f' },
            'f': { 'd', 'e', 'f' }
        }

    """
    graph = defaultdict(set)

    for group in groups:
        for item in group:
            graph[item] = graph[item].union(set(group))

    return graph


def get_distinct_upid_groups(groups:  List[List[str]]) -> List[List[str]]:
    """
        Converts list of lists of upids to new list of distinct upid lists,
    Args:
        groups: List[List[str]] - list of lists of UPIDs e.g. [['a', 'b'], ['b', 'c'], ['d', 'e', 'f']]

    Returns:
        graph: List[List[str]] - [[ 'a', 'b', 'c'], ['d', 'e', 'f']]
    """
    graph = get_graph(groups)

    already_seen = set()
    result = []
    for node in graph:
        if node not in already_seen:
            connected_group, already_seen = _get_connected_group(node, already_seen, graph)
            result.append(connected_group)
    return result


def _get_connected_group(node: str, already_seen: Set[str], graph: Dict[Any, Set[Any]]):
    result = []
    nodes = {node}
    while nodes:
        node = nodes.pop()
        already_seen.add(node)
        nodes = nodes or graph[node] - already_seen
        result.append(node)
    result.sort()
    return result, already_seen


def get_first_item_from_distinct_upid_group(upid: str, upid_lists: List[List[str]]) -> str:
    """
        Returns the first item from a list containing a given value, given a list of lists
        If no list or multiple lists contain that value, raise ValueError
    Args:
        upid: str
        upid_lists: List[List[str]] - distinct groups of UPIDS

    Returns:
        first_upid: str - first UPID from distinct UPID group containing that UPID
    """
    lists_containing_value = list(filter(lambda l: upid in l, upid_lists))
    if len(lists_containing_value) == 0:
        raise ValueError("No list contains value")
    if len(lists_containing_value) > 1:
        raise ValueError("Multiple lists contain value")

    first_upid = lists_containing_value[0][0]
    return first_upid


