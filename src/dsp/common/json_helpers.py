from decimal import Decimal
from functools import partial

import json


def custom_sort_order(entry):
    """Variation of alphabetical sorting which promotes py:data:`keys_at_start` and demotes py:data:`keys_at_end`

    Args:
        entry (tuple) - tuple of the key and value to be sorted

    Returns:
        Modified key with prefix that will promote or demote the necessary keys.
    """
    keys_at_start = ['log_utc_time', 'log_level', 'action_type', 'action_status', 'message_type',
                     'description']
    keys_at_end = ['log_component', 'app', 'proc', 'host', 'task_uuid', 'timestamp']

    key = entry[0]
    if key in keys_at_start:
        modified_key = '1.{0}.{1}'.format(keys_at_start.index(key), key)
    elif key in keys_at_end:
        modified_key = '3.{0}.{1}'.format(keys_at_end.index(key), key)
    else:
        modified_key = '2.{0}'.format(key)
    return modified_key


class ISODateEncoder(json.JSONEncoder):
    """Encoder class used to extend the type capabilities of JSONEncoder."""

    def default(self, o):  # pylint: disable=method-hidden
        if hasattr(o, 'isoformat'):
            return o.isoformat()
        if isinstance(o, Decimal):
            return float(o)
        return json.JSONEncoder.default(self, o)


dump_to_json_no_ordering = partial(json.dumps, cls=ISODateEncoder, separators=(',', ':'))  # pylint: disable=invalid-name
"""Pre-configured json.dumps function with parameters suitable for structured log output.

``cls=NTSJSONEncoder`` - Additional encoding capabilities for types that simplejson doesn't handle by default
"""

dump_to_json = partial(json.dumps, item_sort_key=custom_sort_order, cls=ISODateEncoder,  # pylint: disable=invalid-name
                       separators=(',', ':'))
"""Pre-configured json.dumps function with parameters suitable for ordered structured log output.

``item_sort_key=custom_sort_order`` - sorts output alphabetically, promotes py:data:`keys_at_start` and demotes
py:data:`keys_at_end`
``cls=NTSJSONEncoder`` - Additional encoding capabilities for types that simplejson doesn't handle by default
"""


def pretty_json(input_data):
    """Formatted JSON output with line breaks and indentation, also removes the "ns:" prefix from keys since they don't
    play nice with the JSON tool jq.

    :param input_data: Data to be output
    :return: string representation of the data in JSON.
    """
    json_output = json.dumps(input_data, indent=2, item_sort_key=custom_sort_order, for_json=True, cls=ISODateEncoder)
    return json_output.replace('ns:', '')
