from typing import Mapping


UNSAFE_CHARS = '&;='
PAIR_SEPARATOR = ';'


class HeaderUnparseable(ValueError):
    pass


class HeaderNotEncodeable(ValueError):
    pass


def _is_safe_value(value: str) -> bool:
    unsafe_chars = UNSAFE_CHARS.split()
    for char in unsafe_chars:
        if char in str(value):
            return False
    return True


def mesh_header_encode(key_values: Mapping):
    """
    Encodes a key-value mapping into a MESH-header safe string in the format of:
    key1=value1;key2=value2;

    See UNSAFE_CHARS for a list of characters that are not supported in either of keys or values.

    Args:
        key_values: a mapping (e.g. a dict) of keys and values

    Returns:
        an encoded string that can be injected into a MESH header (e.g. subject field); keys will be alphabetized
    """
    for k, v in key_values.items():
        if not _is_safe_value(k):
            raise HeaderNotEncodeable('Unable to encode key %s', k)
        if not _is_safe_value(v):
            raise HeaderNotEncodeable('Unable to encode value %s', v)
    return PAIR_SEPARATOR.join(['{}={}'.format(*kv) for kv in sorted(key_values.items())]) + PAIR_SEPARATOR


def mesh_header_decode(key_value_string: str):
    if not key_value_string:
        return {}
    try:
        return {k: v for k, v in [
            kvpair_str.split('=') for
            kvpair_str in key_value_string.rstrip(PAIR_SEPARATOR).split(PAIR_SEPARATOR)
            if kvpair_str
        ]}
    except:
        raise HeaderUnparseable('Unable to parse header: %s', key_value_string)
