import os
import re
from functools import partial, wraps
from time import sleep
from typing import Callable

import pip

from src.dsp.common import enum
from src.nhs_reusable_code_library.resuable_codes.shared.aws import local_mode
from src.nhs_reusable_code_library.resuable_codes.shared.logger import add_temporary_global_fields

_re_replaces = [
    (re.compile('^ns:', re.IGNORECASE), ''),
    (re.compile('%'), '_PERC'),
    (re.compile('#'), '_NUM'),
    (re.compile('[^A-Z_0-9]+'), '_'),
    (re.compile('__'), '_'),
    (re.compile('__'), '_')
]


def canonical_name(raw_field_name: str) -> str:
    """ Create a safe name for python vars and parquet

    Args:
        raw_field_name: the raw field name

    Returns:

    """

    field = (raw_field_name or '').strip().upper()

    if not field:
        raise ValueError(raw_field_name)

    for regex, replacement in _re_replaces:
        field = regex.sub(replacement, field)

    field = field.strip('_')

    return field


def canonical_postcode(value):
    return None if value is None else value.replace(' ', '').upper()


def scope_name(scope: str, field: str) -> str:
    return f'{scope}.{field}'


rhs = partial(scope_name, 'rhs')
lhs = partial(scope_name, 'lhs')
meta = partial(scope_name, 'meta')


class ContentTypes(enum.Enum):
    JSON = 'application/json'
    ZLIB = 'application/zlib'
    BASE64 = 'application/base64'
    MSGPACK = 'application/msgpack'
    RAW = 'application/octet-stream'
    MSGPACK_AND_ZLIB = 'application/x-msgpack+zlib'
    PLAIN_TEXT = 'text/plain'


# TODO: check if this can be done better in python3
class SentinelType:
    """
    Base class for all sentinel types
    """

    def __nonzero__(self):
        """
        Like NoneType, Sentinel Types should always be considered False
        :return: ``False``
        """
        return False

    def for_json(self):
        """Project for json output"""
        return str(self)


class _NotApplicable(SentinelType):
    """
    Sentinel type to represent the case where a derivation or validation has been marked a not applicable for a
    particular CDS Record type.
    """

    def derive(self, _):
        """
        A dummy derive method so that when NotApplicable is used in place of a derivation, calling it will return
        NotApplicable
        :return: self
        """
        return self

    def validate(self, _):
        """
        A dummy validate method so that when NotApplicable is used in place of a validation, calling it will return
        NotApplicable
        :return: self
        """
        return self

    def __repr__(self):
        return '(not applicable)'

    def __reduce__(self):
        """ The local name of the singleton instance of this class.

        Used by pickle to ensure object identity is consistent when serializing.

        Returns:
            str: The local name of the singleton instance
        """
        return 'NotApplicable'


NotApplicable = _NotApplicable()
del _NotApplicable


class _DatumUnavailable(SentinelType):
    """
    Sentinel type to represent the case where an item of data is not available for a specific CDS Type and so the token
    or key is defined as DatumUnavailable to represent the case where it is not possible to use it.
    """

    def __repr__(self):
        return '(datum unavailable)'

    def __reduce__(self):
        """ The local name of the singleton instance of this class.

        Used by pickle to ensure object identity is consistent when serializing.

        Returns:
            str: The local name of the singleton instance
        """
        return 'DatumUnavailable'


DatumUnavailable = _DatumUnavailable()
del _DatumUnavailable


def is_sentinel_type(thing):
    """Safely determines if some 'thing' is a sentinel type

    Args:
        thing (Any): The thing to check for sentinel type-ness

    Returns:
        bool: True if 'thing' is a sentinel type
    """
    if thing is DatumUnavailable:
        return True
    if thing is NotApplicable:
        return True
    return False


def site_packages_dir() -> str:
    return os.path.dirname(os.path.dirname(pip.__file__))


def maven_jar(jar: str, is_local_mode=None) -> str:
    is_local_mode = local_mode() if is_local_mode is None else is_local_mode

    if is_local_mode:
        jar_dir = os.path.join(site_packages_dir(), "pyspark/jars")
    elif os.path.isdir('/dbfs/mnt/resources/lib/common'):
        jar_dir = os.path.abspath("/dbfs/mnt/resources/lib/common")
    else:
        jar_dir = "/databricks/jars"

    jar_path = os.path.join(jar_dir, jar)
    assert os.path.isfile(jar_path), "No jar installed {}".format(jar_path)
    return jar_path


class DataTypeConversionError(ValueError):
    pass


def retry(
        max_retries=6,
        should_retry_predicate: Callable = None,
        constant_backoff: int = 0,
        linear_backoff_multiplier: int = 0,
        exponential_backoff: bool = False
):
    """Function decorator to allow functions to be retried. Important... if you want to log each of the
    retries, apply the log_action() decorator below the @retry() decorator on the function that you want
    to retry.

    Args:
        max_retries: maximum number of retries default 6
        should_retry_predicate: a function to call to return whether an exception warrants another retry (True) or not (False)
        constant_backoff: a backoff constant - same backoff every time
        linear_backoff_multiplier: a multiplier by which to multiple the number of retries to calculate how much to sleep
        exponential_backoff: the exponent power of 2 to calculate how much to sleep
    """

    def _wrapping(f):
        @wraps(f)
        def _wrapper(*args, **kwargs):
            retries = 0
            while True:
                with add_temporary_global_fields(retries=retries):
                    try:
                        result = f(*args, **kwargs)
                        return result
                    except BaseException as err:
                        if should_retry_predicate:
                            if not should_retry_predicate(err):
                                raise err

                        retries += 1
                        if retries > max_retries:
                            raise err

                        if exponential_backoff:
                            sleep(pow(2, retries))
                        elif linear_backoff_multiplier:
                            sleep(retries * linear_backoff_multiplier)
                        else:
                            sleep(constant_backoff)

        return _wrapper

    return _wrapping


def safe_column_name(column_name: str):
    """
    Removes special characters from column name, if no column name is passed then ValueError is raised
    Args:
        column_name: column name to remove special characters from

    Returns:
        column name without special characters
    """
    if not column_name:
        raise ValueError('column name required')

    return re.sub(r"[ !@#$%^&*'`|/\\,+=~;:?<>{}\[\]()\"]", '', column_name)
