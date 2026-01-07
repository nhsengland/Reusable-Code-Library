import asyncio
import concurrent.futures as concurrent
import contextlib
import hashlib
import os
from datetime import datetime, date
from functools import partial, wraps
from time import sleep
from typing import Any, Generator, IO, List, Optional, Union, Dict, Iterable, Tuple, Sequence, Mapping, Callable

from shared.constants import PATHS
from shared.content_types import STANDARD_DATE_FORMAT, TIMESTAMP_FORMAT, NO_SEPARATOR_DATE_FORMAT, \
    ISO8601_DATETIME_FORMAT
from shared.logger import add_temporary_global_fields

DEFAULT_SPARK_VERSION = '6.4.x-esr-scala2.11'


def base36encode(number: int):
    if not isinstance(number, int):
        raise TypeError('number must be an integer')
    if number < 0:
        raise ValueError('number must be positive')

    alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    base36 = ''

    while number:
        number, i = divmod(number, 36)
        base36 = alphabet[i] + base36

    return base36 or '0'


def base36decode(number):
    return int(number, 36)


def strtobool(val: Union[str, int, bool]) -> bool:
    if isinstance(val, bool):
        return val
    val = str(val).lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return True
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return False
    else:
        raise ValueError("invalid truth value %r" % (val,))


def generate_timestamp() -> int:
    return format_timestamp(datetime.utcnow())


def format_timestamp(timestamp: datetime) -> int:
    return int(timestamp.strftime(TIMESTAMP_FORMAT))


def format_date(timestamp: datetime) -> int:
    return int(timestamp.strftime(NO_SEPARATOR_DATE_FORMAT))


def parse_iso_8601_string(date_string: str):
    return datetime.strptime(date_string, ISO8601_DATETIME_FORMAT)


def parse_timestamp(timestamp: Union[int, str, date, datetime]) -> datetime:
    if isinstance(timestamp, datetime):
        return timestamp

    if isinstance(timestamp, date):
        return datetime.combine(timestamp, datetime.min.time())

    return datetime.strptime(str(timestamp).ljust(20, '0'), TIMESTAMP_FORMAT)


def parse_date(timestamp: Union[int, str]) -> Optional[datetime]:
    """
    Parse a date into a datetime object.

    This function can accept a string in %Y-%m-%d format, a date object or a datetime object.

    params:
        timestamp: A date or datetime object or a string representation of a date
    returns:
        A datetime object representing the specified date
    """
    if timestamp is None:
        return None

    if isinstance(timestamp, datetime):
        return timestamp

    if isinstance(timestamp, date):
        return datetime.combine(timestamp, datetime.min.time())

    return datetime.strptime(str(timestamp), STANDARD_DATE_FORMAT)


def format_submission_id(submission_id: int) -> str:
    return str(submission_id).zfill(20)


def submission_path(submission_id: int, *args, root: str = PATHS.SUBMISSIONS) -> str:
    return os.path.join(root, format_submission_id(submission_id), *args)


def enumlike_members(klass: Any, include_bases: bool = False) -> List[str]:
    return list(_enumlike_items(klass, include_bases))


def enumlike_values(klass: Any, include_bases: bool = False) -> List[Union[str, int, float, bool]]:
    return list(_enumlike_items(klass, include_bases).values())


def _enumlike_items(klass: Any, include_bases: bool = False) -> Dict[str, Union[str, int, float, bool]]:
    """Return dictionary of "constants" of a class: non-private attributes with scalar value."""
    items = {}
    for member in klass.__dict__:
        if member.startswith('_'):
            continue
        # TODO: check that all constants are uppercase, convert if necessary.
        # if not re.search('^[A-Z][A-Z_0-9]*$', member):
        #     continue
        value = getattr(klass, member)
        if not isinstance(value, (str, int, float, bool)):
            continue
        items[member] = value

    if include_bases:
        for base in klass.__bases__:
            items = {**items, **_enumlike_items(base, include_bases)}

    return items


@contextlib.contextmanager
def rewinding(file: IO) -> Generator[IO, None, None]:
    position = file.tell()
    yield file
    file.seek(position)


class ConcurrentExceptions(Exception):

    def __init__(self, *exceptions: Iterable[Tuple[str, Exception]]):
        super(ConcurrentExceptions, self).__init__(*exceptions)

        self.exceptions = {tid: e for tid, e in exceptions}  # type: Mapping[str, Exception]

    def __repr__(self):
        return '\n'.join(t + ': ' + repr(e) for t, e in self.exceptions.items())

    def __str__(self):
        child_errors = '\n'.join(t + ': ' + str(e) for t, e in self.exceptions.items())
        return 'Inner Exceptions:\n' + child_errors


def concurrent_tasks(
        parallels: Iterable[Tuple[str, Callable, Sequence[Any]]],
        raise_if_ex: bool = True, with_results=True, max_workers: int = None
) -> Mapping[str, Any]:
    """
    Execute a collection of tasks in parallel, wait for all tasks to complete and return the outcomes

    Args:
        parallels: The tasks to be queued to execute, each specified as a three-tuple: the first element is a string to
            use as an identifier for the task; the second is the callable to be invoked; and the third a sequence of
            arguments to be passed to the callable
        raise_if_ex: Whether to raise an instance of ConcurrentExceptions should any of the tasks raise an exception
        with_results: Whether to return the task results - if set to false, the returned dictionary will just contain
            either a True or a False value depending on the success of the task

    Returns:
        A dictionary keyed by the task IDs and whose values are either the value produced by the task or the exception
        raised by the task; or, if with_results is set to False, True for tasks that ran successfully and False for
        tasks which raised exceptions
    """
    with concurrent.ThreadPoolExecutor(max_workers=max_workers) as executor:
        task_ids = {}
        futures = []
        for task_id, func, args in parallels:
            f = executor.submit(
                func, *args
            )
            task_ids[id(f)] = str(task_id)
            futures.append(f)

        results = {}
        exceptions = []
        for future in concurrent.as_completed(futures):
            task_id = task_ids[id(future)]
            ex = future.exception()
            if not ex:
                results[task_id] = future.result() if with_results else True
                continue
            results[task_id] = ex if with_results else False
            exceptions.append((future, ex))

        if not raise_if_ex or not exceptions:
            return results

        if len(exceptions) == 1:
            exceptions[0][0].result()

        raise ConcurrentExceptions(*((task_ids[id(f)], e) for f, e in exceptions))


async def coro(func: callable, *args, **kwargs):
    """
        coroutine wrapper to wrap non async calls
    Args:
        func: the function to call
        *args: positional args to pass to fund
        **kwargs: kwargs to pass to func

    Returns:

    """

    return func(*args, **kwargs)


def create_task(func: callable, *args, **kwargs):
    """
        coroutine wrapper to wrap non async calls
    Args:
        func: the function to call
        *args: positional args to pass to fund
        **kwargs: kwargs to pass to func

    Returns:

    """

    return asyncio.ensure_future(coro(func, *args, **kwargs))


async def run_in_executor(func: callable, *args, **kwargs):
    """
        async wrapper k
    Args:
        func: the function to call
        *args: positional args to pass to fund
        **kwargs: kwargs to pass to func

    Returns:

    """

    loop = asyncio.get_running_loop()

    to_execute = partial(func, *args, **kwargs)

    return await loop.run_in_executor(None, to_execute)


def calculate_hash(hash_type: str, body: IO):
    """
    Calculate hash of the IO using specified algorithm.
    Args:
        hash_type (str): hash function
        body (IO): bytes-like object for which the hash is calculated

    Returns:
        str: hash of the provided object
    """
    hash_object = getattr(hashlib, hash_type)()
    chunk = body.read(1024 * 1024 * 8)
    if not chunk:
        return None
    while chunk:
        hash_object.update(chunk)
        chunk = body.read(1024 * 1024 * 8)
    return hash_object.hexdigest()


def retry(
        max_retries: int = 6,
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
