import io
import logging
import os
import traceback
from typing import Tuple, Union, Callable


def find_caller_info(caller_of: Callable = None, stack_info: bool = False) -> Tuple[str, int, str, Union[str, None]]:
    """ cloned from python logger to allow caller info from app logger context

    Args:
        caller_of (function): function to find the caller of
        stack_info (bool): whether to include the stack info

    Returns:
        Tuple[str, int, str, Union[str, None]]: tuple filename, line number, function , stack info
    """

    try:
        return _find_caller_info(caller_of, stack_info)
    except ValueError:  # pragma: no cover
        return "(unknown file)", 0, "(unknown function)", None


def _find_caller_info(caller_of: Callable = None, stack_info: bool = False):
    """ cloned from python logger to allow caller info from app logger context

    Args:

        caller_of (function): function to find the caller of
        stack_info (bool): whether to include the stack info

    Returns:
        Tuple[str, int, str, Union[str, None]]: tuple filename, line number, function , stack info
    """
    f = logging.currentframe()

    callee_func = caller_of.__code__.co_name if caller_of else None
    callee_path = os.path.normcase(caller_of.__code__.co_filename) if caller_of else None

    if f is not None:
        f = f.f_back

    while hasattr(f, "f_code"):
        fco = f.f_code
        filename = os.path.normcase(fco.co_filename)
        func_name = fco.co_name

        if callee_path and filename == callee_path:
            f = f.f_back
            if callee_func and callee_func == func_name:
                callee_path = None
            continue

        if not callee_path and filename.startswith(logger_module) and not filename.endswith('logger_tests.py'):
            f = f.f_back
            continue

        sinfo = None
        if stack_info:
            sio = io.StringIO()
            sio.write('Stack (most recent call last):\n')
            traceback.print_stack(f, file=sio)
            sinfo = sio.getvalue()
            if sinfo[-1] == '\n':
                sinfo = sinfo[:-1]
            sio.close()

        return fco.co_filename, f.f_lineno, fco.co_name, sinfo

    return "(unknown file)", 0, "(unknown function)", None


logger_module = os.path.dirname(os.path.normcase(_find_caller_info.__code__.co_filename))
