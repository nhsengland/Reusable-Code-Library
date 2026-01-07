import logging
import sys
from typing import List

from shared.logger.formatters import StructuredFormatter, JSONFormatter

_filter_are_errors = staticmethod(lambda r: r.levelno >= logging.ERROR)
_filter_not_errors = staticmethod(lambda r: r.levelno < logging.ERROR)


class CapturingHandler(logging.Handler):

    def __init__(self, messages: List[dict], level=logging.NOTSET):
        super(CapturingHandler, self).__init__(level)
        self.messages = messages
        self.formatter = StructuredFormatter()

    def emit(self, record: logging.LogRecord):

        log = self.format(record)

        self.messages.append(log)


def capturing_log_handlers(stdout_cap: List[dict], stderr_cap: List[dict]):

    stdout_handler = CapturingHandler(stdout_cap)
    stdout_handler.addFilter(type('', (logging.Filter,), {'filter': _filter_not_errors}))

    stderr_handler = CapturingHandler(stderr_cap)
    stderr_handler.setLevel(logging.ERROR)
    stderr_handler.addFilter(type('', (logging.Filter,), {'filter': _filter_are_errors}))

    return [stdout_handler, stderr_handler]


def sys_std_handlers():

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(JSONFormatter())
    stdout_handler.addFilter(type('', (logging.Filter,), {'filter': _filter_not_errors}))

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.ERROR)
    stderr_handler.setFormatter(JSONFormatter())
    stderr_handler.addFilter(type('', (logging.Filter,), {'filter': _filter_are_errors}))

    return [stdout_handler, stderr_handler]
