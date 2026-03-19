import logging
import os
import sys
from typing import Callable, Union, Tuple, List, Type

from dsp.shared.logger.caller_info import find_caller_info
from dsp.shared.logger.constants import Constants
from dsp.shared.logger.formatters import JSONFormatter
from dsp.shared.logger.handlers import sys_std_handlers


class _Logger:

    def __init__(self):

        logging.addLevelName(5, 'TRACE')
        logging.addLevelName(25, 'NOTICE')
        logging.addLevelName(30, 'AUDIT')

        for handler in logging.root.handlers:
            handler.formatter = JSONFormatter()

        self.service_name = 'NOT SET'
        self._logger = None
        self.log_at_level = logging.getLevelName(os.environ.get('LOG_AT_LEVEL', Constants.DEFAULT_LOG_AT_LEVEL))
        self._is_setup = False

    def setup(self,
              service_name: str,
              handlers: List[logging.Handler] = None,
              append: bool = False,
              overwrite: bool = False
              ):
        """Set up the logger with a bunch of handlers. Is a no-op if the setup has already been performed.

        Args:
            service_name: the name for this logger
            handlers: an optional list of handlers for this logger
            append: append the list of handlers to the root ones
            overwrite: overwrite the list of handlers with the provided list, or if none are provided, with a set of \
            standard ones
        """
        if not self._is_setup:
            if handlers and append:
                logging.root.handlers += handlers
            elif overwrite or not logging.root.handlers:
                logging.root.handlers = (handlers or sys_std_handlers())

            self._logger = None
            self.service_name = service_name
            self._is_setup = True

    def logger(self) -> logging.Logger:

        if not self._logger:
            self._logger = logging.getLogger(self.service_name)
            self._logger.setLevel(self.log_at_level)

        return self._logger

    def log(
            self, log_level: int = logging.INFO, exc_info=None,
            args: Union[str, Callable[[], dict], None] = None, caller_info=None
    ):

        if log_level < self.log_at_level:
            return

        pathname, line_no, func, stack_info = (caller_info if caller_info else find_caller_info(stack_info=True))

        msg = None
        if isinstance(args, str):
            msg = args
            args = {'message': args}

        record_args = [args] if args else None

        log_record = logging.LogRecord(
            msg=msg,
            name=self.service_name,
            level=log_level,
            pathname=pathname,
            lineno=line_no,
            args=record_args,
            exc_info=exc_info,
            func=func,
            sinfo=stack_info if log_level >= logging.WARNING else None
        )

        self.logger().handle(log_record)

    def trace(self, args: Union[str, Callable[[], dict], None] = None):

        self.log(log_level=5, args=args)

    def debug(self, args: Union[str, Callable[[], dict], None] = None):

        self.log(log_level=logging.DEBUG, args=args)

    def info(self, args: Union[str, Callable[[], dict], None] = None):

        self.log(log_level=logging.INFO, args=args)

    def notice(self, args: Union[str, Callable[[], dict], None] = None):

        self.log(log_level=25, args=args)

    def audit(self, args: Union[str, Callable[[], dict], None] = None):

        self.log(log_level=logging.WARN, args=args)

    def warn(self, args: Union[str, Callable[[], dict], None] = None):

        self.log(log_level=logging.WARN, args=args)

    def error(self, args: Union[str, Callable[[], dict], None] = None):

        self.log(log_level=logging.ERROR, args=args)

    def fatal(self, args: Union[str, Callable[[], dict], None] = None):

        self.log(log_level=logging.FATAL, args=args)

    def critical(self, args: Union[str, Callable[[], dict], None] = None):

        self.log(log_level=logging.CRITICAL, args=args)

    def exception(
            self, args: Union[str, Callable[[], dict], None] = None,
            exc_info: Tuple[Type, Exception, object] = None, log_level: int = None
    ):

        exc_info = exc_info or sys.exc_info()

        log_level = log_level or logging.ERROR

        self.log(log_level=log_level, exc_info=exc_info, args=args)


app_logger = _Logger()
