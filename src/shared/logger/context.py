import asyncio
import logging
import threading
from time import time
from typing import Any, AnyStr, Dict, List, Optional, Tuple, Union
from uuid import uuid4
from weakref import WeakKeyDictionary

from shared.logger import find_caller_info
from .constants import Constants
from .logger import app_logger


CallerInfo = Tuple[str, int, str, Optional[str]]


class LoggedAction(object):

    def __init__(
            self,
            register_global: bool = True,
            forced_log_level: bool = False,
            caller_info: Optional[CallerInfo] = None,
            **fields
    ):
        self.fields = {}
        self.register_global = register_global
        self.forced_log_level = forced_log_level
        self.start_time = None
        self.end_time = None
        self.caller_info = caller_info
        self.add_fields(**(fields or {}))

    @property
    def action_type(self):
        return self.fields.get(Constants.ACTION_FIELD, 'not_set')

    @property
    def log_level(self):
        return self.fields.get(Constants.LOG_LEVEL, Constants.DEFAULT_LOG_LEVEL)

    @property
    def task_uuid(self):
        return self.fields[Constants.TASK_UUID_FIELD]

    def __enter__(self):
        if self.register_global:
            logging_context.push(self)

        self.start_time = time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.register_global:
            popped_action = logging_context.pop()
            if not popped_action == self:
                raise ValueError('Mismatch action popped from stack!')

        self.end_time = time()
        exec_info = (exc_type, exc_val, exc_tb) if exc_val else None
        logging_context.handle_action_complete(self, exec_info)

        return False

    def add_fields(self, **fields):
        if not fields:
            return

        self.fields.update(
            {
                k: v
                if not v or not hasattr(v, 'to_primitive')
                else v.to_primitive()
                for k, v in fields.items()
            }
        )

    def serialize_for_broadcast(self, **other_args):
        to_broadcast = other_args
        to_broadcast[Constants.TASK_UUID_FIELD] = self.task_uuid
        to_broadcast[Constants.LOG_AT_LEVEL] = logging.getLevelName(logging_context.log_at_level())
        return to_broadcast


class _ThreadLocalContextStorage(threading.local):

    def __init__(self):
        self._stack = []  # type: List[LoggedAction]
        self._global_fields = {}  # type: Dict[str, Any]
        self._force_log_at_levels = []  # type: List[int]

    @property
    def stack(self) -> List[LoggedAction]:
        return self._stack

    @property
    def global_fields(self) -> Dict[str, Any]:
        return self._global_fields

    @property
    def forced_log_levels(self) -> List[int]:
        return self._force_log_at_levels


class _TaskIsolatedContextStorage:

    def __init__(self):
        self._task_store = WeakKeyDictionary()

    def _get_task_store(self):
        try:
            task = asyncio.current_task()
        except RuntimeError:
            # No loop for this thread:
            return None
        if not task:
            return None

        if task not in self._task_store:
            self._task_store[task] = dict(stack=[], globals={}, forced_levels=[])

        return self._task_store[task]

    @property
    def stack(self) -> Optional[List[LoggedAction]]:

        return self._get_task_store()['stack']

    @property
    def global_fields(self) -> Optional[Dict[str, Any]]:

        return self._get_task_store()['globals']

    @property
    def forced_log_levels(self) -> List[int]:

        return self._get_task_store()['forced_levels']


class _LoggingContext(threading.local):
    """
    Call stack-based context, storing the current L{Action}.

    Bit like L{twisted.python.context}, but:

    - Single purpose.
    - Allows support for Python context managers (this could easily be added
      to Twisted, though).
    - Does not require Twisted; Eliot should not require Twisted if possible.
    """

    def __init__(self, storage: Union[_TaskIsolatedContextStorage, _ThreadLocalContextStorage] = None):
        self._storage = storage

    configured = False

    def __getstate__(self):
        attributes = self.__dict__.copy()
        attributes["_storage"] = None
        return attributes

    @property
    def storage(self):
        if self._storage is None:
            self._storage = _ThreadLocalContextStorage()

        return self._storage

    def force_log_at_level(self, log_at_level: Union[int, str]):
        """ push a forced log level onto the stack

        Args:
            log_at_level (Union[int, str]): the log level to force
        """

        log_level = logging.getLevelName(log_at_level) if isinstance(log_at_level, str) else log_at_level
        self.storage.forced_log_levels.append(log_level)

    def undo_force_log_at_level(self):
        self.storage.forced_log_levels.pop(-1)

    def log_at_level(self) -> int:
        if self.storage.forced_log_levels:
            return self.storage.forced_log_levels[-1]

        return app_logger.log_at_level

    def push(self, action: LoggedAction):
        """

        Args:
            action (LoggedAction): logged action to track

        Returns:

        """
        self.storage.stack.append(action)

    def pop(self) -> LoggedAction:
        """ pop the last logged action from the stack

        Returns:
            LoggedAction
        """
        return self.storage.stack.pop(-1)

    def current(self) -> Union[LoggedAction, None]:
        """returns the most recent action from the logging stack

        Returns:
            LoggedAction: the action logging class
        """
        if not self.storage.stack:
            return None
        return self.storage.stack[-1]

    def add_global_fields(self, **fields):
        """extend the global fields added the all messages

        Args:
            **fields:
        """
        if not fields:
            return

        self.storage.global_fields.update(
            {
                k: v
                if not v or not hasattr(v, 'to_primitive')
                else v.to_primitive()
                for k, v in fields.items()
            }
        )

    def remove_global_field(self, field):
        """ remove a global field

        Args:
            field (str): field name

        """
        self.storage.global_fields.pop(field, None)

    def register_exception_extractor(self, exc_type, extractor_func):
        """ adds a specialised helper to enhance the

        Args:
            exc_type (Type): Exception type
            extractor_func (Callable[Exception,dict]):

        """
        if not extractor_func:
            if exc_type in self._exception_extractors:
                del [self._exception_extractors]
            return

        self._exception_extractors[exc_type] = extractor_func

    def emit_message(self, message):
        """ write a message to the lgo
        Args:
            message (dict): fields to write

        """

        log_level = message.get(Constants.LOG_LEVEL, Constants.DEFAULT_LOG_LEVEL)

        log_level = logging.getLevelName(log_level) if isinstance(log_level, str) else log_level

        if log_level < self.log_at_level():
            # do nothing
            return

        final_message = {}
        final_message.update(self.storage.global_fields)
        final_message.update(message)
        final_message[Constants.TIMESTAMP_FIELD] = final_message.get(Constants.TIMESTAMP_FIELD, time())
        final_message.pop(Constants.LOG_LEVEL, None)

        exc_info = final_message.pop(Constants.EXEC_INFO_FIELD, None)
        caller_info = final_message.pop(Constants.CALLER_INFO_FIELD, None)

        app_logger.log(log_level, exc_info, args=lambda: final_message, caller_info=caller_info)

    def start_action(
            self,
            register_global: bool = True,
            caller_info: Optional[CallerInfo] = None,
            **fields
    ):
        """ Start a new logger action

        If register_global is true or omitted, the new action is pushed onto the global logger stack.
        This will mean it will be the new "current action" and accessible via the global "add_field"
        function.

        If register_global is false, the action is not pushed onto the stack and is only accessible
        via its own methods.

        It is recommended to set register_global to False when starting an action within a generator.

        Args:
            register_global (Optional[bool]): True if this action should be pushed onto the global stack
            caller_info (Union[Tuple[str, int, str, Union[str, None]], None]): caller info

        Returns:
            LoggerAction: The new logger action
        """

        caller_info = caller_info or find_caller_info(self.start_action, True)

        requested_log_level = fields.get(Constants.LOG_AT_LEVEL)
        if requested_log_level is not None:
            del fields[Constants.LOG_AT_LEVEL]
            self.force_log_at_level(requested_log_level)

        log_level = fields.get(Constants.LOG_LEVEL, Constants.DEFAULT_LOG_LEVEL)
        fields[Constants.LOG_LEVEL] = log_level

        task_uuid = fields.get(Constants.TASK_UUID_FIELD, None)

        if not task_uuid:
            last_task = self.current()
            task_uuid = last_task.task_uuid if last_task else uuid4().hex
            fields[Constants.TASK_UUID_FIELD] = task_uuid

        action = LoggedAction(
            register_global=register_global,
            forced_log_level=(requested_log_level is not None),
            caller_info=caller_info,
            **fields
        )

        return action

    def handle_action_complete(self, action: LoggedAction, exec_info: Union[Tuple[type, Exception, Any], None]):
        """ when the action completes this should be called

        Args:
            action (LoggedAction): the logged action
            exec_info (Tuple[type, object, traceback]): exception info

        """
        message = {}
        message.update(action.fields)

        message[Constants.EXECUTION_SECONDS_FIELD] = message.get(
            Constants.EXECUTION_SECONDS_FIELD, float('%0.7f' % (action.end_time - action.start_time))
        )

        if exec_info:
            message[Constants.MESSAGE_TYPE_FIELD] = message.get(Constants.MESSAGE_TYPE_FIELD, Constants.TRACEBACK_FIELD)
            message[Constants.EXEC_INFO_FIELD] = exec_info

        message[Constants.ACTION_STATUS_FIELD] = Constants.FAILED_STATUS \
            if exec_info is not None else Constants.SUCCEEDED_STATUS

        if exec_info:
            message[Constants.LOG_LEVEL] = logging.ERROR

        self.emit_message(message)

        if action.forced_log_level:
            self.undo_force_log_at_level()

    def async_context_storage(self):

        self._storage = _TaskIsolatedContextStorage()

    def thread_local_context_storage(self):

        self._storage = _ThreadLocalContextStorage()


logging_context = _LoggingContext()


def safe_str(obj: AnyStr) -> bytes:
    """
    Convert any arbitrary object into a UTF-8 encoded str

    @param obj: An object of some sort.

    @return: C{str(obj)}
    @rtype: C{str}
    """
    if isinstance(obj, bytes):
        return obj

    return obj.encode("utf-8", errors='backslashreplace')
