"""A log handler using the resource handling"""

import logging
import shutil
import sys
import tempfile
import warnings
import weakref
from collections.abc import Iterator
from contextlib import contextmanager
from threading import Lock, RLock
from typing import IO, ClassVar, Optional, Union

from dve.parser.exceptions import LogDataLossWarning
from dve.parser.file_handling.service import open_stream
from dve.parser.type_hints import URI


@contextmanager
def null_context() -> Iterator[None]:
    """A null context manager to be used in place of a lock."""
    yield None


class _ResourceHandlerManager:
    """A manager to ensure that there is only a single resource handler
    for each URI at any one time.

    """

    def __init__(self):
        self._extant_handlers: dict[URI, "ResourceHandler"] = weakref.WeakValueDictionary()
        self._lock = RLock()

    def get_handler(
        self,
        level: Union[str, int] = logging.NOTSET,
        *,
        resource: URI,
        type_: type["ResourceHandler"],
    ) -> "ResourceHandler":
        """Get a handler for a given URI."""
        with self._lock:
            handler = self._extant_handlers.get(resource, None)

            if not handler:
                handler = object.__new__(type_)
                self._extant_handlers[resource] = handler
            elif level is not logging.NOTSET:
                if not isinstance(level, int):
                    requested_level = logging.getLevelName(level)
                    if not isinstance(requested_level, int):
                        warnings.warn(
                            f"Requested level {level!r} not a log level, setting to DEBUG"
                        )
                        requested_level = logging.DEBUG
                else:
                    requested_level = level
                if requested_level < handler.level:
                    warnings.warn(
                        f"Changing level for existing {resource!r} log handler to "
                        f"lower level {requested_level} (current: {handler.level})",
                        category=LogDataLossWarning,
                    )
                    handler.level = requested_level
                elif requested_level > handler.level:
                    warnings.warn(
                        f"Not changing level for existing {resource!r} log handler to "
                        f"higher level {requested_level} (current: {handler.level})",
                        category=LogDataLossWarning,
                    )

            return handler

    def remove_handler(self, resource: URI):
        """Remove a handler for a given URI, if present."""
        with self._lock:
            try:
                del self._extant_handlers[resource]
            except KeyError:
                pass

    def close(self):  # pragma: no cover
        """Close and remove the handlers for the manager."""
        with self._lock:
            for handler in list(self._extant_handlers.values()):
                try:
                    handler.close()
                except:  # pylint: disable=W0702
                    pass


class ResourceHandler(logging.Handler):
    """A log handler which writes log records to resources using `open_stream`.

    This will work via a temporary file, and records will only be written upon
    the handler closing. A finalizer is registered for the handler, which
    should mean the handler is cleanly closed on exit. It would be wise
    to call `logging.shutdown()` explicitly.

    The remote resource will always be truncated before writing.

    """

    _MANAGER: ClassVar[_ResourceHandlerManager] = _ResourceHandlerManager()
    """The manager for the resource handlers."""

    def __new__(cls, level=logging.NOTSET, *, resource: URI) -> "ResourceHandler":
        """Creates a new ResourceHandler"""
        return cls._MANAGER.get_handler(level, resource=resource, type_=cls)

    def __init__(self, level=logging.NOTSET, *, resource: URI) -> None:
        if hasattr(self, "_stream"):
            return

        super().__init__(level)
        self._resource: URI = resource
        # Clear the location and ensure we can write to it. Better to know
        # early if we're going to have issues.
        with open_stream(resource, "wb"):
            pass

        # pylint: disable=consider-using-with
        self._stream: IO[str] = tempfile.NamedTemporaryFile("r+")
        # Register the finalizer to be called when the handler is garbage collected
        # or at exit.
        weakref.finalize(
            self,
            self._cleanup,
            lock=self.lock,
            stream=self._stream,
            resource=self._resource,
        )

    @property
    def resource(self) -> URI:  # pragma: no cover
        """The URI that the logs will be written to upon handler closure."""
        return self._resource

    @property
    def closed(self) -> bool:
        """Whether the handler is closed."""
        self.acquire()  # Wait for lock in case cleanup in progress.
        try:
            return self._stream.closed
        finally:
            self.release()

    def emit(self, record: logging.LogRecord):
        """Emit a record."""
        self.acquire()
        try:
            try:
                msg = self.format(record)
                self._stream.write(msg + "\n")
                self._stream.flush()
            # pylint: disable=bare-except
            except:  # noqa: E722 pragma: no cover
                self.handleError(record)
        finally:
            self.release()

    def close(self):
        """Close the log handler."""
        if self._stream.closed:
            return

        self._cleanup(stream=self._stream, resource=self._resource, lock=self.lock)

    @classmethod
    def _cleanup(
        cls: type["ResourceHandler"], *, stream: IO[str], resource: URI, lock: Optional[Lock] = None
    ):  # pragma: no cover
        """Write the logs to the remote location. This needs to be done in a
        separate classmethod so that it can run as a 'real' finalizer.

        """
        with lock or null_context():
            try:
                stream.flush()
            except:  # pylint: disable=W0702
                pass
            try:
                stream.seek(0)
            except:  # pylint: disable=W0702
                pass

            try:
                if stream.closed:
                    return
                with open_stream(resource, "w", encoding="utf-8") as remote_stream:
                    shutil.copyfileobj(stream, remote_stream)
            except:
                warnings.warn(
                    f"Unable to write to {resource!r}, attempting to print log to stdout",
                    category=LogDataLossWarning,
                )
                shutil.copyfileobj(stream, sys.stdout)
                raise
            finally:
                try:
                    stream.close()
                except:  # pylint: disable=W0702
                    pass
                try:
                    cls._MANAGER.remove_handler(resource)
                except:  # pylint: disable=W0702
                    pass

    def __repr__(self):
        """String representation of resource handler"""
        if not hasattr(self, "_stream"):
            return f"<{type(self).__name__} UNINITIALIZED>"
        level = logging.getLevelName(self.level)
        return f"<{type(self).__name__} {self._resource!r} ({level})>"
