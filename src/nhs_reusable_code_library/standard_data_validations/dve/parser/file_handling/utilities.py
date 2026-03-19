"""Utilities for working with files."""

import tempfile
from pathlib import Path
from types import TracebackType
from typing import Optional

from dve.parser.exceptions import UnsupportedSchemeError
from dve.parser.file_handling.service import is_supported, remove_prefix
from dve.parser.type_hints import URI


class TemporaryPrefix:
    """Like 'TemporaryDirectory', but with support for a URL prefix."""

    def __init__(self, prefix: Optional[URI] = None):
        """Set up the prefix.

        Args:
         - `prefix`: the URL prefix to use as temporary storage. This
           will default to a local temporary folder.

        """
        if not prefix:
            prefix = Path(tempfile.mkdtemp()).as_uri()
        self._prefix = prefix.rstrip("/") + "/"

        # Ensure we have an implementation for this prefix.
        if not is_supported(self._prefix):  # pragma: no cover
            raise UnsupportedSchemeError(f"No supported implementation for {prefix!r}")
        self._in_context = False

    @property
    def prefix(self) -> URI:  # pragma: no cover
        """The URI prefix of the temporary directory."""
        if not self._in_context:
            raise ValueError(f"`{self.__class__.__name__}` must be used as context manager")
        return self._prefix

    def __enter__(self) -> URI:
        """Enters the context manager and yields the prefix"""
        self._in_context = True
        return self._prefix

    def __exit__(
        self,
        exc_type: Optional[type[Exception]],
        exc_value: Optional[Exception],
        traceback: Optional[TracebackType],
    ):
        """Exits the context manager and cleans up the temporary prefix"""
        self._in_context = False
        remove_prefix(self._prefix, recursive=True)
