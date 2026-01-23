"""Type hints for the parser."""

from pathlib import Path
from typing import Any, Optional, Union

from typing_extensions import Literal

PathStr = str
"""A filesystem path, as a string (cursed)."""
URI = str
"""A URI representing a remote or local resource."""
Filename = str
"""A string representing a filename."""
Scheme = str
"""The scheme attribute of the URI."""
Hostname = Optional[str]
"""The hostname attribute of the URI."""
URIPath = str
"""The path attribute of the URI."""
Extension = str
"""A file extension (e.g. '.csv')."""
TextFileOpenMode = Literal["r", "a", "w", "a+"]
"""An opening mode for a file in text mode."""
BinaryFileOpenMode = Literal["ab", "rb", "wb", "ba", "br", "bw"]
"""An opening mode for a file in binary mode."""
FileOpenMode = Union[TextFileOpenMode, BinaryFileOpenMode]
"""An opening mode for a file."""
NodeType = Literal["resource", "directory"]
"""The type of node in a filesystem."""

Location = Union[PathStr, Path, URI]
"""
A filesystem or remote location. An annoying, difficult to resolve union
(see `parser.file_handling.service.resolve_location`).

"""

ReaderName = str
"""A parser name. This must be importable from `parser.readers`"""
ReaderArgs = Optional[dict[str, Any]]
"""Keyword arguments to be passed to the parser's constructor."""
FieldName = str
"""The name of a field within the dataset."""

SparkXMLMode = Literal["PERMISSIVE", "FAILFAST", "DROPMALFORMED"]
"""The mode to use when parsing XML files with Spark."""
