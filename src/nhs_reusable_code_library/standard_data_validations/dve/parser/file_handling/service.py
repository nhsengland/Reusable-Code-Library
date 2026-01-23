"""Service layer for working with files. This basically boils down to
convenience functions which dispatch to concrete file handling
implementations based on the URI scheme.

"""

# pylint: disable=logging-not-lazy
import hashlib
import platform
import shutil
import uuid
import warnings
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import IO, Optional, overload
from urllib.parse import unquote, urlparse

from typing_extensions import Literal

from dve.parser.exceptions import FileAccessError, UnsupportedSchemeError
from dve.parser.file_handling.helpers import NonClosingTextIOWrapper
from dve.parser.file_handling.implementations import (
    BaseFilesystemImplementation,
    DBFSFilesystemImplementation,
    LocalFilesystemImplementation,
    S3FilesystemImplementation,
)
from dve.parser.file_handling.implementations.file import file_uri_to_local_path
from dve.parser.type_hints import (
    URI,
    BinaryFileOpenMode,
    Extension,
    Filename,
    FileOpenMode,
    Location,
    NodeType,
    Scheme,
    TextFileOpenMode,
    URIPath,
)

_IMPLEMENTATIONS: list[BaseFilesystemImplementation] = [
    S3FilesystemImplementation(),
    LocalFilesystemImplementation(),
]
"""Supported filesystem implementations."""
try:
    # Will fail on Windows if env var unset.
    _IMPLEMENTATIONS.append(DBFSFilesystemImplementation())
except ValueError:
    pass

_SUPPORTED_SCHEMES: set[Scheme] = set().union(
    *[impl.SUPPORTED_SCHEMES for impl in _IMPLEMENTATIONS]
)
"""Supported URI schemes."""

ALL_FILE_MODES: set[FileOpenMode] = {"r", "a", "a+", "w", "ab", "rb", "wb", "ba", "br", "bw"}
"""All supported file modes."""
TEXT_MODES: set[TextFileOpenMode] = {"r", "a", "w"}
"""Text file modes."""
APPEND_MODES: set[FileOpenMode] = {"a", "ab", "ba"}
"""Modes that append to the resource."""
READ_ONLY_MODES: set[FileOpenMode] = {"r", "br", "rb"}
"""Modes that only read the file and do not write to it."""

ONE_MEBIBYTE = 1024**3
"""The size of a binary megabyte in bytes."""


def add_implementation(implementation: BaseFilesystemImplementation):  # pragma: no cover
    """Add a new filesystem implementation."""
    if not isinstance(implementation, BaseFilesystemImplementation):
        raise TypeError("Implementation must be `BaseFilesystemImplementation` subclass")
    _IMPLEMENTATIONS.append(implementation)
    _SUPPORTED_SCHEMES.update(implementation.SUPPORTED_SCHEMES)


def _get_implementation(uri: URI) -> BaseFilesystemImplementation:
    """Get the filesystem implementation needed for a given URI."""
    scheme = urlparse(uri).scheme or "file"
    for implementation in _IMPLEMENTATIONS:
        if scheme in implementation.SUPPORTED_SCHEMES:
            return implementation

    raise UnsupportedSchemeError(
        f"No implementations with support for scheme {scheme!r}"
    )  # pragma: no cover


def scheme_is_supported(scheme: Scheme) -> bool:
    """Return whether a given scheme is actually supported."""
    return scheme in _SUPPORTED_SCHEMES


def is_supported(uri: URI) -> bool:  # pragma: no cover
    """Return whether a given URI is actually supported."""
    try:
        _get_implementation(uri)
    except UnsupportedSchemeError:
        return False
    return True


@overload
def open_stream(
    resource: URI,
    mode: TextFileOpenMode = "r",
    encoding: Optional[str] = None,
    ensure_seekable: bool = False,
) -> AbstractContextManager[IO[str]]:
    pass  # pragma: no cover


@overload
def open_stream(
    resource: URI,
    mode: BinaryFileOpenMode,
    encoding: None = None,
    ensure_seekable: bool = False,
) -> AbstractContextManager[IO[bytes]]:
    pass  # pragma: no cover


@contextmanager
def open_stream(
    resource: URI,
    mode: FileOpenMode = "r",
    encoding: Optional[str] = None,
    ensure_seekable: bool = False,
) -> Iterator[IO]:
    """Open the resource as a Python file stream, returning a context manager
    over the stream.

    If `ensure_seekable` is `True`, ensure that streams for reading are
    able to be seeked. This will ensure file contents are transferred
    to a local temporary file, where appropriate.

    """
    implementation = _get_implementation(resource)

    if mode not in ALL_FILE_MODES:
        raise FileAccessError(f"Unsupported file mode {mode!r}, expected one of {ALL_FILE_MODES!r}")

    # Previously used TemporaryFile directly, but this enables Python
    # to handle `a` mode the way it's supposed to work.
    with TemporaryDirectory() as temp_dir:
        temp_file_path = Path(temp_dir, uuid.uuid4().hex)
        if mode in APPEND_MODES or mode in READ_ONLY_MODES:
            # Should raise an error if we try to 'r' or 'rb' a file that doesn't exist,
            # but if we're in append mode that's fine.
            # Can't just try to read because we can't distinguish between permission
            # errors and files that don't exist, so we need to use LBYL.
            if get_resource_exists(resource):
                byte_stream: Optional[IO[bytes]] = implementation.get_byte_stream(resource)
            elif mode in APPEND_MODES:
                byte_stream = None
            else:
                raise FileAccessError(f"Unable to access file at {resource!r}")

            if byte_stream:  # Will always be true if mode is read, _may_ be true for append.
                with byte_stream:
                    # Handle read-only modes without copy if the stream is already seekable or
                    # if we don't need it to be seekable.
                    if mode in READ_ONLY_MODES and (not ensure_seekable or byte_stream.seekable()):
                        if mode in TEXT_MODES:
                            with NonClosingTextIOWrapper(byte_stream, encoding) as text_stream:
                                yield text_stream
                        else:
                            yield byte_stream
                        return

                    # For append modes, or if we require a seekable stream and the stream is _not_
                    # seekable, use a temp file.
                    with open(temp_file_path, "wb") as temp_file:
                        shutil.copyfileobj(byte_stream, temp_file)

        with open(temp_file_path, mode, encoding=encoding) as temp_file:
            yield temp_file

        if mode not in READ_ONLY_MODES:
            with open(temp_file_path, "rb") as temp_file:
                implementation.put_byte_stream(temp_file, resource)

        temp_file_path.unlink()

    return


def get_content_length(resource: URI) -> int:
    """Get the size of the resource, in bytes."""
    return _get_implementation(resource).get_content_length(resource)


def get_resource_exists(resource: URI) -> bool:
    """Check if the resource at the provided URI exists."""
    return _get_implementation(resource).get_resource_exists(resource)


def get_resource_digest(resource: URI, algorithm: str = "md5") -> str:
    """Get the digest (AKA checksum) of the provided resource using the specified
    hashing algorithm.

    """
    hash_func = hashlib.new(algorithm)
    with open_stream(resource, "rb") as byte_stream:
        while True:
            block = byte_stream.read(ONE_MEBIBYTE)
            if not block:
                break
            hash_func.update(block)

    return hash_func.hexdigest()


def remove_resource(resource: URI):
    """Delete the resource at the given location."""
    return _get_implementation(resource).remove_resource(resource)


def iter_prefix(prefix: URI, recursive: bool = False) -> Iterator[tuple[URI, NodeType]]:
    """List the contents of a given prefix."""
    return _get_implementation(prefix).iter_prefix(prefix, recursive)


def remove_prefix(prefix: URI, recursive: bool = False):
    """Remove a given prefix. If removal is not recursive, raise an error if the
    prefix contains directory nodes.

    """
    return _get_implementation(prefix).remove_prefix(prefix, recursive)


def build_relative_uri(base_uri: URI, relative_location: str) -> URI:
    """Build a uri based on a base uri and a relative location"""
    return _get_implementation(base_uri).build_relative_uri(base_uri, relative_location)


def _transfer_resource(
    source_uri: URI, target_uri: URI, overwrite: bool, action: Literal["copy", "move"]
) -> None:
    """Transfer a resource from one location to another. If `action` is `'move'`,
    delete the source resource.

    """
    if action not in ("move", "copy"):  # pragma: no cover
        raise ValueError(f"Unsupported action {action!r}, expected one of: 'copy', 'move'")

    source_impl = _get_implementation(source_uri)
    target_impl = _get_implementation(target_uri)

    if source_impl is target_impl:
        if action == "move":
            source_impl.move_resource(source_uri, target_uri, overwrite)
        else:
            source_impl.copy_resource(source_uri, target_uri, overwrite)
        return

    target_exists = target_impl.get_resource_exists(target_uri)
    if target_exists and not overwrite:
        raise FileAccessError(f"Target URI {target_uri!r} already exists")

    source_stream = source_impl.get_byte_stream(source_uri)
    target_impl.put_byte_stream(source_stream, target_uri)
    if action == "move":
        source_impl.remove_resource(source_uri)


def copy_resource(source_uri: URI, target_uri: URI, overwrite: bool = False) -> None:
    """Copy a resource from one location to another."""
    _transfer_resource(source_uri, target_uri, overwrite, "copy")


def move_resource(source_uri: URI, target_uri: URI, overwrite: bool = False) -> None:
    """Move a resource from one location to another."""
    _transfer_resource(source_uri, target_uri, overwrite, "move")


def create_directory(target_uri: URI):
    """Create a directory locally."""
    if not isinstance(_get_implementation(target_uri), LocalFilesystemImplementation):
        return
    Path(urlparse(target_uri).path).mkdir(parents=True, exist_ok=True)
    return


def _transfer_prefix(
    source_prefix: URI, target_prefix: URI, overwrite: bool, action: Literal["copy", "move"]
):
    """Transfer a prefix from one location to another. If `action` is `'move'`,
    delete the source resources.

    """
    if action not in ("move", "copy"):  # pragma: no cover
        raise ValueError(f"Unsupported action {action!r}, expected one of: 'copy', 'move'")

    if not source_prefix.endswith("/"):
        source_prefix += "/"
    if not target_prefix.endswith("/"):
        target_prefix += "/"

    source_uris: list[URI] = []
    target_uris: list[URI] = []

    source_impl = _get_implementation(source_prefix)
    target_impl = _get_implementation(target_prefix)

    for source_uri, node_type in source_impl.iter_prefix(source_prefix, True):
        if node_type != "resource":
            continue

        if not source_uri.startswith(source_prefix):  # pragma: no cover
            raise FileAccessError(
                f"Listed URI ({source_uri!r}) not relative to source prefix "
                + f"({source_prefix!r})"
            )

        path_within_prefix = source_uri[len(source_prefix) :]
        target_uri = target_prefix + path_within_prefix

        source_uris.append(source_uri)
        target_uris.append(target_uri)

    if not overwrite:
        would_overwrite = [uri for uri in target_uris if target_impl.get_resource_exists(uri)]
        if would_overwrite:
            raise FileAccessError(f"Some target URIs already exist: {would_overwrite!r}")

    if source_impl is target_impl:
        if action == "move":
            func = source_impl.move_resource
        else:
            func = source_impl.copy_resource
    else:
        if action == "move":
            func = move_resource
        else:
            func = copy_resource

    for source_uri, target_uri in zip(source_uris, target_uris):
        func(source_uri, target_uri, overwrite)

    if action == "move":  # Slightly unnecessary, but cleans up dirs locally.
        source_impl.remove_prefix(source_prefix, True)


def copy_prefix(source_prefix: URI, target_prefix: URI, overwrite: bool = False):
    """Copy a prefix from one location to another."""
    _transfer_prefix(source_prefix, target_prefix, overwrite, "copy")


def move_prefix(source_prefix: URI, target_prefix: URI, overwrite: bool = False):
    """Move a prefix from one location to another."""
    _transfer_prefix(source_prefix, target_prefix, overwrite, "move")


def resolve_location(filename_or_url: Location) -> URI:
    """Resolve a union of filename and URI to a URI."""
    if isinstance(filename_or_url, Path):
        return filename_or_url.expanduser().resolve().as_uri()

    parsed_url = urlparse(filename_or_url)
    if parsed_url.scheme == "file":  # Passed a URL as a file.
        return file_uri_to_local_path(filename_or_url).as_uri()

    if platform.system() != "Windows":
        # On Linux, a filesystem path will never present with a scheme.
        if not parsed_url.scheme:
            return resolve_location(Path(filename_or_url))
    else:  # pragma: no cover linux
        # On Windows, a filesystem path _might_ present with a scheme if it's e.g.
        # a Windows path with forward slashes (e.g. 'C:/path/to/file'). This is
        # unfortunately indistinguishable from a URI.

        # Path uses backslashes at the start of the 'URL path' or is a Windows network
        # path beginning with two backslashes.
        if parsed_url.path.startswith("\\") or filename_or_url.startswith("\\\\"):
            return resolve_location(Path(filename_or_url))

        if len(parsed_url.scheme) == 1:  # Could be URL or path.
            warnings.warn(
                "Scheme contains only a single letter, but path does not start with a "
                + "backslash. Assuming URL, but could be a Windows file path"
            )

    if parsed_url.scheme not in _SUPPORTED_SCHEMES:  # pragma: no cover
        raise UnsupportedSchemeError(
            f"Unsupported scheme {parsed_url.scheme!r}, expected one of {_SUPPORTED_SCHEMES!r}"
        )
    return filename_or_url


def get_file_name(uri: URI) -> Filename:
    """Get the file name from a URI."""
    return unquote(uri.rstrip("/").rsplit("/", 1)[-1])


def get_file_stem(uri: URI) -> Filename:
    """Get the file stem from a URI."""
    return get_file_name(uri).rsplit(".", 1)[0]


def get_file_suffix(uri: URI) -> Optional[Extension]:
    """Get the top level file extension from a URI."""
    try:
        _, extension = get_file_name(uri).rsplit(".", 1)
        return extension
    except ValueError:
        return None


def joinuri(base_uri: URI, *path_components: URIPath):
    """Like `pathlib.Path.joinpath` for URI components.

    If the base URI does not already have a trailing slash, one will be added.
    Components will be joined with slashes, whether they have leading/trailing
    slashes or not (as this is valid for some URI schemes).

    """
    if base_uri.endswith("/"):
        base_uri = base_uri[:-1]
    return "/".join([base_uri, *path_components])


def get_parent(uri: URI) -> URI:
    """Get a parent location given a URI"""
    return unquote(uri).rstrip("/").rsplit("/", 1)[0]
