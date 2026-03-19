"""A filesystem implementation built on top of the local filesystem."""

import platform
import shutil
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import IO, Any, NoReturn, Optional
from urllib.parse import unquote

from typing_extensions import Literal

from dve.parser.exceptions import FileAccessError, UnsupportedSchemeError
from dve.parser.file_handling.helpers import parse_uri
from dve.parser.file_handling.implementations.base import BaseFilesystemImplementation
from dve.parser.type_hints import URI, NodeType, PathStr, Scheme

FILE_URI_SCHEMES: set[Scheme] = {"file"}
"""A set of all allowed file URI schemes."""


def file_uri_to_local_path(uri: URI) -> Path:
    """Resolve a `file://` URI to a local filesystem path."""
    scheme, hostname, path = parse_uri(uri)
    if scheme not in FILE_URI_SCHEMES:  # pragma: no cover
        raise UnsupportedSchemeError(
            f"Local filesystem must use an allowed file URI scheme, got {scheme!r}"
        )

    path = unquote(path)
    # Unfortunately Windows is awkward.
    if platform.system() == "Windows":  # pragma: no cover linux
        path = path.lstrip("/")  # '/C:/' -> 'C:/'
        if hostname:  # If this is populated, we have a network path.
            hostname = unquote(hostname)
            return Path(f"//{hostname}/{path}").resolve()

    return Path(path).resolve()


class LocalFilesystemImplementation(BaseFilesystemImplementation):
    """An implementation of a local filesystem."""

    SUPPORTED_SCHEMES = FILE_URI_SCHEMES

    def _uri_to_path(self, uri: URI) -> Path:
        """Turn a file URI into a path. This is intended to make it
        easier to create implementations for 'file-like' protocols.

        """
        return file_uri_to_local_path(uri)

    def _path_to_uri(self, path: Path) -> URI:
        """Turn a path into a file URI. This is intended to make it
        easier to create implementations for 'file-like' protocols.

        """
        return path.as_uri()

    @staticmethod
    def _handle_error(
        err: Exception, resource: URI, mode: str, extra_args: Optional[dict[str, Any]] = None
    ) -> NoReturn:
        """Handle a local file opening error."""
        message = f"Unable to access file at {resource!r} ({mode!r} mode, got {err!r})"
        if extra_args:
            extra = "; ".join(f"{k}: {v!r}" for k, v in extra_args.items())
            message = "".join((message, " ", "[", extra, "]"))

        raise FileAccessError(message) from err

    def get_byte_stream(self, resource: URI) -> IO[bytes]:
        """Gets a stream of bytes to the given resource"""
        try:
            return self._uri_to_path(resource).open("rb")
        except Exception as err:  # pylint: disable=broad-except
            self._handle_error(err, resource, "read")

    def put_byte_stream(self, byte_stream: IO[bytes], resource: URI):
        """Writes a stream of bytes to the given resource"""
        try:
            path = self._uri_to_path(resource)
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)

            with path.open("wb") as file:
                shutil.copyfileobj(byte_stream, file)
        # pylint: disable=broad-except
        except Exception as err:  # pragma: no cover
            self._handle_error(err, resource, "write")

    def get_content_length(self, resource: URI) -> int:
        """Checks the size of the file"""
        try:
            return self._uri_to_path(resource).stat().st_size
        # pylint: disable=broad-except
        except Exception as err:  # pragma: no cover
            self._handle_error(err, resource, "read")

    def get_resource_exists(self, resource: URI) -> bool:
        """Checks if a resource exists at the given uri"""
        try:
            local_path = self._uri_to_path(resource)
            return local_path.exists() and local_path.is_file()
        # pylint: disable=broad-except
        except Exception as err:  # pragma: no cover
            self._handle_error(err, resource, "read")

    def _iter_prefix_path(self, prefix_path: Path, recursive: bool = False) -> Iterator[Path]:
        """Iterate over a prefix as a path."""
        if not prefix_path.exists():
            return
        for child in prefix_path.iterdir():
            yield child
            if child.is_dir() and recursive:
                yield from self._iter_prefix_path(child, recursive)

    def iter_prefix(self, prefix: URI, recursive: bool = False) -> Iterator[tuple[URI, NodeType]]:
        """Iterates over the given prefix yielding any resources or directories"""
        try:
            for child in self._iter_prefix_path(self._uri_to_path(prefix), recursive):
                child_uri = self._path_to_uri(child)

                if not child.is_dir():
                    yield child_uri, "resource"
                else:
                    yield child_uri + "/", "directory"
        # pylint: disable=broad-except
        except Exception as err:  # pragma: no cover
            self._handle_error(err, prefix, "list")

    def remove_resource(self, resource: URI):
        """Removes the given resource"""
        try:
            path = self._uri_to_path(resource)
            if not path.exists():
                return
            path.unlink()
        # pylint: disable=broad-except
        except Exception as err:  # pragma: no cover
            self._handle_error(err, resource, "delete")

    def remove_prefix(self, prefix: URI, recursive: bool = False):
        """Deletes the given prefix"""
        path = self._uri_to_path(prefix)

        if not path.exists():  # pragma: no cover
            return

        try:
            if not path.is_dir():  # pragma: no cover
                path.unlink()
                return
            if recursive:
                shutil.rmtree(str(path))
                return
        # pylint: disable=broad-except
        except Exception as err:  # pragma: no cover
            self._handle_error(err, prefix, "remove prefix")

        super().remove_prefix(prefix, recursive)
        try:
            path.rmdir()
        # pylint: disable=broad-except
        except Exception as err:  # pragma: no cover
            self._handle_error(err, prefix, "remove prefix")

    def _transfer_resource(
        self, source_uri: URI, target_uri: URI, overwrite: bool, action: Literal["move", "copy"]
    ) -> None:
        source_path = self._uri_to_path(source_uri)
        target_path = self._uri_to_path(target_uri)

        if target_path.exists() and not overwrite:
            raise FileAccessError(f"Target URI {target_uri!r} already exists")
        if target_path.is_dir():
            raise FileAccessError(f"Target URI {target_uri!r} is a directory")

        for parent in reversed(target_path.parents):
            if parent.exists():
                if not parent.is_dir():
                    raise FileAccessError(f"Parent of target URI {target_uri!r} is not a directory")
            else:
                parent.mkdir(exist_ok=True)

        if action == "copy":
            func: Callable[[PathStr, PathStr], None] = shutil.copy
        elif action == "move":
            func = shutil.move
        else:  # pragma: no cover
            raise ValueError(f"Unsupported action {action!r}, expected one of: 'copy', 'move'")
        try:
            func(str(source_path), str(target_path))
        except Exception as err:  # pylint:  disable=broad-except
            self._handle_error(err, source_uri, action, {"Target URI": target_uri})

    def copy_resource(self, source_uri: URI, target_uri: URI, overwrite: bool = False) -> None:
        """Copies the resource from the source uri to the target uri"""
        return self._transfer_resource(source_uri, target_uri, overwrite, "copy")

    def move_resource(self, source_uri: URI, target_uri: URI, overwrite: bool = False) -> None:
        """Moves the resource from the source uri to the target uri"""
        return self._transfer_resource(source_uri, target_uri, overwrite, "move")

    def build_relative_uri(self, base_uri: URI, relative_location: str) -> URI:
        """Build a uri based on a base uri and a relative location"""
        base_path = file_uri_to_local_path(base_uri)
        return Path(base_path, relative_location).resolve().as_uri()
