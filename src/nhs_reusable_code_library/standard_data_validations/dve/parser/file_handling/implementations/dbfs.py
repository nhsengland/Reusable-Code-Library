"""A filesystem implementation built on top of DBFS.

This is a thin wrapper around the `file` implementation.

"""

import os
import platform
from pathlib import Path
from typing import Optional
from urllib.parse import quote
from uuid import uuid4

from dve.parser.exceptions import UnsupportedSchemeError
from dve.parser.file_handling.helpers import parse_uri
from dve.parser.file_handling.implementations.file import LocalFilesystemImplementation
from dve.parser.type_hints import URI, Scheme


class DBFSFilesystemImplementation(LocalFilesystemImplementation):
    """An implementation of DBFS using the local filesystem."""

    DBFS_SCHEME: Scheme = "dbfs"
    """The scheme for DBFS URIs."""
    SUPPORTED_SCHEMES = {DBFS_SCHEME}

    @staticmethod
    def _get_default_dbfs_path():  # pragma: no cover
        """Get the default DBFS path."""
        dbfs_root_string = os.getenv("DBFS_ROOT")
        if dbfs_root_string:
            dbfs_path = Path(dbfs_root_string)
        elif platform.system() != "Windows":
            dbfs_path = Path("/dbfs")
        else:
            raise ValueError("Cannot use DBFS on Windows without setting `DBFS_ROOT` env var.")

        return dbfs_path

    @staticmethod
    def _ensure_dbfs_root(dbfs_root: Path):  # pragma: no cover
        """Ensure that the DBFS path exists and is writeable."""
        # Poor man's pydantic validator...
        try:
            if not dbfs_root.is_dir():
                dbfs_root.mkdir()
        except OSError as err:
            raise ValueError(f"{dbfs_root} does not exist and cannot be created") from err

        temp_path = dbfs_root.joinpath(uuid4().hex)
        try:
            temp_path.write_bytes(b"")
        except OSError as err:
            raise ValueError(f"{dbfs_root} is not writable") from err
        temp_path.unlink()
        return dbfs_root

    def __init__(self, dbfs_root_override: Optional[Path] = None) -> None:
        dbfs_root = dbfs_root_override or self._get_default_dbfs_path()
        self._dbfs_root: Path = self._ensure_dbfs_root(dbfs_root)
        super().__init__()

    @property
    def dbfs_root(self) -> Path:
        """The DBFS root as a concrete filesystem path."""
        return self._dbfs_root

    def _uri_to_path(self, uri: URI) -> Path:
        scheme, hostname, path = parse_uri(uri)
        if scheme not in self.SUPPORTED_SCHEMES:  # pragma: no cover
            raise UnsupportedSchemeError(
                f"DBFS filesystem must use an allowed file URI scheme, got {scheme!r}"
            )
        if hostname:  # pragma: no cover
            raise ValueError("DBFS filesystem implementation does not support URIs with hostnames")
        return self.dbfs_root.joinpath(path.lstrip("/"))

    def _path_to_uri(self, path: Path) -> URI:
        relpath_posix = path.relative_to(self.dbfs_root).as_posix()
        path_component = quote(relpath_posix.lstrip("/"))
        return f"{self.DBFS_SCHEME}:/{path_component}"
