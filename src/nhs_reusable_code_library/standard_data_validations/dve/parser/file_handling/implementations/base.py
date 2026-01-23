"""An abstract implementation of the filesystem layer."""

from abc import ABCMeta, abstractmethod
from collections.abc import Iterable, Iterator
from typing import IO

from dve.parser.exceptions import FileAccessError
from dve.parser.type_hints import URI, NodeType, Scheme


class BaseFilesystemImplementation(metaclass=ABCMeta):
    """An abstract implementation of a filesystem and associated operations."""

    @property
    @abstractmethod
    def SUPPORTED_SCHEMES(self) -> set[Scheme]:  # pylint: disable=invalid-name
        """Schemes supported by the filesystem implementation."""

    @abstractmethod
    def get_byte_stream(self, resource: URI) -> IO[bytes]:
        """Get a context manager to read the resource as bytes."""

    @abstractmethod
    def put_byte_stream(self, byte_stream: IO[bytes], resource: URI):
        """Write the provided byte stream to the resource."""

    @abstractmethod
    def get_content_length(self, resource: URI) -> int:
        """Get the size of the resource, in bytes."""

    @abstractmethod
    def get_resource_exists(self, resource: URI) -> bool:
        """Check if the resource at the provided URI exists. This should return `False`
        for directories.

        """

    @abstractmethod
    def iter_prefix(self, prefix: URI, recursive: bool = False) -> Iterator[tuple[URI, NodeType]]:
        """List the contents of a given prefix. Directory URIs should be returned with a
        trailing /.

        """

    @abstractmethod
    def remove_resource(self, resource: URI):
        """Delete the resource at the given location."""

    def build_relative_uri(self, base_uri: URI, relative_location: str) -> URI:
        """Build a uri based on a base uri and a relative location"""
        raise NotImplementedError()

    def remove_prefix(self, prefix: URI, recursive: bool = False):
        """Remove a given prefix. If removal is not recursive, raise an error if the
        prefix contains directory nodes.

        """
        nodes = self.iter_prefix(prefix, recursive=recursive)

        resources: Iterable[URI]
        if recursive:
            resources = (resource for resource, node_type in nodes if node_type == "resource")
        else:
            resources = []
            for resource, node_type in nodes:
                if node_type == "directory":
                    raise FileAccessError(
                        f"Unable to remove {prefix!r}, prefix contains directory nodes and "
                        + "removal is not recursive"
                    )

                resources.append(resource)

        for resource in resources:
            self.remove_resource(resource)

    def copy_resource(self, source_uri: URI, target_uri: URI, overwrite: bool = False) -> None:
        """Copy a resource from one location to another.

        Both URIs must be supported by the same implementation.

        """
        target_exists = self.get_resource_exists(target_uri)
        if target_exists and not overwrite:
            raise FileAccessError(f"Target URI {target_uri!r} already exists")

        source_stream = self.get_byte_stream(source_uri)
        self.put_byte_stream(source_stream, target_uri)

    def move_resource(self, source_uri: URI, target_uri: URI, overwrite: bool = False):
        """Move a resource from one location to another.

        Both URIs must be supported by the same implementation.

        """
        self.copy_resource(source_uri, target_uri, overwrite)
        self.remove_resource(source_uri)
