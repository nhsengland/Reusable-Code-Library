"""File handling implementations which use AWS S3."""

# pylint: disable=broad-except
import os
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from math import ceil
from threading import Lock
from typing import IO, TYPE_CHECKING, Any, NoReturn, Optional
from urllib.parse import quote, unquote

import boto3
from botocore.exceptions import ClientError

from dve.parser.exceptions import FileAccessError, UnsupportedSchemeError
from dve.parser.file_handling.helpers import parse_uri
from dve.parser.file_handling.implementations.base import BaseFilesystemImplementation
from dve.parser.type_hints import URI, NodeType, Scheme

if TYPE_CHECKING:
    from mypy_boto3_s3.type_defs import CompletedPartTypeDef

Bucket = str
"""An S3 bucket."""
Key = str
"""A key within an S3 bucket."""

PartNumber = int
"""A part number for the multipart upload API."""
ByteRange = str
"""A byte range for the multipart upload API."""

ENDPOINT_URL = os.environ.get("S3_HOST")
"""The S3 url to use"""

FIVE_MEBIBYTES = 5 * (1024**2)
"""A constant indicating five mebibytes in bytes."""
FIVE_GIBIBYTES = FIVE_MEBIBYTES * 1024
"""A constant indicating five gibibytes in bytes."""
MULTIPART_CHUNK_SIZE = 8 * (1024**2)
"""The default multipart chunk size in bytes."""


class SessionPool:
    """A pool of boto3 sessions to use across threads"""

    def __init__(self):
        self._lock: Lock = Lock()
        self._pool: list[boto3.Session] = []

    def pop(self) -> boto3.Session:
        """Take a session from the pool. If no sessions exist,
        create one.
        """
        with self._lock:
            if len(self._pool) == 0:
                return boto3.Session()
            return self._pool.pop()

    def put(self, session: boto3.Session):
        """Return a session to the pool"""
        with self._lock:
            self._pool.append(session)


@contextmanager
def get_session(session_pool: SessionPool):
    """Helper to manage session pool access."""
    try:
        session = session_pool.pop()
        yield session
    finally:
        session_pool.put(session)


_session_pool: SessionPool = SessionPool()


class S3FilesystemImplementation(BaseFilesystemImplementation):
    """An implementation of an S3 filesystem."""

    SUPPORTED_SCHEMES = {"s3", "s3a"}

    def _handle_boto_error(
        self,
        err: Exception,
        resource: URI,
        access_type: str,
        extra_args: Optional[dict[str, Any]] = None,
    ) -> NoReturn:
        """Handle an error from boto3."""
        if not isinstance(err, ClientError):
            extra = f"unexpected error: {err!r}"  # pragma: no cover
        else:
            error_code = err.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "NoSuchBucket", "AccessDenied"):
                extra = repr(error_code)
            else:
                extra = f"unexpected error: {error_code!r}"

        message = f"Unable to access file at {resource!r} (attempted {access_type}, got {extra})"

        if extra_args:
            extra = "; ".join(f"{k}: {v!r}" for k, v in extra_args.items())
            message = "".join((message, " ", "[", extra, "]"))

        raise FileAccessError(message) from err

    def _parse_s3_uri(self, uri: URI) -> tuple[Scheme, Bucket, Key]:
        """Parse an S3 URI to a bucket and key"""
        scheme, bucket, key = parse_uri(uri)
        if scheme not in self.SUPPORTED_SCHEMES:  # pragma: no cover
            raise UnsupportedSchemeError(f"Scheme {scheme!r} not supported for this implementation")

        if not bucket:  # pragma: no cover
            raise FileAccessError("Missing hostname (bucket) for S3 URI")
        bucket, key = unquote(bucket), unquote(key)
        key = os.path.normpath(key)
        if key == ".":
            key = "/"
        if key.startswith("/"):
            key = key[1:]

        return scheme, bucket, key

    def _build_s3_uri(self, *, scheme: Scheme = "s3", bucket: Bucket, key: Key) -> URI:
        """Build an S3 URI from the bucket and key (and optionally the scheme)."""
        bucket, key = quote(bucket), quote(key)
        return f"{scheme}://{bucket}/{key}"

    def get_byte_stream(self, resource: URI) -> IO[bytes]:
        """Gets the resource as a stream of bytes"""
        with get_session(_session_pool) as session:
            try:
                _, bucket, key = self._parse_s3_uri(resource)
                client = session.client("s3", endpoint_url=ENDPOINT_URL)
                obj = client.get_object(Bucket=bucket, Key=key)
                body: IO[bytes] = obj["Body"]  # type: ignore
            except Exception as err:
                self._handle_boto_error(err, resource, "read")
        return body

    def put_byte_stream(self, byte_stream: IO[bytes], resource: URI):
        """Writes a byte stream"""
        with get_session(_session_pool) as session:
            try:
                _, bucket, key = self._parse_s3_uri(resource)
                client = session.client("s3", endpoint_url=ENDPOINT_URL)
                client.upload_fileobj(byte_stream, bucket, key)
            except Exception as err:  # pragma: no cover
                self._handle_boto_error(err, resource, "write")

    def get_content_length(self, resource: URI) -> int:
        """Returns the size of the given resource"""
        with get_session(_session_pool) as session:
            try:
                _, bucket, key = self._parse_s3_uri(resource)
                s3_resource = session.resource("s3", endpoint_url=ENDPOINT_URL)
                obj = s3_resource.Object(bucket, key)
                content_length = obj.content_length
            except Exception as err:  # pragma: no cover
                self._handle_boto_error(err, resource, "read")

        return content_length

    def get_resource_exists(self, resource: URI) -> bool:
        """Checks that a given resource exists"""
        with get_session(_session_pool) as session:
            try:
                _, bucket, key = self._parse_s3_uri(resource)
                if not key:
                    return False
                s3_resource = session.resource("s3", endpoint_url=ENDPOINT_URL)
                try:
                    s3_resource.Object(bucket, key).load()
                except ClientError as err:  # pragma: no cover
                    if err.response["Error"]["Code"] in ("403", "404"):
                        return False
                    raise
                finally:
                    _session_pool.put(session)
            except Exception as err:  # pragma: no cover
                self._handle_boto_error(err, resource, "read")

        return True

    def iter_prefix(self, prefix: URI, recursive: bool = False) -> Iterator[tuple[URI, NodeType]]:
        """Iterates over the given prefix"""
        with get_session(_session_pool) as session:
            try:
                scheme, bucket, key = self._parse_s3_uri(prefix)
                client = session.client("s3", endpoint_url=ENDPOINT_URL)

                prefix_keys: deque[Key] = deque([key])
                while prefix_keys:
                    next_key = prefix_keys.popleft().rstrip("/") + "/"

                    paginate_args = {
                        "Bucket": bucket,
                        "Delimiter": "/",
                    }
                    if next_key != "/":
                        paginate_args["Prefix"] = next_key

                    paginator = client.get_paginator("list_objects_v2")
                    for result in paginator.paginate(**paginate_args):  # type: ignore
                        for dir_node in result.get("CommonPrefixes", []):
                            dir_node_key = dir_node["Prefix"]
                            if recursive:
                                prefix_keys.append(dir_node_key)
                            uri = self._build_s3_uri(scheme=scheme, bucket=bucket, key=dir_node_key)
                            yield uri, "directory"
                        for res_node in result.get("Contents", []):
                            uri = self._build_s3_uri(
                                scheme=scheme, bucket=bucket, key=res_node["Key"]
                            )

                            # Check for weird directory listing objects that 'exist' but can't
                            # have HEAD requests. THese also appear in `CommonPrefixes`.
                            if res_node["Size"] == 0 and not self.get_resource_exists(uri):
                                continue

                            yield uri, "resource"

            except Exception as err:  # pragma: no cover
                self._handle_boto_error(err, prefix, "list")

    def remove_resource(self, resource: URI):
        """Deletes the given resource"""
        with get_session(_session_pool) as session:
            try:
                _, bucket, key = self._parse_s3_uri(resource)
                s3_resource = session.resource("s3", endpoint_url=ENDPOINT_URL)
                obj = s3_resource.Object(bucket, key)
                try:
                    obj.delete()
                except ClientError as err:  # pragma: no cover
                    if err.response["Error"]["Code"] not in ("403", "404"):
                        raise
            except Exception as err:  # pragma: no cover
                self._handle_boto_error(err, resource, "delete")

    @staticmethod
    def _calculate_file_chunks(
        file_size_bytes: int, chunk_size: int = MULTIPART_CHUNK_SIZE
    ) -> Iterator[tuple[PartNumber, ByteRange]]:
        """Calculate the part numbers and byte ranges for a multipart upload's chunks."""
        if chunk_size < FIVE_MEBIBYTES:
            raise ValueError("Chunk size must be at least five mebibytes")

        # Grow the chunk size if it would result in more than 9999 chunks.
        if ceil(file_size_bytes / chunk_size) <= 9999:
            full_chunk_size = chunk_size
        else:
            full_chunk_size = file_size_bytes // 9999

        file_end = file_size_bytes - 1  # The maximum byte offset for the file.
        range_start = 0
        part_number = 1
        while range_start < file_size_bytes:
            range_end = range_start + full_chunk_size - 1  # Inclusive range, take off final byte.
            if range_end >= (file_end - 1024):  # Don't send a final chunk less than a kilobyte.
                range_end = file_end

            yield (part_number, f"bytes={range_start}-{range_end}")

            part_number += 1
            range_start = range_end + 1

    def copy_resource(
        self,
        source_uri: URI,
        target_uri: URI,
        overwrite: bool = False,
        _force_multipart: bool = False,
        _multipart_chunk_size: int = MULTIPART_CHUNK_SIZE,
    ) -> None:
        """Copies the resource from the source_uri to the target_uri"""
        source_exists = self.get_resource_exists(source_uri)
        if not source_exists:
            raise FileAccessError(f"Source URI {source_uri!r} does not exist")

        if not overwrite and self.get_resource_exists(target_uri):
            raise FileAccessError(f"Target URI {target_uri!r} already exists")

        _, source_bucket, source_key = self._parse_s3_uri(source_uri)
        _, target_bucket, target_key = self._parse_s3_uri(target_uri)

        with get_session(_session_pool) as session:
            s3_client = session.client("s3", endpoint_url=ENDPOINT_URL)
            try:
                source_size = self.get_content_length(source_uri)

                if source_size <= FIVE_GIBIBYTES and not _force_multipart:
                    s3_client.copy_object(
                        CopySource={"Bucket": source_bucket, "Key": source_key},
                        Bucket=target_bucket,
                        Key=target_key,
                    )
                    return

                cmu_response = s3_client.create_multipart_upload(
                    Bucket=target_bucket, Key=target_key
                )
                upload_id = cmu_response["UploadId"]
                try:
                    parts: list["CompletedPartTypeDef"] = []
                    if source_size <= FIVE_MEBIBYTES:
                        # Can't use UploadRange on <= 5MiB file
                        upc_response = s3_client.upload_part_copy(
                            CopySource={"Bucket": source_bucket, "Key": source_key},
                            Bucket=target_bucket,
                            Key=target_key,
                            UploadId=upload_id,
                            PartNumber=1,
                        )
                        parts.append(
                            {"ETag": upc_response["CopyPartResult"]["ETag"], "PartNumber": 1}
                        )
                    else:
                        chunk_ranges = self._calculate_file_chunks(
                            source_size, _multipart_chunk_size
                        )
                        for part_number, chunk_range in chunk_ranges:
                            upc_response = s3_client.upload_part_copy(
                                CopySource={"Bucket": source_bucket, "Key": source_key},
                                CopySourceRange=chunk_range,
                                Bucket=target_bucket,
                                Key=target_key,
                                UploadId=upload_id,
                                PartNumber=part_number,
                            )
                            parts.append(
                                {
                                    "ETag": upc_response["CopyPartResult"]["ETag"],
                                    "PartNumber": part_number,
                                }
                            )
                except Exception:
                    s3_client.abort_multipart_upload(
                        Bucket=target_bucket, Key=target_key, UploadId=upload_id
                    )
                    raise

                s3_client.complete_multipart_upload(
                    Bucket=target_bucket,
                    Key=target_key,
                    MultipartUpload={"Parts": parts},
                    UploadId=upload_id,
                )

                return
            except Exception as err:  # pragma: no cover
                self._handle_boto_error(
                    err, target_uri, "copy", extra_args={"Target URI": target_uri}
                )

    def build_relative_uri(self, base_uri: URI, relative_location: str) -> URI:
        """Build a uri based on a base uri and a relative location"""
        scheme, bucket, key = self._parse_s3_uri(base_uri)
        key_parts = key.split("/")
        rel_parts = relative_location.split("/")
        for prt in rel_parts:
            if prt == ".":
                continue
            if prt == "..":
                key_parts.pop()
                continue
            key_parts.append(prt)
        return self._build_s3_uri(scheme=scheme, bucket=bucket, key="/".join(key_parts))
