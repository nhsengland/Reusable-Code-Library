from datetime import datetime
from typing import Dict, Iterable, List, Union

import boto3
from src.nhs_reusable_code_library.resuable_codes.shared.aws import s3_client
import os

S3_PROTOCOL = "s3://"


class File:
    absolute_path: str
    relative_path: str
    last_modified: datetime
    version: str
    key: str
    bucket: str

    # Note that obj_metadata may be a listed object or head object, so the keys used
    # should only be those from the intersection of these objects.
    def __init__(self, bucket: str, user: str, key: str, obj_metadata: Dict):
        self.absolute_path = self._get_absolute_path(bucket, key)
        self.relative_path = self._get_relative_path(user, key)
        self.last_modified = self._get_last_modified(obj_metadata)
        self.version = self._get_version(obj_metadata)
        self.bucket = bucket
        self.key = key

    @staticmethod
    def _get_absolute_path(bucket: str, key: str) -> str:
        return f"{S3_PROTOCOL}{bucket}/{key}"

    @staticmethod
    def _get_relative_path(user: str, key: str) -> str:
        user_prefix = f"{user}/"
        return key[len(user_prefix) :]

    @staticmethod
    def _get_last_modified(obj_metadata: Dict) -> datetime:
        return obj_metadata["LastModified"]

    @staticmethod
    def _get_version(obj_metadata: Dict) -> str:
        return obj_metadata["ETag"].strip('"')


class FileStore:
    _buckets: Dict[str, List[str]]

    def __init__(self, buckets: Dict[str, List[str]]):
        self._buckets = buckets

    def list_files(self) -> List[File]:
        files = []

        for bucket, keys in self._buckets.items():
            for key in keys:
                files += self._get_files(bucket, key)

        return files

    def get_file(self, path: str) -> File:
        if not path.startswith(S3_PROTOCOL):
            raise Exception(f"Cannot not process file path '{path}'.")

        resource_path = path.lstrip(S3_PROTOCOL)
        path_parts = resource_path.split("/")

        if len(path_parts) < 3:
            raise Exception(f"Cannot not process file path '{path}'.")

        bucket = path_parts[0]
        if bucket not in self._buckets.keys():
            raise Exception(f"Attempting to access file '{path}' for unavailable bucket" f" '{bucket}'.")

        user = path_parts[1]
        if user not in self._buckets[bucket]:
            raise Exception(
                f"Attempting to access file '{path}' for unavailable user" f" '{user}' for bucket '{bucket}'."
            )

        key = "/".join(path_parts[1:])
        obj_metadata = self._get_object_metadata(bucket, key)
        return File(bucket, user, key, obj_metadata)

    @staticmethod
    def archive_file(file: File) -> None:
        s3_cli = s3_client()
        archive_key = file.key.split("/")
        archive_key.insert(-1, "archive")
        archive_key = os.path.join(*archive_key)

        s3_cli.copy_object(Bucket=file.bucket, Key=archive_key, CopySource = {"Bucket": file.bucket, "Key": file.key})
        s3_cli.delete_object(Bucket=file.bucket, Key=file.key)
        print(f"File moved from {file.bucket}/{file.key} to {file.bucket}/{archive_key}")

    @staticmethod
    def _is_directory(obj: Dict[str, Union[int, str]]) -> bool:
        return obj["Key"][-1] == "/" and obj["Size"] == 0

    def _get_files(self, bucket: str, user: str) -> List[File]:
        object_pages = self._get_object_pages(bucket, user)
        objects = []

        for page in object_pages:
            # Signifies an empty bucket.
            if "Contents" not in page:
                continue

            objects += [
                File(bucket, user, obj["Key"], obj) for obj in page["Contents"] if not self._is_directory(obj)
            ]

        return objects

    @staticmethod
    def _get_object_pages(bucket: str, user: str) -> Iterable:
        return boto3.client("s3").get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{user}/", Delimiter='/')

    @staticmethod
    def _get_object_metadata(bucket: str, key: str) -> Dict:
        return boto3.client("s3").head_object(Bucket=bucket, Key=key)
