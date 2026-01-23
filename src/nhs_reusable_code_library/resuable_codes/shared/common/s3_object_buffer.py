import io
import sys
from io import SEEK_SET, SEEK_END, SEEK_CUR
from typing import IO, AnyStr, List, Union, Optional, Any

from botocore.response import StreamingBody

from src.nhs_reusable_code_library.resuable_codes.shared.common.retry_predicates import s3_throttle_retry_predicate
from src.nhs_reusable_code_library.resuable_codes.shared.common import retry


class S3ObjectBuffer(IO):

    def __init__(self, obj, encoding: Optional[str] = 'utf-8', line_ending: str = '\n',
                 buffer_size: int = 1024 * 1024 * 5, writable: bool = False, name: str = "") -> None:
        if writable and buffer_size < 1024 * 1024 * 5:
            raise ValueError("Minimum buffer size for writing is 5MB")
        self._object = obj
        self._object_map = None
        self._closed = False
        self._encoding = encoding
        self._buffer = '' if encoding else b''
        self._bytes_read = 0
        self._bytes_written = 0
        self._parts_written = 0
        self._parts = []
        self._fully_read = False
        self._position = 0
        self._body = None  # type: StreamingBody
        self._line_ending = line_ending
        self._buffer_size = buffer_size
        self._mp = None
        self._mode = 'w' if writable else 'r'
        self._name = name

    def __iter__(self):
        return self

    def __next__(self):
        line = self.readline()
        if line is None:
            raise StopIteration()

        return line.strip()

    @property
    def mode(self) -> str:
        return '{}{}'.format(self._mode, '' if self._encoding else 'b')

    @property
    def name(self) -> str:
        return self._name

    @property
    def raw(self):
        return self.body

    @property
    def closed(self):
        return self._closed

    @property
    def object_map(self):
        if self._object_map is None:
            self._object_map = self._object.get()
        return self._object_map

    @property
    def body(self):
        if self._body is None:
            self._body = self.object_map['Body']
        return self._body

    def fileno(self):
        return self.object_map.key

    @retry(max_retries=5, exponential_backoff=True, should_retry_predicate=s3_throttle_retry_predicate)
    def _complete_multipart_upload(self):
        self._mp.complete(MultipartUpload={'Parts': self._parts})

    def close(self):
        if self._mp:
            if self.tell():
                self._upload_next_part()

            if self._bytes_written and self._mp:
                self._complete_multipart_upload()
            elif self._mp:
                # AWS complains with "The XML you provided was not well-formed or
                # did not validate against our published schema" when the input is
                # completely empty => abort the upload, no file created.
                # We work around this by creating an empty file explicitly.
                assert self._mp, "no multipart upload in progress"
                self._mp.abort()

                self._object.put(Body=b'')
            self._mp = None
        if self._closed:
            return
        if self._body:
            self._body.close()
        self._closed = True

    def flush(self):
        raise NotImplementedError()

    def readable(self):
        return self._mode == 'r' and self.body and (self._fully_read or not self._closed)

    def readinto(self, byte_array: bytearray) -> int:
        to_read = len(byte_array)
        chunk = self.read(to_read)
        bytes_read = len(chunk)
        for i, char in enumerate(chunk):
            byte_array[i] = char
        return bytes_read

    def peek(self, num: int = None) -> Union[str, bytes]:
        current_pos = self._position
        bytes_read = self.read(num)
        self._position = current_pos
        return bytes_read

    def read(self, num: int = None) -> Union[str, bytes]:
        if self._mode != 'r':
            raise io.UnsupportedOperation("Not readable")
        if self._fully_read:
            end = self._bytes_read if num is None else max(min(self._position + num, self._bytes_read), 0)
            pos = min(max(self._position, 0), self._bytes_read)

            chunk = self._buffer[min(pos, end):max(pos, end)]
            self._position += end - pos
            return chunk

        if num is None:
            chunk = self.body.read()
            if self._encoding:
                chunk = self.ensure_valid_chunk(chunk)
            else:
                self._bytes_read = len(self._buffer) + len(chunk)
            self._buffer = self._buffer + chunk
            self._fully_read = True
            self._position = len(self._buffer)
            return chunk

        if num == 0:
            return '' if self._encoding else b''

        end = max(0, self._position + num)

        if end > self._bytes_read:

            bytes_to_read = max(end - self._bytes_read, self._buffer_size)

            chunk = self.body.read(bytes_to_read)
            byte_count = len(chunk)

            if self._encoding:
                chunk = self.ensure_valid_chunk(chunk)
            else:
                self._bytes_read += byte_count

            self._buffer = self._buffer + chunk

            # TODO This should really check that you're not at the end of the file
            if byte_count < bytes_to_read:
                self._fully_read = True

        end = min(end, self._bytes_read)

        read_from = min(self._position, end)
        read_to = max(self._position, end)

        self._position = end
        return self._buffer[read_from: read_to]

    def readline(self, limit: int = -1) -> Union[Optional[str], Any]:
        if self._encoding is None:
            raise ValueError('readline only works if _encoding was defined')

        max_read = limit if limit > 0 else sys.maxsize
        chunk_size = min(max_read, 8192)
        bytes_read = 0
        index = -1
        local_buffer = ''
        while index < 0 and bytes_read < max_read:
            chunk = self.read(chunk_size)
            if not chunk:
                if not local_buffer:
                    return None
                break

            local_buffer += chunk
            index = local_buffer.find(self._line_ending, bytes_read)
            bytes_read += len(chunk)

        if index < 0:
            return local_buffer

        self.seek((bytes_read - index - 1) * -1, SEEK_CUR)
        return local_buffer[0:index + 1]

    def readlines(self, size=None):
        if self._encoding is None:
            raise ValueError('readlines only works if _encoding was defined')

        local_buffer = self.read(size)

        position = 0
        index = local_buffer.find(self._line_ending, position)
        while index > 0:
            yield local_buffer[position:index + 1]
            next_index = local_buffer.find(self._line_ending, index + 1)
            position = index + 1
            index = next_index

        if position < len(local_buffer):
            yield local_buffer[position:]

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        if whence == SEEK_END:
            self.read()  # read full stream
            offset = offset * -1 if offset > 0 else offset
            self._position = max(0, self._bytes_read + offset)
            return self._position

        cur_pos = 0 if whence == SEEK_SET else self._position

        new_pos = max(0, cur_pos + offset)
        if new_pos > self._bytes_read:
            self.read(new_pos - self._position)
            return self._position

        self._position = new_pos
        return self._position

    def seekable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return False

    def tell(self):
        return self._position

    def truncate(self, size=None):
        raise NotImplementedError()

    def writable(self) -> bool:
        return self._mode == 'w'

    def writelines(self, lines: List[AnyStr]) -> None:
        for arg in lines:
            self.write(arg)

    @retry(max_retries=5, exponential_backoff=True, should_retry_predicate=s3_throttle_retry_predicate)
    def _initiate_multipart_upload(self):
        self._mp = self._object.initiate_multipart_upload(**({}))

    def write(self, b):
        """Write the given bytes (binary string) to the S3 file.

        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away."""
        if self._mode != 'w':
            raise io.UnsupportedOperation("Not writable")
        if self._mp is None:
            self._initiate_multipart_upload()
        if not self._encoding:
            self._buffer = b"".join([self._buffer, b])
        elif isinstance(b, str):
            self._buffer = "".join([self._buffer, b])
        else:
            self._buffer = "".join([self._buffer, str(b)])

        self._bytes_written += len(b)
        self._position = len(self._buffer)

        if self.tell() >= self._buffer_size:
            self._upload_next_part()

        return len(b)

    @retry(max_retries=5, exponential_backoff=True, should_retry_predicate=s3_throttle_retry_predicate)
    def _upload_part(self, part):
        return part.upload(Body=self._buffer)

    def _upload_next_part(self):
        part_num = self._parts_written + 1
        part = self._mp.Part(part_num)
        upload = self._upload_part(part)
        self._parts.append({'ETag': upload['ETag'], 'PartNumber': part_num})
        self._parts_written += 1
        self._buffer = '' if self._encoding else b''
        self._position = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            raise exc_val
        else:
            self.close()

    def ensure_valid_chunk(self, chunk, run_count: int = 0):
        """ We need to check that the chunk is decode-able.
            This is because, with multibyte chars, when we create our chunks, we are just ticking through each byte
            A multibyte char is between 2 and 4 bytes, so we need to check that we have not chopped off a char
            midway through it's bytes, so will loop through 3 times, removing last char and attempting to decode."""

        try:
            chunk = chunk.decode(self._encoding)
            self._bytes_read += len(chunk)
        except UnicodeDecodeError as e:
            if run_count < 3:
                run_count += 1
                chunk = chunk[:-1]
                chunk = self.ensure_valid_chunk(chunk, run_count)
            else:
                raise e

        return chunk
