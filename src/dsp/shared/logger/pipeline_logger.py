# pylint: disable=W0703
import atexit
import getpass
import gzip
import logging
import os
import signal
import socket
from datetime import datetime

from dsp.shared.aws import local_mode
from dsp.shared.content_types import STANDARD_DATE_FORMAT
from dsp.shared.logger.formatters import JSONFormatter


class PipelineLogFileHandler(logging.StreamHandler):

    def __init__(
            self,
            pipeline_name: str,
            extra_path_part: str = None,
            base_log_dir: str = None,
            mode: str = 'a+',
            encoding: str = None,
            level=logging.NOTSET,
            compressed: bool = True
    ):
        super(PipelineLogFileHandler, self).__init__()

        logging.Handler.__init__(self, level=level)

        base_log_dir = base_log_dir or ('~/.dsp_dev_logs/dsp' if local_mode() else '/dsp-logs/dsp')

        self.mode = mode
        self.encoding = encoding
        self.formatter = JSONFormatter()
        self._compressed = compressed
        self.terminator = b'\n' if compressed else '\n'
        self._file_opener = gzip.open if compressed else open
        self._extension = 'json.gz' if compressed else 'json'
        self._base_log_dir = base_log_dir
        self._pipeline_name = pipeline_name
        self._extra_path_part = str(extra_path_part or '').strip()
        self._current_logfile = None

        signal.signal(signal.SIGTERM, self.close)
        signal.signal(signal.SIGINT, self.close)
        signal.signal(signal.SIGQUIT, self.close)
        atexit.register(self.close)

    def get_log_file_path(self) -> str:
        """
        Identifies the new log file name to be assigned based on input time.
        """
        hostname_parts = socket.gethostname().split('-')
        hostname = '{}/{}'.format('-'.join(hostname_parts[:3]), '-'.join(hostname_parts[3:])).strip('/')
        date_part = datetime.now().strftime(STANDARD_DATE_FORMAT)
        username = getpass.getuser()
        root_path = os.path.join(self._base_log_dir, username, "pipeline", self._pipeline_name, date_part)

        if self._extra_path_part:
            root_path = os.path.join(root_path, self._extra_path_part)

        return os.path.join(
            root_path, f'{hostname}/{datetime.now().hour}/{os.getpid()}-{id(self)}.{self._extension}'
        )

    def _maybe_close_stream(self):

        if not self.stream or type(self.stream).__module__ == '_pytest.capture':
            return

        try:
            self.flush()
        finally:
            stream = self.stream
            self.stream = None
            stream.close()

    def close(self):
        """
        Closes the stream.
        """
        self.acquire()
        try:
            try:
                self._maybe_close_stream()
            finally:
                # Issue #19523: call unconditionally to
                # prevent a handler leak when delay is set
                logging.StreamHandler.close(self)
        finally:
            self.release()

    def _maybe_open_stream(self):
        """
        Open the current base file with the (original) mode and encoding.
        Return the resulting stream.
        """
        if self.stream and type(self.stream).__module__ == '_pytest.capture':
            return
        desired_logfile = self.get_log_file_path()
        if desired_logfile == self._current_logfile and self.stream:
            return

        self.acquire()
        try:
            self._maybe_close_stream()

            os.makedirs(os.path.dirname(desired_logfile), exist_ok=True)

            self.stream = self._file_opener(desired_logfile, self.mode, encoding=self.encoding)
            self._current_logfile = desired_logfile
        finally:
            self.release()

    def format(self, record):
        json_line = self.formatter.format(record)
        if not self._compressed:
            return json_line
        return json_line.encode()

    def emit(self, record):
        """
        Emit a record.

        If the stream was not opened because 'delay' was specified in the
        constructor, open it before calling the superclass's emit.
        """
        if not record:
            return

        self._maybe_open_stream()

        logging.StreamHandler.emit(self, record)

    def __repr__(self):
        level = logging.getLevelName(self.level)
        return '<%s %s (%s)>' % (self.__class__.__name__, self._current_logfile, level)
