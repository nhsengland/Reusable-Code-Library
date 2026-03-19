import json
import logging
import traceback
from collections import OrderedDict
from datetime import datetime, date

from dsp.shared.version_data import FULL_VERSION_STRING


def json_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if isinstance(obj, type) or callable(obj):
        return repr(obj)

    return str(repr(obj))


class StructuredFormatter(logging.Formatter):

    def format(self, record: logging.LogRecord) -> dict:

        log = OrderedDict()

        log.update({
            'timestamp': datetime.fromtimestamp(record.created).timestamp(),
            'level': record.levelname
        })

        if record.args:

            args = record.args

            if isinstance(args, list) and callable(args[0]):
                args = args[0]()
            elif not isinstance(args, dict):
                args = dict(args=record.args)

            if args:
                rec_ts = args.get('timestamp')
                if rec_ts:
                    args['timestamp'] = datetime.fromtimestamp(rec_ts).timestamp()

                log.update(args)

        log_info = {
            'logger': record.name,
            'level': record.levelname,
            'path': record.pathname,
            'module': record.module,
            'line_no': record.lineno,
            'func': record.funcName,
            'filename': record.filename,
            'pid': record.process,
            'version': FULL_VERSION_STRING
        }

        if record.thread:
            log_info['thread'] = record.thread
            log_info['thread_name'] = record.threadName

        if record.processName:
            log_info['process_name'] = record.processName

        if record.msg:

            log['message'] = record.getMessage()

        log['log_inf'] = log_info

        exc_info = record.__dict__.get('exc_info', None)

        if exc_info:
            _, exc, _ = exc_info

            log['ex_type'] = '{}.{}'.format(exc.__class__.__module__, exc.__class__.__name__)

            if exc.__cause__:
                log['ex_cause'] = exc.__cause__

            log['ex'] = str(exc)

            log['ex_tb'] = ''.join(traceback.format_tb(exc.__traceback__))

            log_info['stack_info'] = record.stack_info

        return log


class JSONFormatter(StructuredFormatter):

    def format(self, record: logging.LogRecord) -> str:

        record = super(JSONFormatter, self).format(record)

        return json.dumps(record, default=json_serializer)
