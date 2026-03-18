import logging
from inspect import getfullargspec

from pyspark import Broadcast

from dsp.pipeline import spark_state
from dsp.shared.logger.pipeline_logger import PipelineLogFileHandler
from dsp.shared.logger import app_logger, find_caller_info, get_method_name
from dsp.shared.logger.context import _ThreadLocalContextStorage, _LoggingContext


def worker_pipeline_stage(action: str = None):
    """
    Sets up your Databricks worker logging and broadcast state.

    In order to log the same set of fields in your worker as on your master, you have to create a Spark broadcast.

    Args:
        action: name of the worker action

    Examples:
        extra_args = {
            'run_id': 1234,
            'cluster_id': 'fake-5678',
        }


        # wrap your 'worker' code in pipeline_stage and specify a meaningful action
        @worker_pipeline_stage(action='worker_times_two')
        def worker(row):
            return row * 2


        @setup_pipeline_logging(pipeline_name='master', **extra_args)
        def master():
            # create a broadcast of the arguments to log in workers when worker function is called (this is optional)
            broadcast_state = sc.broadcast(extra_args)

            rdd = sc.parallelize(range(10))
            rdd = rdd.map(functools.partial(
                worker,
                extra_path_part=str(metadata['submission_id']),  # optional, will not log to subfolder
            ))
            rdd.collect()
    """

    def _worker_decorator(f):

        def _process_wrapper(
                *args,
                **kwargs
        ):

            broadcast_state = None
            if 'broadcast_state' in kwargs:
                broadcast_state = kwargs['broadcast_state']
                if 'broadcast_state' in getfullargspec(f).args:
                    kwargs['broadcast_state'] = broadcast_state

            if args and not broadcast_state:
                for arg in args:
                    if isinstance(arg, Broadcast):
                        broadcast_state = arg
                        break

            if not broadcast_state:
                raise ValueError('broadcast_state required')

            spark_state.set_broadcast_state(broadcast_state.value)
            log_args = spark_state.log_params().copy()

            pipeline_name = log_args.pop('pipeline_name', None)
            extra_path_part = log_args.pop('extra_path_part', None)

            if not pipeline_name:  # when called without setup_pipeline_logging
                # such as during an ad-hoc databricks notebook call
                return f(*args, **kwargs)

            if app_logger.service_name != 'worker' and not logging.root.handlers:

                app_logger.setup('worker', [PipelineLogFileHandler(pipeline_name, extra_path_part)])

            caller_info = find_caller_info(_process_wrapper, True)
            method_name = get_method_name(f, *args, **kwargs)

            _action_type = action or method_name

            # have to re-instantiate _LoggingContext, as it's not safe to pickle
            # and would not work with pyspark
            with _LoggingContext(_ThreadLocalContextStorage()).start_action(
                    action=action, caller_info=caller_info, **log_args
            ):
                return f(*args, **kwargs)

        return _process_wrapper

    return _worker_decorator
