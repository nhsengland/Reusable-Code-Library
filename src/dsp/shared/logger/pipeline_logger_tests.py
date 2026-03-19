import logging

import pytest

from dsp.pipeline.pipeline_logging import setup_pipeline_logging
from shared.logger import app_logger


@pytest.fixture()
def logger_not_setup():

    handlers = logging.root.handlers.copy()
    service_name = app_logger.service_name
    logging.root.handlers.clear()

    app_logger._is_setup = False

    yield True

    app_logger._is_setup = False
    app_logger.setup(service_name, handlers, overwrite=True)


def test_pipeline_logging(logger_not_setup):

    do_thing()


@setup_pipeline_logging(pipeline_name="test")
def do_thing():

    app_logger.info(args=lambda: dict(abc=123))
