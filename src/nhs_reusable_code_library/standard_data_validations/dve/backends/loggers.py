"""Logger setup for the core engine."""

import logging
import sys
import time


# pylint: disable=too-few-public-methods
class LogNameFilter(logging.Filter):  # pragma: no cover
    """Filter the logger's name to take only the topmost level."""

    def filter(self, record):
        """Filter the logger's name to take only the topmost level."""
        record.name = record.name.rsplit(".", 1)[-1]
        return True


class UTCFormatter(logging.Formatter):  # pragma: no cover
    """A formatter with timestamps in the UTC timezone."""

    converter = time.gmtime


def get_default_handler() -> logging.Handler:  # pragma: no cover
    """Get the default logging handler."""
    handler = logging.StreamHandler(sys.stderr)
    handler.addFilter(LogNameFilter())
    formatter = UTCFormatter("[%(asctime)s] %(levelname)s - %(name)s: %(message)s")
    handler.setFormatter(formatter)
    return handler


def get_logger(component: str) -> logging.Logger:  # pragma: no cover
    """Get a base logger, that handles its own logging and does not propagate."""
    logger = logging.getLogger(component)

    if not logger.hasHandlers():  # First time configuration.
        logger.propagate = False
        handler = get_default_handler()
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    return logger


def get_child_logger(component: str, parent: logging.Logger) -> logging.Logger:  # pragma: no cover
    """Get a child logger from a parent.

    These propagate messages up to the parent, rather than handling
    messages themselves.

    """
    if parent.name == "root":
        logger_name = component
    else:
        logger_name = f"{parent.name}.{component}"
    return logging.getLogger(logger_name)
