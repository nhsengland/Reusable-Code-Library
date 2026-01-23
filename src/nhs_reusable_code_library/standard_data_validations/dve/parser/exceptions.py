"""Exceptions used in the file parser."""

from functools import partial


class UnsupportedSchemeError(ValueError):
    """An error raised when a URI scheme is unsupported."""


class FileAccessError(IOError):
    """File not able to be accessed in the expected location."""


class EmptyFileError(ValueError):
    """The read file was empty."""


class MissingHeaderError(ValueError):
    """A header is expected, but was not parsed."""


class FieldCountMismatch(ValueError):
    """An error raised when the number of expected fields does not match
    the number of parsed fields.

    """


class SDCSTemplateValidationFailure(ValueError):
    """An error to indicate a template validation failure."""

    def __init__(self, *args: object, errors: dict[str, str]) -> None:
        super().__init__(*args)
        self.errors = errors
        """
        The template validation failures. This will map the field name to the
        error message from the template.

        """

    def __reduce__(self):
        """Allows the class to be pickled"""
        return partial(self.__class__, errors=self.errors), tuple(self.args)


class LogDataLossWarning(UserWarning):
    """A warning emitted when a log file is improperly closed and the log data
    would have been lost if not caught by the finaliser.

    """
