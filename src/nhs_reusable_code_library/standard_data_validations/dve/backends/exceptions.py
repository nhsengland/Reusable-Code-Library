"""Errors that are designed to result in more useful critical error messages."""

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any, Optional

from src.nhs_reusable_code_library.standard_data_validations.dve.backends.message import FeedbackMessage
from src.nhs_reusable_code_library.standard_data_validations.dve.metadata_parser.type_hints import EntityName, ErrorCategory, ErrorLocation, Messages

PipelineErrorLocation = str
"""A location within the pipeline execution, for context within the error."""
FieldName = str
"""The name of a field within the data."""
FieldType = str
"""A representation of the type of the field as a string."""
ExpectedFieldType = FieldType
"""A representation of the expected type of the field as a string."""
ActualFieldType = str
"""A representation of the actual type of the field as a string."""


class BackendError(Exception):
    """A base exception type for all backend errors."""


class MessageBearingError(BackendError):
    """A backend error that comes with pre-created messages."""

    def __init__(self, *args: object, messages: Messages) -> None:
        super().__init__(*args)
        self.messages = messages
        """The messages to be returned as part of the error."""


class BackendErrorMixin(ABC, BackendError):
    """A mixin used to create backend error type."""

    @abstractmethod
    def get_message_preamble(self) -> str:
        """Get the start of the message string to be used for logging and feedback.

        This will be joined to the location string with the joiner.
        """

    def get_joiner(self):
        """The joiner between the preamble and the context string."""
        return "in"

    def get_message_epilogue(self) -> Optional[str]:
        """Get the end of the message string to be used for logging and feedback.

        This will be appended straight after the location string. Avoid using this
        unless necessary, since this leads to very verbose errors.

        """
        return None

    def to_message_string(self, location: PipelineErrorLocation) -> str:
        """Create a message from the error."""
        epilogue = self.get_message_epilogue()  # pylint: disable=assignment-from-none
        if epilogue:
            location = "".join((location, epilogue))
        return " ".join((self.get_message_preamble(), self.get_joiner(), location))


class BackendMissingFunctionality(NotImplementedError, BackendErrorMixin):
    """An error raised when a backend is missing the functionality
    required by a given rule config.

    """

    def __init__(self, *args: object, operation: str) -> None:
        super().__init__(*args)
        self.operation = operation
        """The operation which has not been implemented by the backend."""

    def get_message_preamble(self) -> str:
        """Get the start of the message string to be used for logging and feedback.

        This will be joined to the location string with the joiner.
        """
        return f"The running backend does not support the {self.operation!r} operation"

    # pylint: disable=unused-argument
    def get_joiner(self):
        """The joiner between the preamble and the context string."""
        return "specified in"


class MissingEntity(KeyError, BackendErrorMixin):
    """An error to be emitted when a required entity is missing."""

    def __init__(self, *args: object, entity_name: str) -> None:
        super().__init__(*args)
        self.entity_name = entity_name
        """The name of the missing entity."""

    def get_message_preamble(self) -> str:
        """Get the start of the message string to be used for logging and feedback.

        This will be joined to the location string with the joiner.
        """
        return f"Missing entity {self.entity_name!r}"

    def get_joiner(self):
        """The joiner between the preamble and the context string."""
        return "required by"


class MissingRefDataEntity(MissingEntity):  # pylint: disable=too-many-ancestors
    """An error to be emitted when a required refdata entity is missing."""

    def get_message_preamble(self) -> str:
        """Get the start of the message string to be used for logging and feedback.

        This will be joined to the location string with the joiner.
        """
        return f"Missing reference data entity {self.entity_name!r}"


class ConstraintError(ValueError, BackendErrorMixin):
    """Raised when a given constraint is violated."""

    def __init__(self, *args: object, constraint: str) -> None:
        super().__init__(*args)
        self.constraint = constraint
        """The constraint that was violated. This should reference the relevant entities."""

    def get_message_preamble(self) -> str:
        """Get the start of the message string to be used for logging and feedback.

        This will be joined to the location string with the joiner.
        """
        return "Constraint violated"

    def get_message_epilogue(self) -> str:
        """Get the end of the message string to be used for logging and feedback.

        This will be appended straight after the location string.

        Avoid using this unless necessary, since this leads to very verbose errors.
        """
        return f": {self.constraint}"


class ReaderErrorMixin(BackendErrorMixin):
    """A mixin to be used by the reader errors."""

    def get_joiner(self) -> str:
        return "for"


class ReaderLacksEntityTypeSupport(ReaderErrorMixin, TypeError):
    """An error raised when a reader class lacks direct support for
    a given entity type.

    """

    def __init__(self, *args: object, entity_type: Any) -> None:
        super().__init__(*args)
        self.entity_type = entity_type
        """The entity type that is not supported directly by the reader."""

    def get_message_preamble(self) -> EntityName:
        return f"Reader does not support reading directly to entity type {self.entity_type!r}"


class EmptyFileError(ReaderErrorMixin, ValueError):
    """The read file was empty."""

    def get_message_preamble(self) -> str:
        return "File location provided is empty"


class MissingHeaderError(ReaderErrorMixin, ValueError):
    """A header is expected, but was not parsed."""

    def get_message_preamble(self) -> str:
        return "No header was provided"


class FieldCountMismatch(ReaderErrorMixin, ValueError):
    """An error raised when the number of expected fields does not match
    the number of parsed fields.

    """

    def __init__(self, *args: object, n_expected_fields: int, n_actual_fields: int) -> None:
        super().__init__(*args)
        self.n_expected_fields = n_expected_fields
        """The number of fields that should have been in the file."""
        self.n_actual_fields = n_actual_fields
        """The number of fields that were actually in the file."""

    def get_message_preamble(self) -> str:
        return (
            f"Number of fields expected ({self.n_expected_fields}) does not "
            + f"match the number in the provided data ({self.n_actual_fields})"
        )


class SchemaMismatch(ReaderErrorMixin, ValueError):
    """An error raised when a file does not match a given schema (e.g. fields
    are missing).

    """

    def __init__(
        self,
        *args: object,
        missing_fields: Optional[set[FieldName]] = None,
        extra_fields: Optional[set[FieldName]] = None,
        wrong_types: Optional[Mapping[FieldName, tuple[ActualFieldType, ExpectedFieldType]]] = None,
    ):
        self.missing_fields = missing_fields or set()
        """Fields that are missing from the expected schema."""
        self.extra_fields = extra_fields or set()
        """Fields that are additional to the expected schema."""
        self.wrong_types = wrong_types or {}
        """
        Fields that are the wrong type in the expected schema (a mapping of field name to a
        tuple of actual and expected field type, both as strings).

        """
        super().__init__(*args)

    def get_message_preamble(self) -> str:
        return "Schema mismatch"

    def get_message_epilogue(self) -> str:
        message_components = []
        if self.missing_fields:
            fields_str = ", ".join(map(repr, sorted(self.missing_fields)))
            message_components.append(f"The following fields are missing: {fields_str}")
        if self.extra_fields:
            fields_str = ", ".join(map(repr, sorted(self.extra_fields)))
            message_components.append(f"The following extra fields were provided: {fields_str}")
        if self.wrong_types:
            type_strings = []
            for field_name in sorted(self.wrong_types):
                actual_type, expected_type = self.wrong_types[field_name]
                type_strings.append(
                    f"{field_name!r} (got {actual_type!r}, expected {expected_type!r})"
                )
            fields_str = ", ".join(type_strings)
            message_components.append(f"The following fields had the wrong types: {fields_str}")

        return ". ".join(("", *message_components))


class NoComplexSchemaSupport(ReaderErrorMixin, ValueError):
    """An error raised when a schema is invalid for a given file reader
    (e.g. the reader does not support complex types).

    """

    def get_message_preamble(self) -> str:
        return "The specified reader does not support complex data types"


def render_error(
    error: Exception,
    location: PipelineErrorLocation,
    logger: Optional[logging.Logger] = None,
    entity_name: Optional[EntityName] = None,
    error_location: Optional[ErrorLocation] = None,
    error_category: Optional[ErrorCategory] = None,
) -> Messages:
    """Convert a generic error to a message string."""
    if isinstance(error, MessageBearingError):
        return error.messages

    if isinstance(error, BackendErrorMixin):
        msg = error.to_message_string(location)
        entity_name = entity_name or getattr(error, "entity_name", None)
        error_location = error_location or getattr(error, "error_location", None)
        error_category = error_category or getattr(error, "error_category", None)
    else:
        msg = f"Unexpected error ({type(error).__name__}: {error}) in {location}"

    if logger:
        logger.error(msg)
        logger.exception(error)
    return [
        FeedbackMessage(
            entity_name,
            None,
            failure_type="integrity",
            error_message=msg,
            error_location=error_location,
            category=error_category,
        )
    ]
