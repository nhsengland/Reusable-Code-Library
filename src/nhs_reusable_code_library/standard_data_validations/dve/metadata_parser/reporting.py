"""Metadata for reporting information."""

import json
import warnings
from collections.abc import Callable
from typing import Any, ClassVar, Optional, Union

from pydantic import BaseModel, root_validator, validate_arguments
from typing_extensions import Literal

from dve.core_engine.templating import template_object
from dve.core_engine.type_hints import (
    ErrorCategory,
    ErrorCode,
    ErrorEmitValue,
    ErrorLocation,
    ErrorMessage,
    TemplateVariables,
)


class BaseReportingConfig(BaseModel):
    """A base reporting config.

    In general, we want the reporting config to be strictly validated, but
    we also want to be able to effectively template it (so can't be too
    strict with the initial validation).

    """

    UNTEMPLATED_FIELDS: ClassVar[set[str]] = set()
    """Fields that should not be templated."""

    emit: Optional[str] = None
    """
    The level of information in the message.

    This should be coercable to an `ErrorEmitLevel` literal after templating.

    """
    code: ErrorCode = None
    """The error code for the message."""
    message: ErrorMessage = None
    """The string contained within the message."""
    category: Optional[str] = None
    """
    The category of the message.

    This should be coercable to an `ErrorCategory` after templating.

    """
    location: ErrorLocation = None
    """
    The location of the aspect of the record that the message pertains to.

    This can use dot notation to specify multiple fields within a struct
    or array of structs (but only up to a single level deep).

    ## Multiple fields

    For multiple fields within the record, use commas to separate fields.
    For multiple fields within a struct/array of struct, use braces after the
    dot and commas within braces to select fields.

    ## Examples

    For the following example record:
    ```python
    {
        "field_a": "value",
        "field_b": "another",
        "struct": {"A": 1, "B": 2, "C": 3},
        "array": [{"A": 1, "B": 2}, {"A": 3, "B": 4}]
    }
    ```

     - The location `'field_a'` will yield `{"field_a": "value"}`
     - The location `'field_a, field_b'` will yield `{"field_a": "value", "field_b": "another"}`
     - The location `'struct'` will yield `{"struct": {"A": 1, "B": 2}}`
     - The location `'struct.A'` will yield `{"A": 1}`.
     - The location `'struct.{A, B}'` will yield `{"A": 1, "B": 2}`
     - The location `'struct.*'` will yield `{"A": 1, "B": 2, "C": 3}`
     - The location `'array'` will yield `{"array": [{"A": 1, "B": 2}, {"A": 3, "B": 4}]}`
     - The location `'array.A'` will yield `[{"A": 1}, {"A": 3}]`
     - The location `'array.{A, B}'` will yield `[{"A": 1, "B": 2}, {"A": 3, "B": 4}]`
     - The location `'array.*'` will yield `[{"A": 1, "B": 2}, {"A": 3, "B": 4}]`

    """
    reporting_entity_override: Optional[str] = None
    """
    A string to be used in place of the real entity name in the error report.

    If this is not provided, the entity name from the offending rule will be used in
    the report.

    """
    reporting_field_override: Optional[str] = None
    """
    A string (indicating a field name or a comma separated list of field names) to
    be used in place of the reporting fields in the error report.

    If this is not provided, the top level names from the `location` field
    will be used in the report.

    """

    def template(
        self,
        local_variables: TemplateVariables,
        *,
        global_variables: Optional[TemplateVariables] = None,
    ) -> "BaseReportingConfig":
        """Template a reporting config."""
        type_ = type(self)
        if global_variables:
            variables = global_variables.copy()
            variables.update(local_variables)
        else:
            variables = local_variables
        templated = template_object(self.dict(exclude=self.UNTEMPLATED_FIELDS), variables, "jinja")
        templated.update(self.dict(include=self.UNTEMPLATED_FIELDS))
        return type_(**templated)


class ReportingConfig(BaseReportingConfig):
    """A base model defining the 'final' reporting config for a message."""

    emit: ErrorEmitValue = "record_failure"
    category: ErrorCategory = "Bad value"

    def _get_root_and_fields(self) -> tuple[Optional[str], Union[Literal["*"], list[str]]]:
        """Get the source field (or None, if the source is the root of the record)
        and a list of fields (or `'*'`) if all fields are to be selected from
        the location.

        """
        if self.location is None:
            raise ValueError("No root/fields for `None` location")

        nesting_splits = self.location.split(".")
        if len(nesting_splits) > 2:
            raise ValueError("Nesting must be a maximum of one level")

        fields: Union[Literal["*"], list[str]]
        fields = [field.strip() for field in nesting_splits[-1].strip("{}").split(",")]
        if fields and fields[0] == "*":
            fields = "*"

        if len(nesting_splits) == 1:
            return None, fields
        return nesting_splits[0], fields

    @property
    def legacy_location(self) -> Optional[str]:
        """DEPRECATED: The legacy error location, extracted from `location`."""
        warnings.warn("Use new combined `location` field", DeprecationWarning)
        if self.location is None:
            return None

        root_field, fields = self._get_root_and_fields()
        if root_field:
            return root_field
        if fields != "*" and len(fields) == 1:
            return fields[0]
        return None

    @property
    # pylint: disable=too-many-return-statements
    def legacy_reporting_field(self) -> Union[str, list[str], None]:
        """DEPRECATED: The legacy reporting field, extracted from `location`."""
        warnings.warn("Use new combined `location` field", DeprecationWarning)
        if self.location is None:
            return None

        root_field, fields = self._get_root_and_fields()
        if fields == "*":
            if not root_field:
                warnings.warn("Cannot include all fields in root of record in legacy reporting")
            return None

        # Need to enable trailing comma to duplicate old `["field"]` and `"field"`
        # distinction when using legacy reporting.
        pruned_fields = [field for field in fields if field]
        if not pruned_fields:
            return None

        if root_field:
            # This will definitely be a 'real' name since we've returned `None` above
            # for missing names.
            if len(fields) == 1:
                return fields[0]
            # If `["field", ""]` (e.g. has trailing comma), return `["field"]`
            return pruned_fields

        if len(fields) == 1:  # This will be in the error location, 'real' or no.
            return None
        return pruned_fields

    @property
    def legacy_error_type(self) -> Literal["record", "submission", "integrity"]:
        """DEPRECATED: The legacy error type."""
        warnings.warn("Use new combined `emit` field", DeprecationWarning)
        if self.emit == "critical_failure":
            return "integrity"
        if self.emit == "submission_failure":
            return "submission"
        return "record"

    def get_location_selector(
        self,
    ) -> Callable[[dict[str, Any]], Union[list[dict[str, Any]], dict[str, Any], None]]:
        """Get a function which extracts the location from a provided record."""
        # TODO: Check this against the schema to eliminate type checks at runtime.
        # This should enable us to use some really efficient 'getter' functions.
        if self.location is None:
            return lambda _: None

        root_field, fields = self._get_root_and_fields()
        if not root_field:
            if fields == "*":
                return lambda record: record
            return lambda record: {field: record[field] for field in fields if field}

        if fields == "*":
            return lambda record: record[root_field]  # type: ignore

        def _selector(record: dict[str, Any]) -> Union[list[dict[str, Any]], dict[str, Any], None]:
            map_or_list = record[root_field]  # type: ignore
            if not isinstance(map_or_list, (dict, list)):
                return None

            pruned = [field for field in fields if field]
            if isinstance(map_or_list, list):
                return [{field: struct[field] for field in pruned} for struct in map_or_list]
            return {field: map_or_list[field] for field in pruned}

        return _selector

    def get_location_value(
        self, record: Optional[dict[str, Any]]
    ) -> Union[list[dict[str, Any]], dict[str, Any], None]:
        """Get the value of the location field from a record."""
        if record is None:
            return None
        return self.get_location_selector()(record)


class LegacyReportingConfig(BaseReportingConfig):
    """An untemplated reporting config. This _must_ be templated prior to use.

    This class also enables the conversion of deprecated fields to their
    modern equivalents as part of the templating process.

    """

    legacy_location: Optional[str] = None
    """DEPRECATED: The legacy error location, now a component of `location`."""
    legacy_reporting_field: Optional[Union[str, list[str]]] = None
    """DEPRECATED: The legacy reporting field, now a component of `location`."""
    legacy_error_type: Optional[str] = None
    """DEPRECATED: The legacy error type."""
    legacy_is_informational: Optional[Union[bool, str]] = None
    """DEPRECATED: The legacy 'is_informational' flag."""

    @root_validator(allow_reuse=True, skip_on_failure=True)
    @classmethod
    def _ensure_only_one_reporting_config(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Ensure only the modern or legacy location is populated."""
        has_modern = bool(values.get("location"))
        has_legacy = bool(values.get("legacy_location") or values.get("legacy_reporting_field"))

        if has_modern and has_legacy:
            raise ValueError(
                "Cannot have current (`location`) and legacy (`legacy_location` "
                + "/`legacy_reporting_field`) error location specification"
            )
        return values

    @root_validator(allow_reuse=True, skip_on_failure=True)
    @classmethod
    def _ensure_only_one_error_type_config(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Ensure only the modern or legacy error type is populated."""
        has_modern = bool(values.get("emit"))
        has_legacy = bool(
            values.get("legacy_error_type") or (values.get("legacy_is_informational") is not None)
        )

        if has_modern and has_legacy:
            raise ValueError(
                "Cannot have current (`location`) and legacy (`legacy_location` "
                + "/`legacy_reporting_field`) error location specification"
            )
        return values

    @staticmethod
    @validate_arguments
    def _convert_legacy_emit_value(
        failure_type: Literal["record", "submission", "integrity", "group"], is_informational: bool
    ) -> str:
        """Resolve the legacy 'failure_type' and 'is_informational' strings to a new
        'emit' string.

        """
        if is_informational:
            emit = "warning"
        elif failure_type in ("submission", "group"):
            emit = "submission_failure"
        elif failure_type == "integrity":
            emit = "critical_failure"
        else:
            emit = "record_failure"
        return emit

    @staticmethod
    @validate_arguments
    def _convert_legacy_reporting_fields(
        error_location: Optional[str] = None, reporting_field: Union[str, list[str], None] = None
    ) -> Optional[str]:
        """Convert legacy reporting field specification to a new location string."""
        if error_location is None and reporting_field is None:
            return None

        if isinstance(reporting_field, str) and reporting_field.startswith("["):
            reporting_field = json.loads(reporting_field)

        # RIP record location confusion
        if reporting_field is None and error_location is None:
            location: Optional[str] = None
        elif reporting_field is None:
            location = error_location
        elif error_location is None:
            if isinstance(reporting_field, str):
                location = reporting_field
            else:
                location = ", ".join(reporting_field)
                if len(reporting_field) == 1:
                    location += ","
        elif isinstance(reporting_field, list):
            location = f"{error_location}.{{{','.join(reporting_field)}}}"
        else:
            location = f"{error_location}.{reporting_field}"

        return location

    def template(
        self,
        local_variables: TemplateVariables,
        *,
        global_variables: Optional[TemplateVariables] = None,
    ) -> "ReportingConfig":
        """Template the untemplated reporting config."""
        if global_variables:
            variables = global_variables.copy()
            variables.update(local_variables)
        else:
            variables = local_variables

        templated = template_object(self.dict(exclude=self.UNTEMPLATED_FIELDS), variables, "jinja")
        templated.update(self.dict(include=self.UNTEMPLATED_FIELDS))
        error_location = templated.pop("legacy_location")
        reporting_field = templated.pop("legacy_reporting_field")
        if templated.get("location") is None:
            templated["location"] = self._convert_legacy_reporting_fields(
                error_location, reporting_field
            )

        is_informational = templated.pop("legacy_is_informational")
        failure_type = templated.pop("legacy_error_type")
        if templated.get("emit") is None and (
            is_informational is not None or failure_type is not None
        ):
            templated["emit"] = self._convert_legacy_emit_value(failure_type, is_informational)

        return ReportingConfig(**templated)
