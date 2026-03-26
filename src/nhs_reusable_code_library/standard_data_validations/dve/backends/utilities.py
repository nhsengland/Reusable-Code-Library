"""A base set of utilities that should be useful across multiple backend implementations"""

import warnings
from collections import deque
from collections.abc import Sequence
from typing import Optional

from dve.core_engine.message import FeedbackMessage
from dve.core_engine.type_hints import ExpressionArray, MultiExpression

BRACKETS = {"(": ")", "{": "}", "[": "]", "<": ">"}
"""A mapping of opening brackets to their closing counterpart."""
STRING_START_CHARS = {'"', "'"}
"""Characters which define the start or end of a string."""
ESCAPE_CHARACTERS = {"\\"}
"""Characters which escape the next character."""
CLOSING_BRACKETS = {value: key for key, value in BRACKETS.items()}
"""A mapping of closing brackets to their opening counterpart."""


class ExpressionsMisparseWarning(UserWarning):
    """Emitted when multiple SQL expressions are probably not split correctly."""


def _split_multiexpr_string(expressions: MultiExpression) -> ExpressionArray:
    """Split multiple SQL expressions in a comma-delimited string into an array
    of SQL expressions.

    """
    # Need to ensure we don't consider any characters which are being escaped.
    in_escape = False
    # Or any characters which are inside strings.
    string_opened_with: Optional[str] = None
    # ...and need to ensure we don't split on commas inside brackets, so have to
    # keep track of what brackets are open and closed (except those in strings).
    bracket_stack: deque[str] = deque()

    expression_list, slice_start = [], 0
    for slice_end, char in enumerate(expressions):
        # Handle character escaping
        if in_escape:
            in_escape = False
            continue

        if char in ESCAPE_CHARACTERS:
            in_escape = True
            continue

        # Ignore everything that goes on in a string
        if not string_opened_with and char in STRING_START_CHARS:
            string_opened_with = char
            continue

        if string_opened_with:
            if char == string_opened_with:
                string_opened_with = None
            continue

        # Manage the open and close brackets
        if char in BRACKETS:
            bracket_stack.append(char)
            continue

        if char in CLOSING_BRACKETS:
            opening_bracket = bracket_stack.pop()
            # Don't strictly need to check this: Spark will spit out an error, but might
            # as well provide more context.
            if CLOSING_BRACKETS[char] != opening_bracket:
                bracket_stack.append(opening_bracket)
            continue

        # Finally consider whether the character is a comma outside of any brackets.
        if char == "," and not bracket_stack:
            expression_list.append(expressions[slice_start:slice_end])
            slice_start = slice_end + 1

    warning_reasons = []
    if bracket_stack:
        warning_reasons.append("unclosed brackets in expressions")
    if string_opened_with:
        warning_reasons.append("unterminated string in expressions")
    if in_escape:
        warning_reasons.append("unexpected escape character at end of expressions")
    if warning_reasons:
        warning_message = "; ".join(warning_reasons)
        warning_message = warning_message[0].upper() + warning_message[1:]
        warnings.warn(warning_message, ExpressionsMisparseWarning)

    expression_list.append(expressions[slice_start:])
    return list(filter(bool, map(str.lstrip, expression_list)))


def generate_error_casting_entity_message(entity_name: str):
    """Generic error message to include if failure during casting
    of entity during data contract phase
    """
    return FeedbackMessage(
        entity_name,
        failure_type="integrity",
        record={},
        error_type="no_records",
        category="Blank",
        error_message=(
            "Error in converting to correct types\n"
            "This can be caused by data that is not structured correctly"
        ),
        error_code="",
        value="",
        reporting_field=entity_name,
    )


def _get_non_heterogenous_type(types: Sequence[type]) -> type:
    """Get a single type from a sequence of types (e.g. from a list or union),
    raising an error if the types are hetereogenous (though nullable types
    are permitted).

    """
    type_list = list(types)
    try:
        type_list.remove(type(None))
    except ValueError:
        pass

    n_types = len(type_list)
    if n_types == 0:
        raise ValueError(f"Types must contain at least one non-null type (got {types!r})")
    if n_types != 1:
        raise ValueError(
            "Set of types must be non-heterogenous: Spark does not support "
            + f"union types (got {type_list!r}) but nullable types are okay"
        )
    return type_list[0]
