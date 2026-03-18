from typing import Tuple, Callable, Union

from pyspark.sql.functions import col, length

from dsp.dam.dq_errors import DQErrs

is_numeric = (
    lambda c1: (col(c1).rlike(r'^\s?\d*\s?$')),
    DQErrs.DQ_FMT_numeric
)

is_alpha_numeric = (
    lambda c1: col(c1).rlike(r'^[a-zA-Z0-9]*$'),
    DQErrs.DQ_FMT_alpha_numeric
)


def format_check(expected_format: str, dq_message: DQErrs):
    """

    Args:
        expected_format: RegEx
        dq_message: Specific dq message from DQErrs.DIDSxxx

    Returns: If string does not match RegEx then dq error is returned

    """
    return (
        lambda c1: col(c1).rlike(expected_format), dq_message
    )


def length_check(min_size: Union[int, float] = 0, max_size: Union[int, float] = float('inf'),
                 dq_message: DQErrs = DQErrs.DQ_FMT_length):
    return (
        lambda c1: ((min_size <= length(c1)) & (length(c1) <= max_size)), dq_message
    )


def allow_empty(inner_condition: Tuple[Callable[[str], col], DQErrs]) -> Tuple[Callable[[str], col], DQErrs]:

    inner, message = inner_condition

    return (
        lambda c1: ((col(c1).isNull()) | (col(c1) == '')) | inner(c1), message
    )


def allow_empty_multiple(inner_condition: Tuple[Callable[[str, str], col], DQErrs]) -> Tuple[Callable[[str, str], col],
                                                                                             DQErrs]:

    inner, message = inner_condition

    return (
        lambda c1, c2: ((col(c1).isNull()) | (col(c1) == '')) | inner(c1, c2), message
    )

