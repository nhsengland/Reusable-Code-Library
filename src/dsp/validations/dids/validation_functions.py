from typing import Union, Callable

from pyspark.sql import Column
from pyspark.sql.functions import trim, lit, col, length

from dsp import udfs
from dsp.common.regex_patterns import YYYY_MM_DD_PATTERN
from nhs_reusable_code_library.resuable_codes.spark_helpers import normalise


def column_not_blank_rule_expr(column: Union[str, Column]):
    """
    Validates if given column as null value
    Args:
        column: column to be checked.

    Returns:
        true if column is not null or not empty.
    """
    return normalise(column).isNotNull() & (trim(normalise(column).cast('string')) != lit(""))


def is_active_with_condition(code: Union[str, Column, list], point_in_time, condition: Callable, offset = None):
    """
    evaluates the given condition with point in time
    Args:
        code: code to evaluate
        point_in_time: date to check against
        condition: condition to evaluate

    Returns:
        True if condition is valid else False
    """

    if not offset:
        return (
                (~column_not_blank_rule_expr(code)) |
                (~column_not_blank_rule_expr(point_in_time)) |
                (~col(point_in_time).rlike(YYYY_MM_DD_PATTERN)) |
                condition(col(code), col(point_in_time))
        )
    else:
        return (
                (~column_not_blank_rule_expr(code)) |
                (~column_not_blank_rule_expr(point_in_time)) |
                (~col(point_in_time).rlike(YYYY_MM_DD_PATTERN)) |
                condition(col(code), col(point_in_time), lit(offset))
        )


def alpha_numeric(code: Union[str, Column], min_size: Union[int, float] = 0, max_size: Union[int, float] = float('inf')):

    return col(code).rlike(r'^[a-zA-Z0-9]*$') \
           & min_size <= length(code) <= max_size


    #(length(code).cast('string')) <= max_size
    #& min_size <= length(code) <= max_size


def length_check_an(column: Union[str, Column], min_size: Union[int, float] = 0, max_size: Union[int, float] = float('inf')):

    column = normalise(column)
    return (column).rlike(r'^[a-zA-Z0-9]*$') & ((min_size <= length(column)) & (length(column) <= max_size))

