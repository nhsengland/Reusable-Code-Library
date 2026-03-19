from typing import Union

from pyspark.sql import Column
from pyspark.sql.functions import col, when, to_date

from dsp.common.regex_patterns import YYYY_MM_DD_PATTERN
from dsp.common.spark_helpers import normalise
from dsp.dam.dq_errors import DQErrs

yyyyMMdd_dashed = (
    # TODO Move the null checks out of the format dq validations
    lambda c1: (col(c1).isNull()) | (col(c1).rlike(r'^[1-9]\d{3}-(?:0[1-9]|1[0-2])-[0-3]\d')),
    DQErrs.DQ_FMT_yyyyMMdd_dashed
)


def date_comparision_expr_with_pattern(first_date:  Union[str, Column], second_date: Union[str, Column]
                                 , pattern: str = YYYY_MM_DD_PATTERN):
    first_date = normalise(first_date)
    second_date = normalise(second_date)
    return (to_date(first_date).isNull() | to_date(second_date).isNull() |
            ~first_date.rlike(pattern) | ~second_date.rlike(pattern))


def valid_date_yyyyMMdd_dashed(dq_message: DQErrs = DQErrs.DQ_INVALID_DATE_yyyyMMdd_dashed):
    return (
        lambda c1: (col(c1).isNull()) | (col(c1).rlike(YYYY_MM_DD_PATTERN)), dq_message
    )