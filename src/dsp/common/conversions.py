from functools import partial
from typing import Tuple

from pyspark.sql.functions import col, concat_ws, date_format, lit, split, to_date, to_timestamp, when

__all__ = [
    'dt_to_yyyyMMdd_dashed',
    'dt_to_yyyyMMdd',
    'dt_to_ddMMyy',
    'to_decimal',
    'to_str',
    'to_tinyint',
    'to_int',
    'to_comma_separated_str',
    'to_array_from_comma_separated_str',
    'to_date_from_string',
    'to_timestamp_from_time_string',
    'to_timestamp_from_string',
    'to_datetime_from_datetime_string',
]

dt_to_yyyyMMdd_dashed = partial(date_format, format='yyyy-MM-dd')

dt_to_yyyyMMdd = partial(date_format, format='yyyyMMdd')

dt_to_ddMMyy = partial(date_format, format='ddMMyy')


def to_str(column_name):
    return col(column_name).cast('string')


def to_decimal(column_name, precision: Tuple[int, int] = (17, 0)):
    return col(column_name).cast('Decimal({}, {})'.format(*precision))


def to_tinyint(column_name):
    return col(column_name).cast('byte')


def to_bigint(column_name):
    return col(column_name).cast('bigint')


def to_int(column_name):
    return col(column_name).cast('int')


def to_comma_separated_str(column_name):
    return concat_ws(',', col(column_name))


def to_array_from_comma_separated_str(column_name):
    return split(col(column_name), ',')


def to_date_from_string(column_name, format='yyyy-MM-dd'):
    """
    This will return a date from a formated string
    :param column_name: date string with format
    :param format: format of the date field
    :return: date
    """
    return to_date(col(column_name), format=format)


def to_timestamp_from_time_string(column_value):
    """
    This will convert the time value in the column to datetime where the date=1970-01-01 and time=value in columns
    :param column_value: the column that contains the time value
    :return: timestamp value of the string with the date part set as 1st Jan 1970
    """
    return when(column_value.isNull(), None).otherwise(
        to_timestamp(concat_ws(' ', lit('1970-01-01'), column_value)))


def to_timestamp_from_string(column_name, format='yyyy-MM-dd'):
    """
    This will return a timestamp from a formatted string
    :param column_name: datetime string with format
    :param format: format of the datetime field
    :return: datetime
    """
    return to_timestamp(col(column_name), format=format)


def to_datetime_from_datetime_string(column_name):
    """
     This will return a datetime from a string
    :param column_name: the column that contains the datetime value
    :return: datetime value of the string
    """
    return to_timestamp(col(column_name))

