# noinspection PyPackageRequirements
from typing import Union

from pyspark.sql import DataFrame, Column
# noinspection PyPackageRequirements
from pyspark.sql.functions import col, upper as psupper, regexp_replace, substring, when as pswhen, lit, \
    to_timestamp, split

from nhs_reusable_code_library.resuable_codes.spark_helpers import normalise
from dsp.datasets.postcode.constants import NHSLFDTestsConstants
from dsp.datasets.postcode.shared.derivations import SharedDerivationFields


def clean_uppercase_transform(x: str) -> Column: return regexp_replace(psupper(col(x)), '[ "\'_,-]', '')


def clean_email(x: str) -> Column: return psupper(regexp_replace(regexp_replace(x, ' ', ''), r'(\+\p{ASCII}*)(@)', '@'))


def clean_gender(x: str) -> Column:
    return pswhen(psupper(col(x)) == lit('MALE'), '1').when(psupper(col(x)) == lit('FEMALE'), '2').otherwise(col(x))


def clean_mobile_number(x: str) -> Column:
    return substring(regexp_replace(col(x), '[ -]', ''), -7, 7)


def _cleaned_column(df, col_name, transform, new_col_name=None):
    if new_col_name is None:
        new_col_name = col_name

    return df.withColumn(new_col_name, transform(col_name))


def cleaned_email(df: DataFrame, col_name=SharedDerivationFields.MPSEmailAddress, new_col_name=None):
    return _cleaned_column(df, col_name, clean_email, new_col_name=new_col_name)


def cleaned_first_name(df: DataFrame, col_name=SharedDerivationFields.MPSFirstName, new_col_name=None):
    return _cleaned_column(df, col_name, clean_uppercase_transform, new_col_name=new_col_name)


def cleaned_gender(df: DataFrame, col_name=SharedDerivationFields.MPSGender, new_col_name=None):
    return _cleaned_column(df, col_name, clean_gender, new_col_name=new_col_name)


def cleaned_middle_name(df: DataFrame, col_name, new_col_name=None):
    return _cleaned_column(df, col_name, clean_uppercase_transform, new_col_name=new_col_name)


def cleaned_last_name(df: DataFrame, col_name=SharedDerivationFields.MPSLastName, new_col_name=None):
    return _cleaned_column(df, col_name, clean_uppercase_transform, new_col_name=new_col_name)


def cleaned_mobile_number(df: DataFrame, col_name=SharedDerivationFields.MPSLastName, new_col_name=None):
    return _cleaned_column(df, col_name, clean_mobile_number, new_col_name=new_col_name)


def is_valid_uk_date_format(column: Union[str, Column], slash_format: bool = False):
    if slash_format:
        return is_valid_date_format(column,
                                    NHSLFDTestsConstants.UK_DATE_SLASH_FORMAT,
                                    r'^(0[1-9]|[1-2][0-9]|3[0-1])\/(0[1-9]|1[0-2])\/[1-9][0-9]{3}')

    return is_valid_date_format(column,
                                NHSLFDTestsConstants.UK_DATE_FORMAT,
                                r'^[1-9][0-9]{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])')


def is_valid_us_date_format(column: Union[str, Column], slash_format: bool = False):
    if slash_format:
        return is_valid_date_format(column,
                                    NHSLFDTestsConstants.US_DATE_SLASH_FORMAT,
                                    r'^(0[1-9]|1[0-2])\/(0[1-9]|[1-2][0-9]|3[0-1])\/[1-9][0-9]{3}')

    return is_valid_date_format(column,
                                NHSLFDTestsConstants.US_DATE_FORMAT,
                                r'^[1-9][0-9]{3}-(0[1-9]|[1-2][0-9]|3[0-1])-(0[1-9]|1[0-2])')


def is_valid_uk_date_format_yy(column: Union[str, Column], slash_format: bool = False):
    if slash_format:
        return is_valid_date_format(column,
                                    NHSLFDTestsConstants.UK_DATE_SLASH_FORMAT_YY,
                                    r'^(0[1-9]|[1-2][0-9]|3[0-1])\/(0[1-9]|1[0-2])\/2[0-9]$')

    return is_valid_date_format(column,
                                NHSLFDTestsConstants.UK_DATE_FORMAT_YY,
                                r'^(0[1-9]|[1-2][0-9]|3[0-1])-(0[1-9]|1[0-2])-2[0-9]$')


def is_valid_uk_date_format_single_digit(column: Union[str, Column], slash_format: bool = False):
    if slash_format:
        return is_valid_date_format(column,
                                    NHSLFDTestsConstants.UK_DATE_SLASH_FORMAT_YY,
                                    r'^([1-9]|[1-2][0-9]|3[0-1])\/([1-9]|1[0-2])\/2[0-9]{3}')

    return is_valid_date_format(column,
                                NHSLFDTestsConstants.UK_DATE_FORMAT_YY,
                                r'^([1-9]|[1-2][0-9]|3[0-1])-([1-9]|1[0-2])-2[0-9]{3}')


def is_valid_uk_date_format_hyphen(column: Union[str, Column]):
    return is_valid_date_format(column,
                                NHSLFDTestsConstants.UK_DATE_HYPHEN_FORMAT,
                                r'^(0[1-9]|[1-2][0-9]|3[0-1])-(0[1-9]|1[0-2])-[1-9][0-9]{3}')


def is_valid_date_format(column: Union[str, Column], parse_format: str, regex_check: str):
    column_normalized = normalise(column)
    matches_valid_date_regex = column_normalized.rlike(regex_check)
    convert_to_date = to_timestamp(column_normalized, parse_format).isNotNull()
    condition = matches_valid_date_regex & convert_to_date

    return column_normalized.isNotNull() & condition


def postcode_outcode(df: DataFrame, normalised_postcode_column: Union[str, Column],
                     outcode_column: Union[str, Column]) -> DataFrame:
    return df.withColumn(outcode_column, split(col(normalised_postcode_column), ' ').getItem(0))
