from typing import Callable, Tuple, Union, FrozenSet, Iterable

from pyspark.sql import Column
from pyspark.sql.functions import (
    col,
    concat,
    create_map,
    lit,
    struct,
    to_date,
    when
)
from pyspark.sql.types import StringType, DateType

from dsp.common.relational import JoinChain, TableField
from nhs_reusable_code_library.resuable_codes.spark_helpers import normalise, empty_array
from dsp.datasets.common import Fields as Common, DQMessageType
from dsp.datasets.definitions.mhsds.mhsds_v6.submission_constants import MHS000, Metadata
from dsp.dq_files.output import Fields as DQFields
from dsp.validations.common import DateDelta
from dsp.validations.mhsds_v6.person_score_validations import (
    is_valid_assessment_scale_code,
    is_valid_assessment_scale_score,
)
from dsp.validations.mhsds_v6.validation_messages import (
    ValidationDetail,
    ValidationSeverity,
    get_validation_detail,
)
from dsp.validations.validation_rules import ValidationRule, DASHED_YYYYMMDD, Validation, ValidationRuleType
import dsp.validations.common_validation_functions as v_func

# from dsp.validations.common_validation_functions import (
#     date_before_or_on_date_rule,
#     date_on_or_after_date_rule,
#     date_inside_reporting_period_rule,
#     placeholder_rule,
#     column_comparison_rule
# )
from dsp.common.regex_patterns import YYYY_MM_DD_T_HH_MM_SS_OFFSET_PATTERN

MAX_PERSON_AGE = 120


def enrich_validation_rule(rule: ValidationRule) -> Validation:
    """
    Enrich a validation rule with the appropriate details
    
    Args:
        rule: The rule to be enriched

    Returns:
        An enriched validation
    """
    return Validation(rule, get_validation_detail(rule.code))


def enrich_validation_rules(*rules: ValidationRule) -> Tuple[Validation]:
    """
    Enrich a sequence of validation rules with the appropriate details
    
    Args:
        *rules: The rules to be enriched

    Returns:
        A tuple of enriched validations
    """
    return tuple(map(enrich_validation_rule, rules))


def column_comparison_rule_mhsds(code: str, target_field: TableField, compare_fields: Tuple[TableField, ...],
                           comparison_operator: Callable[..., Column]) -> ValidationRule:
    """
    Shorthand for column_comparison_rule() which includes MHS000 as the header table

    Args:
        code (str): The unique code of the validation rule
        target_field (TableField): The target field of the validation rule
        compare_fields (Tuple[TableField, ...]): The fields to compare the target against
        comparison_operator (Callable[..., Column]): The comparison operation to apply between the target and
            compared fields

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    return v_func.column_comparison_rule(code, target_field, compare_fields, comparison_operator,
                                                 header_table=MHS000, metadata_table=Metadata)


def is_valid_datetime_column_rule(code: str, table_name: str, column: Union[str, Column],
                                  check_timestamp_regex: bool = True) -> ValidationRule:
    """
    Retrieve a ValidationRule to determine whether a given datetime value is valid. The TOS specifies that all
    datetime values must be no earlier than January 1st 1890.

    Args:
        code (str): The unique code for this validation
        table_name (str): The table to apply the validation against
        column (Union[str, Column]): The column to apply the validation against
        check_timestamp_regex (bool): Flag to check datetime regex when passing a string

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """

    def expr():
        column_normalized = normalise(column)
        date_column = column_normalized.cast(DateType())

        if check_timestamp_regex:
            # Check date is after 01-01-1890 && Regex check for YYYY-MM-DDTHH:mm:ssZ pattern.
            is_valid_timestamp_pattern = column_normalized.rlike(YYYY_MM_DD_T_HH_MM_SS_OFFSET_PATTERN)

            valid_datetime = (when(is_valid_timestamp_pattern,
                                   date_column >= to_date(lit('1890-01-01'), format='yyyy-MM-dd'))
                              .otherwise(False))
        else:
            # Check date is after 01-01-1890.
            valid_datetime = date_column >= to_date(lit('1890-01-01'), format='yyyy-MM-dd')

        return column_normalized.isNull() | valid_datetime

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.IS_VALID_DATETIME_COLUMN_RULE)


def is_valid_time_column_rule(code: str, table_name: str, column: Union[str, Column]):
    """
    The accdb in which the data comes enforces a format on the time columns via its IDB schema.
    Thus the time fields cannot be in the wrong format and so do not need validation.
    In fact, the accdb exports time fields with a dummy date prepended (07:26:15 gets exported as 1899-12-30 07:26:15)
    so a note that if we did validate it here, it would be necessary to ignore the prepended date.

    Args:
        code (str): The unique code for this validation
        table_name (str): The table to apply the validation against
        _ (Union[str, Column]): The column to apply the validation against

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    return v_func.placeholder_rule(code, table_name, "No restrictions on time validity")


def is_valid_assessment_scale_code_rule(code: str, field: TableField) -> ValidationRule:
    """
    Retrieve a ValidationRule to determine whether a given field contains a valid SNOMED assessment code for tables
    MHS606, MHS607 and MHS608

    Args:
        code (str): The unique code for this validation
        field (TableField): The field to be validated

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    return ValidationRule(
        code,
        field.table.name,
        lambda: is_valid_assessment_scale_code(col(field.qualified_name)),
        rule_type=ValidationRuleType.IS_VALID_ASSESSMENT_SCALE_CODE_RULE
    )


def is_valid_assessment_scale_score_rule(code: str, code_field: TableField, score_field: TableField) -> ValidationRule:
    """
    Retrieve a ValidationRule to determine whether a given score is valid for the given SNOMED assessment code for
    tables MHS606, MHS607 and MHS608

    Args:
        code (str): The unique code for this validation
        code_field (TableField): The field containing the SNOMED assessment code
        score_field (TableField): The field containing the assessment score to be validated

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    return ValidationRule(code, score_field.table.name,
                          lambda: is_valid_assessment_scale_score(col(code_field.qualified_name),
                                                                  col(score_field.qualified_name)),
                          rule_type=ValidationRuleType.IS_VALID_ASSESSMENT_SCALE_SCORE_RULE)


def date_check_expr(first_date, second_date):
    return to_date(first_date, DASHED_YYYYMMDD).isNull() | to_date(second_date, DASHED_YYYYMMDD).isNull()


def mark_if_date_is_inside_the_reporting_period(code: str, fields: Iterable[TableField],
                                                offset: DateDelta = DateDelta()) -> ValidationRule:
    """
    Retrieve a ValidationRule that marks row for which any of the provided fields is inside (including borders) of the
    reporting period.

    Args:
        code (str): Internal validation code
        fields (Iterable[TableFields]): fields containing the dates to be validated
        offset (DateDelta): added to reporting period end date before comparison

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    header_table = MHS000
    reporting_period_columns = (MHS000['ReportingPeriodStartDate'], MHS000['ReportingPeriodEndDate'])
    return v_func.date_inside_reporting_period_rule(code, fields, header_table, reporting_period_columns, offset)


def severity_str(severity: int) -> str:
    """
    Adapt a ValidationSeverity code into a string to report back to end users

    Args:
        severity (int): A ValidationSeverity code

    Returns:
        str: The string representation of the given severity code

    Raises:
        ValueError: If the given code does not represent a valid severity
    """
    if severity == ValidationSeverity.WARNING:
        return "Warning"
    if severity == ValidationSeverity.REJECT_RECORD:
        return "Record rejected"
    if severity == ValidationSeverity.REJECT_GROUP:
        return "Group rejected"
    if severity == ValidationSeverity.REJECT_FILE:
        return "File rejected"

    raise ValueError("No severity associated with code {}".format(severity))


def get_required_joins(*validation_codes: str) -> FrozenSet[JoinChain]:
    """
    Get the joins required in order to populate the validation messages for a given set of validation codes

    Args:
        *validation_codes (str): The validation codes to retrieve join parameters for

    Returns:
        FrozenSet[JoinChain]: A reduced collection of joins required to populate the messages for the given validations
    """
    return JoinChain.combine(*(required_join for validation_code in validation_codes
                               for required_join in get_validation_detail(validation_code).required_joins))


def get_dq_descriptor(validation_code: str, table_name: str) -> Column:
    """
    Retrieve a Spark Column corresponding to a DQ descriptor for the given code

    Args:
        validation_code (str): The code of the validation to describe
        table_name (str): the table name under test

    Returns:
        Column: A column descriptor for the validation
    """
    validation_detail = get_validation_detail(validation_code)

    # fields = array([lit(f.field.name) for f in validation_detail.fields]) \
    #     if validation_detail.fields else empty_array(StringType())

    validation_type = DQMessageType.Warning \
        if validation_detail.severity == ValidationSeverity.WARNING \
        else DQMessageType.Error

    validation_action = 'reject_file' if validation_detail.severity == ValidationSeverity.REJECT_FILE \
        else 'reject_record' if validation_detail.severity != ValidationSeverity.WARNING \
        else None

    return struct(
        lit(table_name).alias(DQFields.TABLE),
        col('RowNumber').astype(StringType()).alias(DQFields.ROW_INDEX),
        lit(None).alias(Common.ATTRIBUTE),  # @TODO: Retrieve from validation detail
        empty_array(StringType()).alias(Common.FIELDS),  # @TODO: Retrieve from validation detail
        lit(validation_code).alias(Common.CODE),
        print_validation_message(validation_detail).alias(Common.MESSAGE),
        report_validation_fields(validation_detail).alias(DQFields.FIELD_VALUES),
        lit(validation_type).alias(Common.TYPE),
        lit(validation_action).alias(DQFields.ACTION)
    )


def print_validation_message(detail: ValidationDetail) -> Column:
    """
    Adapt a MH validation message into a Spark Column, replacing value placeholders enclosed in < and > symbols with
    values from the corresponding row

    Args:
        detail (ValidationDetail): Detail object of the validation to describe

    Returns:
        Column: A Column corresponding to the message
    """
    severity = severity_str(detail.severity)
    message = [lit(severity), lit(" - "), lit(detail.message)]

    return concat(*message)


def report_validation_fields(detail: ValidationDetail) -> Column:
    """
    Store the fields required for the validation message as a map

    Args:
        detail (ValidationDetail): Detail of the validation to be reported

    Returns:
        Column: A column describing the required fields as a map
    """
    return create_map([column for field in detail.fields
                       for column in (lit(field.data_dictionary_name),
                                      col(field.qualified_name).astype(StringType()))])
