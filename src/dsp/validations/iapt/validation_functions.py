from typing import Tuple, Callable, List

from dsp.common.relational import TableField
from nhs_reusable_code_library.resuable_codes.spark_helpers import normalise
from dsp.datasets.definitions.iapt.submission_constants import IDS000, Metadata
from dsp.validations.common import DateDelta
from dsp.validations.iapt.person_score_validations import (
    is_valid_assessment_scale_code,
    is_valid_assessment_scale_score,
    is_valid_score_format_for_snomed,
    AssessmentToolName
)
from dsp.validations.iapt.validation_messages import get_validation_detail
from dsp.validations.common_validation_functions import (
    date_before_or_on_date_rule,
    date_on_or_after_date_rule,
    placeholder_rule, column_comparison_rule
)
from dsp.validations.validation_rules import ValidationRule, Validation, ValidationRuleType
from typing import Sequence, Union

from pyspark.sql.functions import col, coalesce, collect_list, to_timestamp, lit
from pyspark.sql import Window, Column

from dsp.udfs import (
    UUID,
    GetCountOfCareContactDnaDuplicates
)


def enrich_validation_rule(rule: ValidationRule) -> Validation:
    """
    Enrich an IAPT validation rule with the appropriate details

    Args:
        rule: The rule to be enriched

    Returns:
        An enriched validation
    """
    return Validation(rule, get_validation_detail(rule.code))


def enrich_validation_rules(*rules: ValidationRule) -> Tuple[Validation]:
    """
    Enrich a sequence of IAPT validation rules with the appropriate details

    Args:
        *rules: The rules to be enriched

    Returns:
        A tuple of enriched validations
    """
    return tuple(map(enrich_validation_rule, rules))


def is_valid_assessment_scale_code_rule(
        code: str,
        field: TableField,
        assessment_tool_names: List[AssessmentToolName] = None
) -> ValidationRule:
    """
    Retrieve a ValidationRule to determine whether a given field contains a valid SNOMED assessment code

    Args:
        code (str): The unique code for this validation
        field (TableField): The field to be validated

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    return ValidationRule(
        code, field.table.name,
        lambda: is_valid_assessment_scale_code(col(field.qualified_name), assessment_tool_names),
        rule_type=ValidationRuleType.IS_VALID_ASSESSMENT_SCALE_CODE_RULE
    )


def is_valid_score_format_for_snomed_code(code: str, code_field: TableField, score_field: TableField) -> ValidationRule:
    return ValidationRule(
        code, score_field.table.name,
        lambda: is_valid_score_format_for_snomed(
            code_field.qualified_name, score_field.qualified_name),
        rule_type=ValidationRuleType.IS_VALID_SCORE_FORMAT_FOR_SNOMED_CODE
    )


def is_valid_assessment_scale_score_rule(code: str, code_field: TableField, score_field: TableField) -> ValidationRule:
    """
    Retrieve a ValidationRule to determine whether a given score is valid for the given SNOMED assessment code for
    tables IDS606, IDS607

    Args:
        code (str): The unique code for this validation
        code_field (TableField): The field containing the SNOMED assessment code
        score_field (TableField): The field containing the assessment score to be validated

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    return ValidationRule(
        code, score_field.table.name,
        lambda: is_valid_assessment_scale_score(code_field.qualified_name, score_field.qualified_name),
        rule_type=ValidationRuleType.IS_VALID_ASSESSMENT_SCALE_SCORE_RULE
    )


def uniqueness_rule_iapt_carecontact(
        code: str, table_name: str, unique_columns: Sequence[Union[str, Column]],
        exceptional_column: str
) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records featuring equivalent sets of key columns,
    and only specific sets of values in a nominated column.
    Null values within the key columns are always classed as identical.

    Args:
        code (str): The unique code for the validation rule
        table_name (str): The name of the table to be validated
        unique_columns (Sequence[Union[str, Column]]): The columns to be treated as keys for the purpose of
            validation
        exceptional_column: The column on which to apply the specific value validation.

    Returns:
        ValidationRule: A rule instance representing the given parameters
        """
    assert isinstance(unique_columns, Sequence)

    def expr():
        columns = [normalise(column) for column in unique_columns]
        condition = GetCountOfCareContactDnaDuplicates(
            collect_list(col(exceptional_column)).over(Window.partitionBy(*columns))) <= 1
        return condition

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.UNIQUENESS_RULE_WITH_CONDITION)


def is_valid_time_column_rule(*, code: str,
                              table_name: str,
                              _: Union[str, Column]) -> ValidationRule:
    """
    Return a ValidationRule to confirm if a given datetime value is valid as per TOS.
    The TOS does not specify any conditions in which a time would be considered invalid
    Therefore, the rules returned by this method are simply placeholders.

    Args:
        code (str): The unique code for this validation
        table_name (str): The table to apply the validation against
        _ (Union[str, Column]): The column to apply the validation against

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    return placeholder_rule(code, table_name, "No restrictions on time validity")


def is_valid_datetime_column_rule(*, code: str,
                                  table_name: str,
                                  column: Union[str, Column]) -> ValidationRule:
    """
    Return a ValidationRule to confirm if a given datetime value is valid as per TOS.
    The TOS does not specify any conditions in which a time would be considered invalid
    Therefore, the rules returned by this method are simply placeholders.

    Args:
        code (str): The unique code for this validation
        table_name (str): The table to apply the validation against
        column (Union[str, Column]): The column to apply the validation against

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """

    def expr():
        column_normalized = normalise(column)
        return column_normalized.isNull() | (column_normalized >= to_timestamp(lit('1890-01-01 00:00:00')))

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.IS_VALID_DATETIME_COLUMN_RULE)


def column_comparison_rule_iapt(code: str, target_field: TableField, compare_fields: Tuple[TableField, ...],
                           comparison_operator: Callable[..., Column],
                           rule_type: str = ValidationRuleType.COLUMN_COMPARISON_RULE) -> ValidationRule:
    """
    Shorthand for column_comparison_rule() which includes IDS000 as the header table

    Args:
        code (str): The unique code of the validation rule
        target_field (TableField): The target field of the validation rule
        compare_fields (Tuple[TableField, ...]): The fields to compare the target against
        comparison_operator (Callable[..., Column]): The comparison operation to apply between the target and
            compared fields
        rule_type (str): The ValidationRuleType you want to pass. Defaults to Column_Comparison_Rule

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    return column_comparison_rule(code, target_field, compare_fields, comparison_operator,
                                                 header_table=IDS000, metadata_table=Metadata, rule_type=rule_type)


def iapt_date_before_or_on_date_rule(code: str, field: TableField, later_field: TableField,
                                offset: DateDelta = DateDelta()) -> ValidationRule:
    """
    Calls a common validation function in order to retrieve a ValidationRule to determine whether a date value is not
    after a second date value

    Args:
        code (str): The unique code for this validation
        field (TableField): The field containing the date to be validated
        later_field (TableField): The reference field that must be after (or equal to) the validated field
        offset (DateDelta): added to later_field before comparison

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    return date_before_or_on_date_rule(code, field, later_field, IDS000, Metadata, offset)


def iapt_date_on_or_after_date_rule(code: str, field: TableField, earlier_field: TableField,
                                offset: DateDelta = DateDelta()) -> ValidationRule:
    """
    Calls a common validation function in order to retrieve a ValidationRule to determine whether a date value is not
    after a second date value

    Args:
        code (str): The unique code for this validation
        field (TableField): The field containing the date to be validated
        earlier_field (TableField): The reference field that must be after (or equal to) the validated field
        offset (DateDelta): added to later_field before comparison

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    return date_on_or_after_date_rule(code, field, earlier_field, IDS000, Metadata, offset)
