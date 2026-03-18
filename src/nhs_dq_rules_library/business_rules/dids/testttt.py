from datetime import datetime, timedelta, date
from itertools import chain
from operator import le, ge, ne, gt, lt
from typing import Set, AbstractSet, Any, Union, Tuple, Iterator, Callable, Sequence, List, Iterable, FrozenSet, Optional

from pyspark.sql.types import Row, DateType
from pyspark.sql import SparkSession, Column, Window
from pyspark.sql.functions import hour, minute, second, array_contains, size, array_intersect
from pyspark.sql.dataframe import DataFrame
from itertools import groupby

from dsp.validation.validation_rules import _RulePrecondition

from dsp.datasets.common import Fields as Common
from dsp.datasets.common import DQMessageType
from dsp.datasets.fields.dq.output import Fields as DQFields
from dsp.common.relational import JoinChain, TableField, Table, JoinParameters
from dsp.common.spark_helpers import normalise, all_conditions, chain_joins
from dsp.datasets.validations.common import DateDelta
from dsp.validation.validation_rules import DASHED_YYYYMMDD, ValidationRuleType, ValidationRule
from dsp.common.regex_patterns import TIME_PATTERN, YYYY_MM_DD_T_HH_MM_SS_PATTERN
from dsp.udfs import (
    DefaultOrgValid,
    GPPracticeCodeValid,
    OrganisationExists,
    OrganisationExistsIgnoringSuccession,
    ProPlusStatusCodeIsValid,
    SiteCodeValid,
    SiteCodeValidIgnoringSuccession,
    PostcodeInReferenceData,
    SchoolValid,
    is_valid_nhs_number,
    DefaultOrgValidAnytimeWithoutSuccessor,
    UUID,
    FindCodeIsValid,
    IcdSnomedFindCodeIsValid,
    IsValidSnomedCT,
    ObsCodeIsValid,
    ProCodeIsValid,
    DiagCodeIsValid,
    IcdSnomedDiagCodeIsValid,
    AgeAtEvent,
)
from pyspark.sql.functions import (
    when, col, udf, count, trim, lit, concat, array, array_remove, add_months, date_add, to_date, coalesce, struct,
    year, month
)
from pyspark.sql.types import BooleanType, StringType, IntegerType

from dsp.datasets.definitions.csds_v1_6.submission_constants import \
    CYP000 as CSDS_v1_6_Header_Table, Metadata as CSDS_v1_6_Metadata, CYP001 as CSDS_v1_6_Table_001, CYP101 as CSDS_v1_6_Table_101
from dsp.datasets.definitions.iapt_v2_1.submission_constants import \
    IDS000 as IAPT_V2_1_Header_Table, Metadata as IAPT_V2_1_Metadata, IDS001 as IAPT_V2_1_Table_001, \
    IDS101 as IAPT_V2_1_Table_101
from dsp.datasets.definitions.mhsds_v6.submission_constants import \
    MHS000 as MHSDS_V6_Header_Table, Metadata as MHSDS_V6_Metadata, MHS001 as MHSDS_V6_Table_001, \
    MHS101 as MHSDS_V6_Table_101
from dsp.datasets.definitions.msds.submission_constants import \
    MSD000 as MSDS_Header_Table, Metadata as MSDS_Metadata, MSD001 as MSDS_Table_001, MSD101 as MSDS_Table_101
from dsp.shared.constants import DS

MAX_PERSON_AGE = 120

SAME_END_DATE_DATASET = [
    DS.MHSDS_V6
]


def get_tables_by_dataset(dataset: str, table_type: str):
    constants = {
        DS.CSDS_V1_6: {"header_table": CSDS_v1_6_Header_Table, "metadata": CSDS_v1_6_Metadata,
                  "table_001": CSDS_v1_6_Table_001,
                  "table_101": CSDS_v1_6_Table_101},
        DS.MSDS: {"header_table": MSDS_Header_Table, "metadata": MSDS_Metadata, "table_001": MSDS_Table_001,
                  "table_101": MSDS_Table_101},
        DS.IAPT_V2_1: {"header_table": IAPT_V2_1_Header_Table, "metadata": IAPT_V2_1_Metadata,
                       "table_001": IAPT_V2_1_Table_001, "table_101": IAPT_V2_1_Table_101},
        DS.MHSDS_V6: {"header_table": MHSDS_V6_Header_Table, "metadata": MHSDS_V6_Metadata,
                      "table_001": MHSDS_V6_Table_001, "table_101": MHSDS_V6_Table_101},
    }

    tables = constants.get(dataset)
    table = tables.get(table_type)
    return table


def is_valid_time_column_rule(
        code: str, table_name: str, column: Union[str, Column]
) -> ValidationRule:
    """
    Return a ValidationRule to confirm if a given string time value is valid
    as per TOS.

    Args:
        code (str): The unique code for this validation
        table_name (str): The table to apply the validation against
        column (Union[str, Column]): The column to apply the validation
        against

    Returns:
        ValidationRule: A validation rule corresponding to the given
        parameters
    """

    def expr():
        # regex check for hh:mm:ss time pattern:
        column_normalized = normalise(column)
        return column_normalized.isNull() | column_normalized.rlike(TIME_PATTERN)

    return ValidationRule(
        code, table_name, expr, rule_type=ValidationRuleType.IS_VALID_TIME_COLUMN_RULE)


def is_valid_datetime_column_rule(
        code: str, table_name: str, column: Union[str, Column]) -> ValidationRule:
    """
    Return a ValidationRule to confirm if a given datetime value is valid.

    Args:
        code (str): The unique code for this validation
        table_name (str): The table to apply the validation against
        column (Union[str, Column]): The column to apply the validation against

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """

    def expr():
        column_normalized = normalise(column)

        # regex check for YYYY-MM-DDThh:mm:ss pattern:
        is_valid_datetime_pattern = column_normalized.rlike(YYYY_MM_DD_T_HH_MM_SS_PATTERN)

        # check if date is after 01-01-1890:
        date_column = column_normalized.cast(DateType())
        is_after_01_01_1890 = (when(is_valid_datetime_pattern,
                                    date_column >= to_date(lit('1890-01-01'), format='yyyy-MM-dd')).otherwise(False))

        return column_normalized.isNull() | (is_after_01_01_1890 & is_valid_datetime_pattern)

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.IS_VALID_DATETIME_COLUMN_RULE)


def date_inside_of_reporting_period_rule(
        code: str, field: TableField, rp_start_date: TableField, rp_end_date: TableField, header_table: Table,
        metadata: Table = None, ) -> ValidationRule:
    """
    Retrieve a ValidationRule to determine whether a value is not outside of the reporting period of the submission

    Args:
        rp_end_date:
        rp_start_date:
        header_table: header table to use
        metadata_table: Optional parameter, default set to None. Required for comparison rules that use metadata.
        code (str): The unique code for this validation
        field (TableField): The field containing the date to be validated

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """

    def compare(date_field: Column, rep_start_date: Column, rep_end_date: Column) -> Column:
        return date_field.isNull() \
            | ((to_date(rep_start_date) <= to_date(date_field))
               & (to_date(date_field) <= to_date(rep_end_date)))

    return column_comparison_rule_with_header_table(code, field, (rp_start_date, rp_end_date), compare, header_table,
                                                    metadata,
                                                    ValidationRuleType.DATE_INSIDE_OF_REPORTING_PERIOD_RULE)


def date_check_expr(first_date, second_date):
    return to_date(first_date, DASHED_YYYYMMDD).isNull() | to_date(second_date, DASHED_YYYYMMDD).isNull()


def datetime_check_expr(first_date, second_date):
    return first_date.isNull() | second_date.isNull()


def date_before_or_on_date_rule(
        code: str,
        field: TableField,
        later_field: TableField,
        header_table: Optional[Table] = None,
        metadata: Optional[Table] = None,
        offset: DateDelta = DateDelta()
) -> ValidationRule:
    """
        Retrieve a ValidationRule to determine whether a date value is not after a second date value

        Args:
            header_table: header table to use
            code (str): The unique code for this validation
            field (TableField): The field containing the date to be validated
            later_field (TableField): The reference field that must be after (or equal to) the validated field
            metadata_table: Optional parameter, default set to None. Required for comparison rules that use metadata.
            offset (DateDelta): added to later_field before comparison

        Returns:
            ValidationRule: A validation rule corresponding to the given parameters
    """

    return column_comparison_rule_with_header_table(
        code,
        field,
        (later_field,),
        date_comparison(comp_operator=le, offset=offset, additional_date_check_expr=date_check_expr),
        header_table=header_table, metadata=metadata, rule_type=ValidationRuleType.DATE_BEFORE_OR_ON_DATE_RULE
    )


def date_before_specified_date(code: str, table_name: str, column: Union[str, Column], date_to_compare: str):
    """
    Checks if the date value from the required column is not before the specified date value.
    :param code: Validation Code
    :param table_name: Name of the table
    :param column: input column name for which date value needs to be compared
    :param date_to_compare: date value to check against
    :return: true if the column is none, or column value is greater than specified date value
    """

    def expr():
        column_normalized = normalise(column)
        return column_normalized.isNull() | (column_normalized >= to_date(lit(date_to_compare)))

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.IS_VALID_DATETIME_COLUMN_RULE)


def date_on_or_after_date_rule(
        code: str,
        field: TableField,
        earlier_field: TableField,
        header_table: Optional[Table] = None,
        metadata: Optional[Table] = None,
        offset: DateDelta = DateDelta()
) -> ValidationRule:
    """
        Retrieve a ValidationRule to determine whether a date value is not before a second date value

        Args:
            code (str): The unique code for this validation
            field (TableField): The field containing the date to be validated
            earlier_field (TableField): The reference field that must be before (or equal to) the validated field
            header_table: header table to use
            metadata_table: Optional parameter, default set to None. Required for comparison rules that use metadata.
            offset (DateDelta): added to earlier_field before comparison

        Returns:
            ValidationRule: A validation rule corresponding to the given parameters
    """

    return column_comparison_rule_with_header_table(
        code,
        field,
        (earlier_field,),
        date_comparison(comp_operator=ge, offset=offset, additional_date_check_expr=date_check_expr),
        header_table=header_table, metadata=metadata, rule_type=ValidationRuleType.DATE_ON_OR_AFTER_DATE_RULE
    )


def column_comparison_rule_with_header_table(
        code: str,
        target_field: TableField,
        compare_fields: Tuple[TableField, ...],
        comparison_operator: Callable[..., Column],
        header_table: Table,
        metadata: Table = None,
        rule_type: ValidationRuleType = ValidationRuleType.COLUMN_COMPARISON_RULE
) -> ValidationRule:
    """
    Shorthand for column_comparison_rule() which includes header table

    Args:
        code (str): The unique code of the validation rule
        target_field (TableField): The target field of the validation rule
        compare_fields (Tuple[TableField, ...]): The fields to compare the target against
        comparison_operator (Callable[..., Column]): Comparison operation to apply between target and compared fields
        header_table: header table to use
        metadata_table: Optional parameter, default set to None. Required for comparison rules that use metadata.
        rule_type (str): The ValidationRuleType you want to pass. Defaults to Column_Comparison_Rule

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    return column_comparison_rule(
        code,
        target_field,
        compare_fields,
        comparison_operator,
        header_table=header_table,
        metadata_table=metadata,
        rule_type=rule_type
    )


def column_comparison_rule(
        code: str,
        target_field: TableField,
        compare_fields: Tuple[TableField, ...],
        comparison_operator: Callable[..., Column],
        header_table: Table = None,
        metadata_table: Table = None,
        required_joins: FrozenSet['JoinChain'] = None,
        rule_type: ValidationRuleType = ValidationRuleType.COLUMN_COMPARISON_RULE
) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records in accordance to a comparison between the
    target field and other fields within the dataset

    Args:
        code (str): The unique code of the validation rule
        target_field (TableField): The target field of the validation rule
        compare_fields (Tuple[TableField, ...]): The fields to compare the target against
        comparison_operator (Callable[..., Column]): Comparison operation to apply between target and compared fields
        header_table (Table): The header table for the dataset which can be accessed from any row
        metadata_table (Table): The metadata table for this dataset that can be joined to any other table
        required_joins (JoinChain): The required table joins for this validation rule
        rule_type: type of ValidationRuleType default to COLUMN_COMPARISON_RULE

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """

    def expr():
        return comparison_operator(
            col(target_field.qualified_name),
            *(col(compare_field.qualified_name) for compare_field in compare_fields)
        )

    required_joins = required_joins or JoinChain.combine(
        *(join_chain
          for compare_field in compare_fields
          for join_chain in
          target_field.table.get_required_joins(
              compare_field.table,
              header_table=header_table,
              metadata_table=metadata_table)
          )
    )

    return ValidationRule(
        code,
        target_field.table.name,
        expr,
        *required_joins,
        rule_type=rule_type
    )


def is_valid_record(dq_column_name: str) -> Column:
    """
    Retrieve an expression for identifying whether a row has passed validation

    Args:
        dq_column_name: Name of column containing array of DQ messages

    Returns:
        Column expression which returns a boolean identifying whether the row applied to is valid
    """
    return ~array_contains(col('{}.{}'.format(dq_column_name, DQFields.TYPE)), DQMessageType.Error)


def is_valid_date_column_rule(code: str, table_name: str, column: Union[str, Column]) -> ValidationRule:
    """
    Retrieve a ValidationRule to determine whether a given date value is valid. Dates are transmitted as datetime
    values. The TOS specifies that all date values must be no earlier than January 1st 1890, and the given time must be
    precisely midnight.

    Args:
        code (str): The unique code for this validation
        table_name (str): The table to apply the validation against
        column (Union[str, Column]): The column to apply the validation against

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """

    def expr():
        column_normalized = normalise(column)
        return (column_normalized.isNull()
                | ((column_normalized >= datetime(1890, 1, 1))
                   & (hour(column_normalized) == 0)
                   & (minute(column_normalized) == 0)
                   & (second(column_normalized) == 0)))

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.IS_VALID_DATE_COLUMN_RULE)


YYYY_MM_DD_PATTERN_NON_STRICT = r"""(?x)
    ^ (
        ( # non-leap days
            \d{4} # year
            -
            (
                (0[13578] | 1[02]) - (0[1-9] | [1-2][0-9] | 3[01]) | # long months - any day
                (0[469] | 11) - (0[1-9] | [1-2][0-9] | 30) |         # short months - any day
                02 - ([01][0-9] | 2[0-8])                   # February - day in non-leap year
            )
        ) |
        ( # leap day
            (
                \d{2} (                                     # regular leap years: dividable by 4, but not by 100
                            0[48] |
                            [2468][048] |
                            [13579][26]
                ) |
                2000                                        # multiple of 400 leap years
            )
            - 02 - 29                                       # February 29th
        )
    ) """


def get_rows_without_consecutive_gp_registrations(
        gp_dataframe: DataFrame,
        patient_id_column: Union[Column, str],
        gp_id_column: Union[Column, str],
        start_date_column: str,
        end_date_column: str,
        row_number_column: Union[Column, str],
        dataset: str = None
) -> Set[Any]:
    """
    Analyse a GP-Table-Like Dataframe for whether each entry with a registration end date has a
    consecutive registration start date with a new GP Org Code.

    Args:
        gp_dataframe (DataFrame): The input GP-like dataframe to be evaluated
        patient_id_column (Union[Column, str]): The Local Patient ID column in that dataframe
        gp_id_column (Union[Column, str]): The GP Organisation Code column in that dataframe
        start_date_column (str): The registration start date column in that dataframe
        end_date_column (str): The registration end date column in that dataframe
        row_number_column (Union[Column, str]): The row number column in that dataframe
        dataset (str): the name of the dataset

    Returns:
        Set: The set of row_numbers of the columns that should receive a warning for this condition

    """

    patient_id_column_normalised = normalise(patient_id_column)
    gp_id_column_normalised = normalise(gp_id_column)
    row_number_column_normalised = normalise(row_number_column)

    def safe_to_date(df: DataFrame, column_name: str) -> Column:
        column = normalise(column_name)

        for field in df.schema.fields:
            if field.name == column_name:
                if field.dataType.typeName() == 'string':
                    return when(column.rlike(YYYY_MM_DD_PATTERN_NON_STRICT),
                                to_date(column, DASHED_YYYYMMDD)).otherwise(None)

        return column.cast('date')

    rdd_grouped = gp_dataframe.select(
        patient_id_column_normalised,
        struct(
            gp_id_column_normalised.alias("GP_OrgID"),
            safe_to_date(gp_dataframe, start_date_column).alias("StartDate"),
            safe_to_date(gp_dataframe, end_date_column).alias("EndDate"),
            row_number_column_normalised.alias("RowNumber"),
            col(f"{Common.DQ}.{DQFields.TYPE}").alias("Warnings"),
        ).alias("StartDateEndDatePlusWarnings"),
    ).rdd


    rows_with_warnings = rdd_grouped.groupByKey().map(
            lambda x: _rows_with_non_consecutive_registration_dates(x, dataset in SAME_END_DATE_DATASET)
        ).collect()

    rows_with_warnings = set(chain(*rows_with_warnings))

    return rows_with_warnings




def _rows_with_non_consecutive_registration_dates(
        registration_dates_and_warnings_by_local_patient_id: Tuple[str, Sequence[Row]], use_same_end_date: bool = False
) -> Sequence[Any]:
    """
    Helper function that maps an rdd that is grouped by local patient id to a list of the row
    numbers that do not meet the consecutive end date start date rule.

    Args:
        registration_dates_and_warnings_by_local_patient_id
        (Tuple[str, Sequence[Row]]): Row in an rdd grouped by local patient id.
        requires_distinct_gp_id: Whether or not subsequent registrations require distinct gp org ids
        use_same_end_date (bool): boolean flag to allow for same start and end date

    Returns:
        Sequence[Any]
    """
    lpi, \
    registration_dates_and_existing_warnings = registration_dates_and_warnings_by_local_patient_id
    rows_for_local_patient_id = [
        (row["GP_OrgID"], row["StartDate"], row["EndDate"], row["RowNumber"], row["Warnings"],)
        for row in registration_dates_and_existing_warnings
    ]

    start_date_org_id_pair = [
        (row[0], row[1]) for row in rows_for_local_patient_id if DQMessageType.Error not in row[4]
    ]

    rows_with_warnings = []

    for org_id, start_date, end_date, row_number, _ in rows_for_local_patient_id:
        if not end_date:
            continue

        end_date_plus_one = end_date + timedelta(days=1)

        if use_same_end_date:
            matching_start_date_org_id_pairs = [p for p in start_date_org_id_pair if p[1] == end_date_plus_one or p[1] == end_date]
        else:
            matching_start_date_org_id_pairs = [p for p in start_date_org_id_pair if p[1] == end_date_plus_one]

        if not matching_start_date_org_id_pairs:
            rows_with_warnings.append(row_number)
        else:
            for matching_pair in matching_start_date_org_id_pairs:
                if matching_pair[0] == org_id:
                    rows_with_warnings.append(row_number)

    return rows_with_warnings

def is_intersection_between_column_list_and_literal_list(list_to_check_col, list_to_check_against):
    """
    Checks if there is any common value between two lists.

    Args:
        list_to_check_col: column holding list to check
        list_to_check_against: literal list to check against

    Returns:
    True if there are any common values, else False
    """
    list_to_check_against_col = array(*[lit(role) for role in list_to_check_against])

    return size(array_intersect(list_to_check_col, list_to_check_against_col)) > 0


def values_are_equal_rule(code: str, target_field: TableField, compare_field: TableField, dataset: str) \
        -> ValidationRule:
    """
    Shorthand for column_comparison_rule() which includes CYP000 as the header table

    Args:
        code (str): The unique code of the validation rule
        target_field (TableField): The target field of the validation rule
        compare_field: The fields to compare the target against
        dataset: The dataset name

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    header_table = get_tables_by_dataset(dataset, "header_table")
    metadata = get_tables_by_dataset(dataset, "metadata")

    return column_comparison_rule(code, target_field, (compare_field,),
                                   lambda a, b: a.eqNullSafe(b),
                                   header_table=header_table, metadata_table=metadata,
                                   rule_type=ValidationRuleType.VALUES_ARE_EQUAL_RULE)


def is_valid_procedure_and_procedure_status_code_rule(code: str, scheme_field: TableField, code_field: TableField):
    """
    Retrieve a ValidationRule for procedure scheme in use plus status to determine whether a given code is valid
    Args:
        code (str): The unique code for this validation
        code_field (TableField): The field containing the code
        scheme_field (TableField): The field containing the procedure scheme to be validated

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    return ValidationRule(code, scheme_field.table.name,
                          lambda: ProPlusStatusCodeIsValid(col(scheme_field.qualified_name),
                                                           col(code_field.qualified_name)),
                          rule_type=ValidationRuleType.IS_VALID_PROC_CODE_WITH_PROC_STATUS_RULE)


def is_valid_procedure_code_rule(code: str, scheme_field: TableField, code_field: TableField):
    """
    Retrieve a ValidationRule for procedure scheme in use to determine whether a given code is valid
    Args:
        code (str): The unique code for this validation
        code_field (TableField): The field containing the code
        scheme_field (TableField): The field containing the procedure scheme to be validated

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    return ValidationRule(code, scheme_field.table.name,
                          lambda: ProCodeIsValid(col(scheme_field.qualified_name), col(code_field.qualified_name)),
                          rule_type=ValidationRuleType.IS_VALID_CLINICAL_CODE_RULE)


def is_valid_observation_code_rule(code: str, scheme_field: TableField, code_field: TableField):
    """
    Retrieve a ValidationRule for observation scheme in use to determine whether a given code is valid
    Args:
        code (str): The unique code for this validation
        code_field (TableField): The field containing the observation code
        scheme_field (TableField): The field containing the observation scheme to be validated

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    return ValidationRule(code, scheme_field.table.name,
                          lambda: ObsCodeIsValid(col(scheme_field.qualified_name), col(code_field.qualified_name)),
                          rule_type=ValidationRuleType.IS_VALID_CLINICAL_CODE_RULE)


def is_valid_birth_date_age_range(
        code: str,
        birth_date: TableField,
        start_date: TableField,
        end_date: TableField,
        header_table: Optional[Table] = None,
        metadata: Optional[Table] = None,
        max_person_age: int = 130
):
    def expr(birth_date_col: Column, start_date_col: Column, end_date_col: Column):

        birth_date_invalid = ~birth_date_col.rlike(YYYY_MM_DD_PATTERN_NON_STRICT)
        start_date_invalid = ~start_date_col.rlike(YYYY_MM_DD_PATTERN_NON_STRICT)
        end_date_invalid = ~end_date_col.rlike(YYYY_MM_DD_PATTERN_NON_STRICT)

        any_date_invalid = birth_date_invalid | start_date_invalid | end_date_invalid
        any_date_null = birth_date_col.isNull() | start_date_col.isNull() | end_date_col.isNull()

        age = AgeAtEvent(birth_date_col, start_date_col, lit(True))

        date_of_birth_valid = (birth_date_col <= end_date_col) & (age <= max_person_age)

        return any_date_invalid | any_date_null | date_of_birth_valid

    return column_comparison_rule_with_header_table(
        code,
        birth_date,
        (start_date,end_date,),
        expr,
        header_table=header_table, metadata=metadata, rule_type=ValidationRuleType.COLUMN_COMPARISON_RULE
    )


def onset_date_is_after_birth_date_rule(code: str, onset_date: TableField, birth_date: TableField):
    def expr(onset_date_col: Column, birth_date_col: Column):
        onset_date_invalid = ~onset_date_col.rlike(r"^[0-9]{4}-([0][1-9]|[1][0,1,2])$")

        onset_year = onset_date_col.substr(0, 4).cast(IntegerType())
        onset_month = onset_date_col.substr(6, 2).cast(IntegerType())
        birth_year = year(birth_date_col)
        birth_month = month(birth_date_col)

        return onset_date_invalid | \
               (onset_date_col.isNull() | birth_date_col.isNull()) | \
               (onset_year > birth_year) | \
               ((onset_year == birth_year) & (onset_month >= birth_month))

    return column_comparison_rule(code, onset_date, (birth_date,), expr,
                                   rule_type=ValidationRuleType.COLUMN_COMPARISON_RULE)


def date_inside_reporting_period_rule(code: str,
                                      fields: Iterable[TableField],
                                      header_table: Table,
                                      reporting_period_columns: Tuple[TableField, TableField],
                                      offset: DateDelta = DateDelta(),
                                      date_pattern: str = '') -> "ValidationRule":
    """
    Retrieve a ValidationRule that marks row for which any of the provided fields is inside (including borders) of
    the reporting period with an internal validation error code. The markers need to be removed later.

    Args:
        code (str): Internal validation code
        fields (Iterable[TableFields]): fields containing the dates to be validated
        header_table (Table): The header table for the dataset which can be accessed from any row
        reporting_period_columns (Tuple[TableField, TableField]): reporting period start and end column
        offset (DateDelta): added to reporting period end date before comparison
        date_pattern (str): the pattern to match the string date (used for MSDS date validation)


    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    date_fields = sorted(fields)
    first_date_field = date_fields.pop(0)

    def no_dates_are_inside_reporting_period(first_date_field_: Column,
                                             rep_start_date: Column,
                                             rep_end_date: Column,
                                             *other_date_fields: Iterable[Column]
                                             ) -> Column:
        all_date_fields = [first_date_field_, *other_date_fields]
        column = rep_start_date.isNull() | rep_end_date.isNull()

        for date_field in all_date_fields:
            if offset.days:
                upper_limit = date_add(to_date(rep_end_date, DASHED_YYYYMMDD), offset.days)
            elif offset.months:
                upper_limit = add_months(to_date(rep_end_date, DASHED_YYYYMMDD), offset.months)
            else:
                upper_limit = to_date(rep_end_date, DASHED_YYYYMMDD)
            column |= (date_field.isNotNull()
                       & date_field.rlike(date_pattern)
                       & rep_start_date.rlike(date_pattern)
                       & rep_end_date.rlike(date_pattern)
                       & (to_date(rep_start_date, DASHED_YYYYMMDD) <= to_date(date_field, DASHED_YYYYMMDD))
                       & (to_date(date_field, DASHED_YYYYMMDD) <= upper_limit))
        return ~column

    return column_comparison_rule(
        code,
        first_date_field,
        (*reporting_period_columns, *date_fields),
        no_dates_are_inside_reporting_period,
        header_table=header_table,
        rule_type=ValidationRuleType.MARK_IF_DATE_IS_INSIDE_REPORTING_PERIOD)


def column_value_in_set_with_predicate_rule(code: str, table_name: str, column: Union[str, Column],
                                            acceptable_values: AbstractSet[Any], predicate: Callable[..., Column],
                                            allow_nulls: bool = False) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records where a given attribute's value is not a member of a given
    set of values

    Args:
        code (str): The unique code of the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated
        acceptable_values (AbstractSet[Any]): The acceptable set of values for this column
        allow_nulls(bool) : Set value false to not allow nulls, default value is true
        predicate (Callable[..., Column]): A condition that must be met to continue with the validation

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """

    def expr():
        column_normalized = normalise(column)
        condition = column_normalized.isin(acceptable_values)

        if allow_nulls:
            return ~predicate() | column_normalized.isNull() | condition
        return ~predicate() | column_normalized.isNotNull() & condition

    return ValidationRule(code, table_name, expr,
                          rule_type=ValidationRuleType.COLUMN_VALUE_IN_SET_WITH_PREDICATE_RULE)


def column_is_decimal_specified_rule(code: str,
                                     table_name: str,
                                     column: Union[str, Column],
                                     integer_max_digits: int,
                                     fraction_max_digits: int,
                                     integer_min_digits: int = 1,
                                     fraction_min_digits: int = 1,
                                     allow_nulls: bool = True) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records which are not coercible to a defined
    precision (and scale) length

    Please Note: Due to limitations in Spark DecimalType, if a user specified an integer
    precision of larger than 19 digits, then the casting would fail, even though the regex
    underlying this function would be satisfied.

    Args:
        code (str): The unique code of the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated
        integer_max_digits (int): The maximum number of digits in the integer part of the
            decimal
        fraction_max_digits (int): The maximum number of digits in the fraction part of the
            decimal
        integer_min_digits (int): The minimum number of digits in the integer part of the
            decimal
        fraction_min_digits (int): The minimum number of digits in the fraction part of the
            decimal
        allow_nulls(bool) :  Set value false to not allow nulls, default value is true

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    pattern = rf'^\d{{{integer_min_digits},' \
              rf'{integer_max_digits}}}(\.)\d{{{fraction_min_digits},{fraction_max_digits}}}$'
    return column_format_rule(
        code, table_name, column, pattern, allow_nulls,
        rule_type=ValidationRuleType.COLUMN_IS_DECIMAL_SPECIFIED)


def is_valid_gp_practice_code_or_null_rule(
        code: str, practice_code_column: Union[str, Column], practice_code_table: Table,
        date_column: Union[str, Column] = None, date_table: Table = None,
        header_table: Table = None, date_pattern: str = '', case_sensitive: bool = False):
    return _is_valid_property_at_point_in_time(
        validation_udf=GPPracticeCodeValid, code=code, property_column=practice_code_column,
        property_table=practice_code_table, allow_nulls=True, date_column=date_column,
        date_table=date_table, header_table=header_table, date_pattern=date_pattern,
        rule_type=ValidationRuleType.IS_VALID_GP_PRACTICE_CODE_OR_NULL, case_sensitive=case_sensitive
    )


def is_valid_school_or_null_rule(
        code: str, code_column: Union[str, Column], code_table: Table,
        date_column: Union[str, Column] = None, date_table: Table = None, header_table: Table = None,
        date_pattern: str = ''):
    return _is_valid_property_at_point_in_time(
        validation_udf=SchoolValid, code=code, property_column=code_column,
        property_table=code_table, allow_nulls=True, date_column=date_column,
        date_table=date_table, header_table=header_table, date_pattern=date_pattern,
        rule_type=ValidationRuleType.IS_VALID_SCHOOL)


def is_valid_postcode_or_null_rule(code: str, postcode_column: Union[str, Column], postcode_table: Table,
                                   date_column: Union[str, Column] = None, date_table: Table = None,
                                   header_table: Table = None, date_pattern: str = ''):
    return _is_valid_property_at_point_in_time(
        validation_udf=PostcodeInReferenceData, code=code, property_column=postcode_column,
        property_table=postcode_table, allow_nulls=True, date_column=date_column,
        date_table=date_table, header_table=header_table, date_pattern=date_pattern,
        rule_type=ValidationRuleType.IS_VALID_POSTCODE_OR_NULL)


def is_valid_organisation_identifier_or_null_rule(
        code: str, org_id_column: Union[str, Column], org_id_table: Table,
        date_column: Union[str, Column] = None, date_table: Table = None,
        header_table: Table = None, date_pattern: str = ''):
    return _is_valid_property_at_point_in_time(
        validation_udf=OrganisationExists,
        code=code, property_column=org_id_column, property_table=org_id_table, allow_nulls=True,
        date_column=date_column, date_table=date_table, header_table=header_table, date_pattern=date_pattern,
        rule_type=ValidationRuleType.IS_VALID_ORGANISATION_IDENTIFIER_OR_NULL)


def is_valid_organisation_identifier_or_null_ignoring_succession_rule(
        code: str, org_id_column: Union[str, Column], org_id_table: Table,
        date_column: Union[str, Column] = None, date_table: Table = None,
        header_table: Table = None, date_pattern: str = ''):
    return _is_valid_property_at_point_in_time(
        validation_udf=OrganisationExistsIgnoringSuccession,
        code=code, property_column=org_id_column, property_table=org_id_table, allow_nulls=True,
        date_column=date_column, date_table=date_table, header_table=header_table, date_pattern=date_pattern,
        rule_type=ValidationRuleType.IS_VALID_ORGANISATION_IDENTIFIER_OR_NULL)


def is_valid_organisation_identifier_rule(code: str, org_id_column: Union[str, Column], org_id_table: Table,
                                          date_column: Union[str, Column] = None, date_table: Table = None,
                                          header_table: Table = None, date_pattern: str = ''):
    return _is_valid_property_at_point_in_time(
        validation_udf=OrganisationExists,
        code=code, property_column=org_id_column, property_table=org_id_table, allow_nulls=False,
        date_column=date_column, date_table=date_table, header_table=header_table, date_pattern=date_pattern,
        rule_type=ValidationRuleType.IS_VALID_ORGANISATION_IDENTIFIER)


def is_valid_site_id_or_null_ignoring_succession_rule(
        code: str, org_id_column: Union[str, Column], org_id_table: Table,
        date_column: Union[str, Column] = None, date_table: Table = None,
        header_table: Table = None, date_pattern: str = ''):
    return _is_valid_property_at_point_in_time(
        validation_udf=SiteCodeValidIgnoringSuccession,
        code=code, property_column=org_id_column, property_table=org_id_table, allow_nulls=True,
        date_column=date_column, date_table=date_table, header_table=header_table, date_pattern=date_pattern,
        rule_type=ValidationRuleType.IS_VALID_SITE_ID_OR_NULL)


def is_valid_site_id_or_null_rule(
        code: str, org_id_column: Union[str, Column], org_id_table: Table,
        date_column: Union[str, Column] = None, date_table: Table = None,
        header_table: Table = None, date_pattern: str = ''):
    return _is_valid_property_at_point_in_time(
        validation_udf=SiteCodeValid,
        code=code, property_column=org_id_column, property_table=org_id_table, allow_nulls=True,
        date_column=date_column, date_table=date_table, header_table=header_table, date_pattern=date_pattern,
        rule_type=ValidationRuleType.IS_VALID_SITE_ID_OR_NULL)


def is_valid_default_org_or_null_rule(
        code: str,
        org_id_column: Union[str, Column],
        org_id_table: Table,
        default_codes: List[str],
        date_column: Union[str, Column] = None,
        date_table: Table = None,
        header_table: Table = None,
        date_pattern: str = ''
):
    return _is_default_property_valid(
        validation_udf=DefaultOrgValid, code=code, property_column=org_id_column,
        property_table=org_id_table, default_codes=default_codes,
        allow_nulls=True, date_column=date_column, date_table=date_table, header_table=header_table,
        date_pattern=date_pattern, rule_type=ValidationRuleType.IS_VALID_DEFAULT_ORG_OR_NULL)


def is_valid_default_org_or_null_ignoring_succession_rule(
        code: str,
        org_id_column: Union[str, Column],
        org_id_table: Table,
        default_codes: List[str],
        date_column: Union[str, Column] = None,
        date_table: Table = None,
        header_table: Table = None,
        date_pattern: str = ''
):
    return _is_default_property_valid(
        validation_udf=DefaultOrgValid, code=code, property_column=org_id_column,
        property_table=org_id_table, default_codes=default_codes, allow_successions=False,
        allow_nulls=True, date_column=date_column, date_table=date_table, header_table=header_table,
        date_pattern=date_pattern, rule_type=ValidationRuleType.IS_VALID_DEFAULT_ORG_OR_NULL
    )


def is_valid_default_org_or_null_anytime_rule(
        code: str,
        org_id_column: Union[str, Column],
        org_id_table: Table,
        default_codes: List[str]
):
    return _is_default_property_valid_anytime(
        validation_udf=DefaultOrgValidAnytimeWithoutSuccessor, code=code, property_column=org_id_column,
        property_table=org_id_table, default_codes=default_codes,
        allow_nulls=True, rule_type=ValidationRuleType.IS_VALID_DEFAULT_ORG_OR_NULL_ANYTIME)


def is_valid_find_code_rule(code: str, scheme_field: TableField, code_field: TableField, dataset=None):
    """
    Retrieve a ValidationRule for finding scheme in use to determine whether a given code is valid
    Args:
        code (str): The unique code for this validation
        code_field (TableField): The field containing the code
        scheme_field (TableField): The field containing the finding scheme to be validated
        dataset (str): The dataset

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    find_code_udf = IcdSnomedFindCodeIsValid if dataset in [
        DS.MHSDS_V6, DS.IAPT_V2_1
    ] else FindCodeIsValid

    return ValidationRule(code, scheme_field.table.name,
                          lambda: lit(find_code_udf(scheme_field.qualified_name, code_field.qualified_name)),
                          rule_type=ValidationRuleType.IS_VALID_CLINICAL_CODE_RULE)


def is_valid_diag_code_rule(code: str, scheme_field: TableField, code_field: TableField, dataset=None):
    """
    Retrieve a ValidationRule for diagnoses scheme in use to determine whether a given code is valid
    Args:
        code (str): The unique code for this validation
        code_field (TableField): The field containing the code
        scheme_field (TableField): The field containing the assessment score to be validated
        dataset (str): The dataset

    Returns:
        ValidationRule: A validation rule corresponding to the given parameters
    """
    diag_code_udf = IcdSnomedDiagCodeIsValid if dataset in [
        DS.MHSDS_V6, DS.IAPT_V2_1
    ] else DiagCodeIsValid

    return ValidationRule(code, scheme_field.table.name,
                          lambda: lit(diag_code_udf(scheme_field.qualified_name, code_field.qualified_name)),
                          rule_type=ValidationRuleType.IS_VALID_CLINICAL_CODE_RULE)


def is_valid_snomed_ct_rule(code: str, table_name: str, column: Union[str, Column],
                            max_length: int = 56, min_length: int = 6, allow_nulls: bool = True,
                            side_condition: Callable[..., Column] = lambda: lit(True)) -> 'ValidationRule':
    """
    Retrieve a validation rule to validate that a given field contains a valid SNOMED CT value or a valid
    post coordinated SNOMED CT value.

    Args:
        code: The unique code of the validation rule
        table_name: The name of the table to be validated
        column: The table column to be validated
        max_length: The max length of the code
        min_length: The min length of the code. Defaults to 6
        allow_nulls: Set value false to not allow nulls, default value is true
        side_condition: Additional conditions to be applied alongside the validation, to be ORed with the result

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """

    def expr():
        column_normalized = normalise(column)
        if allow_nulls:
            return ~side_condition() | column_normalized.isNull() | IsValidSnomedCT(column, lit(min_length),
                                                                                    lit(max_length))
        return ~side_condition() | IsValidSnomedCT(column, lit(min_length), lit(max_length))

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.VALID_SNOMED_CT_RULE)


def cross_table_uniqueness_rule(code: str, table1: Table, table2: Table,
                                column1: TableField, column2: TableField) -> "ValidationRule":
    """Return a validation rule that is triggered if column1 in table1 is not null and is equal
    to some value in column2 of table2."""

    def expr():
        return col(column2.qualified_name).isNull()

    return ValidationRule(code, table1.name, expr, JoinChain(JoinParameters(table1, table2, (column1, column2))),
                          rule_type=ValidationRuleType.CROSS_TABLE_UNIQUENESS_RULE)


def uniqueness_with_condition_rule(
        code: str, table_name: str, unique_columns: Sequence[Union[str, Column]],
        side_condition: Callable[[], Column], nulls_are_unique=True
) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records which feature equivalent sets of key columns, for those the
    side condition is true

    Args:
        code (str): The unique code for the validation rule
        table_name (str): The name of the table to be validated
        unique_columns (Sequence[Union[str, Column]]): The columns to be treated as keys for the purpose of
            validation
        side_condition (Callable[..., Column]): The side_condition that must be satisfied to invalidate the record
        nulls_are_unique (bool): flag to indicate whether to regard nulls as unique in the window function

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    assert isinstance(unique_columns, Sequence)

    def expr():
        columns = [normalise(column) for column in unique_columns]
        if nulls_are_unique:
            columns = [coalesce(column, UUID()) for column in columns]
        condition = count("*").over(Window.partitionBy(*columns)) == 1
        return ~side_condition() | condition

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.UNIQUENESS_RULE_WITH_CONDITION)


def singleton_rule(code: str, table_name: str) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records which appear more than once

    Args:
        code (str): The unique code for the validation rule
        table_name (str): The name of the table to be validated

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    return uniqueness_rule(code, table_name, set(), rule_type=ValidationRuleType.SINGLETON_RULE)


def column_is_not_null_rule(code: str, table_name: str, column: Union[str, Column]) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records where a given attribute is null

    Args:
        code (str): The unique code for the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    return ValidationRule(code, table_name, lambda: normalise(column).isNotNull(),
                          rule_type=ValidationRuleType.COLUMN_IS_NOT_NULL_RULE)


def column_is_null_rule(code: str, table_name: str, column: Union[str, Column]) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records where a given attribute is not null

    Args:
        code (str): The unique code for the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    return ValidationRule(code, table_name, lambda: normalise(column).isNull(),
                          rule_type=ValidationRuleType.COLUMN_IS_NULL_RULE)


def column_not_blank_with_condition_rule(code: str, table_name: str, column: Union[str, Column],
                                         key_columns: Sequence[Union[str, Column]],
                                         side_condition: Callable[..., Column]) -> 'ValidationRule':
    """
    Checks if column is not empty with given condition
    Args:
        code: The unique code for the validation rule
        table_name: The name of the table to be validated
        column: The table column to be validated
        key_columns: The columns to be treated as keys for the purpose of validation
        side_condition (Callable[..., Column]): The side_condition that must be satisfied to invalidate the record

    Returns:

    """
    assert isinstance(key_columns, Sequence)

    def expr():
        columns = [normalise(key_column) for key_column in key_columns]
        condition = normalise(column).isNotNull() & (trim(normalise(column).cast('string')) != lit(""))
        return ~side_condition(*columns) | condition

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.COLUMN_NOT_BLANK_RULE_WITH_CONDITION)


def related_column_is_populated_rule(code: str, table_name: str, column: Union[str, Column],
                                     related_column: Union[str, Column]) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records where a given related column is populated

    Args:
        code (str): The unique code for the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated
        related_column (Union[str, Column]): The related_column that must not be blank if column is populated
    Returns:
        ValidationRule: A rule instance representing the given parameters
    """

    def expr():
        nr_column = normalise(related_column)
        n_column = normalise(column)
        return nr_column.isNotNull() | n_column.isNull()

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.RELATED_COLUMN_IS_POPULATED_RULE)


def column_not_in_format_rule(code: str, table_name: str, column: Union[str, Column],
                              expected_format: str, allow_nulls: bool = True,
                              rule_type: ValidationRuleType = ValidationRuleType.COLUMN_FORMAT_RULE) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records where a given attribute does match a regular expression

    Args:
        code (str): The unique code for the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated
        expected_format (str): The regular expression that the column is expected to match
        allow_nulls(bool) :  Set value false to not allow nulls, default value is true
        rule_type: type of ValidationRuleType default to COLUMN_FORMAT_RULE

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """

    def expr():
        column_normalized = normalise(column)
        condition = column_normalized.rlike(expected_format)

        if allow_nulls:
            return column_normalized.isNull() | ~condition
        return column_normalized.isNotNull() & ~condition

    return ValidationRule(code, table_name, expr, rule_type=rule_type)


def _time_comparison(comp_operator: Union[le, ge], offset: DateDelta = DateDelta(),
                     only_on_same_day: bool = True,
                     additional_date_check_expr: Callable[..., Column] = lambda *_: lit(True)) -> Callable:
    """
    Create a time comparison operator suitable for time comparisons used with column_comparison_rule

    Args:
        comp_operator: one of operator.le, operator.ge, direction of comparison
        offset (DateDelta): added to the second date before comparison
        only_on_same_day(bool): if true, only return False if the dates are the same; useful if there is a separate
            date comparison validation
        additional_date_check_expr (column) : additional date field checks

    Returns:
        function of type
            (first_time_column, first_date_column, second_time_column, second_date_column) -> bool_column
    """
    if comp_operator not in (le, ge):
        raise NotImplementedError("operator {} not supported".format(comp_operator))

    # The date_comp_operator returns True if there is no need to compare the times
    if only_on_same_day:
        date_comp_operator = ne
    else:
        date_comp_operator = lt if comp_operator is le else gt

    if offset.days:
        return lambda first_time, first_date, second_time, second_date: (
                first_time.isNull() | second_time.isNull() | additional_date_check_expr(first_date, second_date) |
                date_comp_operator(to_date(first_date, 'yyyy-MM-dd'),
                                   date_add(to_date(second_date, 'yyyy-MM-dd'), offset.days)) |
                (
                        (first_date == date_add(to_date(second_date, 'yyyy-MM-dd'), offset.days)) &
                        comp_operator(first_time, second_time)
                )
        )
    elif offset.months:
        return lambda first_time, first_date, second_time, second_date: (
                first_time.isNull() | second_time.isNull() | additional_date_check_expr(first_date, second_date) |
                date_comp_operator(to_date(first_date, 'yyyy-MM-dd'),
                                   add_months(to_date(second_date, 'yyyy-MM-dd'), offset.months)) |
                (
                        (first_date == add_months(to_date(second_date, 'yyyy-MM-dd'), offset.months)) &
                        comp_operator(first_time, second_time)
                )
        )
    else:
        return lambda first_time, first_date, second_time, second_date: (
                first_time.isNull() | second_time.isNull() | additional_date_check_expr(first_date, second_date) |
                date_comp_operator(to_date(to_date(first_date, 'yyyy-MM-dd'), 'yyyy-MM-dd'),
                                   to_date(second_date, 'yyyy-MM-dd')) |
                (
                        (to_date(first_date, 'yyyy-MM-dd') == to_date(second_date, 'yyyy-MM-dd')) &
                        comp_operator(first_time, second_time)
                )
        )


def _datetime_comparison(comp_operator: Union[le, ge], offset: DateDelta = DateDelta(),
                         additional_date_check_expr: Callable[..., Column] = lambda *_: lit(True)) -> Callable:
    """
    Create a comparison operator suitable for date comparisons used with column_comparison_rule

    Args:
        comp_operator: one of operator.le, operator.ge, direction of comparison
        offset (DateDelta): added to the second date before comparison
        additional_date_check_expr (column) : additional date field checks
    Returns:
        function of type (first_date_column, second_date_column) -> bool_column
    """
    if comp_operator not in (le, ge):
        raise NotImplementedError("operator {} not supported".format(comp_operator))

    if offset.days:
        return lambda first_datetime, second_datetime: (
            additional_date_check_expr(first_datetime, second_datetime) |
            comp_operator(first_datetime), date_add(second_datetime, offset.days)
        )
    elif offset.months:
        return lambda first_datetime, second_datetime: (
                additional_date_check_expr(first_datetime, second_datetime) |
                comp_operator(first_datetime, add_months(second_datetime, offset.months))
        )
    else:
        return lambda first_datetime, second_datetime: (
                additional_date_check_expr(first_datetime, second_datetime) |
                comp_operator(first_datetime, second_datetime)
        )


def datetime_before_or_on_datetime_rule(
        code: str,
        field: TableField,
        later_field: TableField,
        header_table: Optional[Table] = None,
        metadata: Optional[Table] = None,
        offset: DateDelta = DateDelta()
) -> ValidationRule:
    """
        Retrieve a ValidationRule to determine whether a date value is not after a second date value

        Args:
            header_table: header table to use
            code (str): The unique code for this validation
            field (TableField): The field containing the date to be validated
            later_field (TableField): The reference field that must be after (or equal to) the validated field
            metadata_table: Optional parameter, default set to None. Required for comparison rules that use metadata.
            offset (DateDelta): added to later_field before comparison

        Returns:
            ValidationRule: A validation rule corresponding to the given parameters
    """

    return column_comparison_rule_with_header_table(
        code,
        field,
        (later_field,),
        _datetime_comparison(comp_operator=le, offset=offset, additional_date_check_expr=datetime_check_expr),
        header_table=header_table, metadata=metadata, rule_type=ValidationRuleType.DATETIME_BEFORE_OR_ON_DATETIME_RULE
    )


def reject_all_rule(code: str, table_name: str) -> 'ValidationRule':
    """
    Reject all record for the given table name
    Args:
        code (str): The unique code of the validation rule
        table_name (str): The name of the table to be validated
    Return:
        ValidationRule: A rule instance representing the given parameters
    """
    return ValidationRule(code, table_name, lambda: lit(False), rule_type=ValidationRuleType.REJECT_ALL_RULE)


def referential_integrity_rule(code: str, left_hand_table: Table, right_hand_table: Table,
                               conditions: Callable[[], Tuple[Column, ...]] = tuple) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records which do not have a corresponding record in a related table

    Args:
        code (str): The unique code for the validation rule
        left_hand_table (Table): The table on the left hand side of the test
        right_hand_table (Table): The table on the right hand side of the test
        conditions (Callable[[], Tuple[Column, ...]]): Conditions to apply to the right hand table before the join
            takes place

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    if right_hand_table in left_hand_table.related_tables():
        join_parameters = left_hand_table.get_join_parameters(right_hand_table, conditions=conditions)
    else:
        assert left_hand_table in right_hand_table.related_tables(), \
            "No relationship between {} and {} to test referential integrity".format(
                left_hand_table.name, right_hand_table.name)
        join_parameters = right_hand_table.get_join_parameters(left_hand_table).invert(conditions=conditions)

    def expr():
        return all_conditions(
            *(col(left_key.qualified_name).eqNullSafe(col(right_key.qualified_name))
              for left_key, right_key in join_parameters.join_keys)
        )

    return ValidationRule(code, left_hand_table.name, expr, JoinChain(join_parameters),
                          rule_type=ValidationRuleType.REFERENTIAL_INTEGRITY_RULE)


def referential_integrity_using_is_valid_record_rule(
        code: str, left_hand_table: Table, right_hand_table: Table
) -> ValidationRule:
    """
    MHSDS_V6, MSDS -specific referential integrity rule
    Applies is_valid_record as a condition for discarding records from the right-hand table

    Args:
        code (str): The unique code of the validation rule
        left_hand_table (Table): The table on the left hand side of the join
        right_hand_table (Table): The table on the right hand side of the join

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    return referential_integrity_rule(code, left_hand_table, right_hand_table,
                                      conditions=lambda: (is_valid_record(Common.DQ),))


def _collect_rules_by_preconditions(optimize: bool, *rules: 'ValidationRule') -> \
        Iterator[Tuple['_RulePrecondition', Iterator['ValidationRule']]]:
    """
    Collect rules corresponding to required joins and sort rules without joins first

    Args:
        *rules (ValidationRule): The rules to be sorted

    Returns:
        Iterator[Tuple['_RulePrecondition', Iterator['ValidationRule']]]: Sequence of tuples representing the
            preconditions of rules and the rules with that precondition, ordered with unjoined validations first
    """

    sorted_rules = sorted(rules, key=_RulePrecondition.from_rule) if optimize else rules
    grouped_rules = groupby(sorted_rules, key=_RulePrecondition.from_rule)
    return grouped_rules


def apply_referential_integrity_rules(spark: SparkSession, *rules: 'ValidationRule'):
    """
    Apply the given validation rules against dataframes registered to the current Spark session
    and store the result back. No reordering or grouping of joins will happen, which makes this
    function good for referential integrity checks.

    Args:
        spark (SparkSession): The current Spark session
        *rules (ValidationRule): The rules to be applied
    """
    for precondition, rules_for_precondition in _collect_rules_by_preconditions(False, *rules):
        _apply_collected_rules(spark, precondition, *rules_for_precondition)


def _apply_collected_rules(spark: SparkSession, precondition: '_RulePrecondition',
                           *rules: 'ValidationRule'):
    """
    Apply all rules with a matching precondition, storing the validated table back to the Spark session

    Args:
        spark (SparkSession): The current spark session
        precondition (_RulePrecondition): The common precondition of all given rules
        *rules (ValidationRule): The rules to be applied
    """
    df = spark.table(precondition.table_name)
    if Common.DQ not in df.schema.names:
        df = df.withColumn(Common.DQ, array())

    df = chain_joins(spark, df, *precondition.required_joins)

    df = (
        df.withColumn(
            Common.DQ,
            array_remove(
                concat(
                    Common.DQ,
                    array(*(when(~rule.condition(), lit(rule.code)).otherwise(lit('')) for rule in rules))
                ),
                ''
            )
        )
    )

    final_df = df.select(precondition.table_name + ".*", Common.DQ)
    final_df.createOrReplaceTempView(precondition.table_name)


def apply_rules(spark: SparkSession, *rules: 'ValidationRule'):
    """
    Apply the given validation rules against dataframes registered to the current Spark session
    and store the result back. The rules will be optimized and reordered to reduce the number of
    joins that have to happen. This makes it not fit for referential integrity checks, which have
    to maintain ordering.

    Args:
        spark (SparkSession): The current Spark session
        *rules (ValidationRule): The rules to be applied
    """
    for precondition, rules_for_precondition in _collect_rules_by_preconditions(True, *rules):
        _apply_collected_rules(spark, precondition, *rules_for_precondition)


def datetime_is_valid_with_regex_epma_rule(code: str,
                                           table_name: str,
                                           column: Union[str, Column],
                                           date_format: str = 'dd/MM/yyyy',
                                           date_regex: str = r'^\d{2}/\d{2}/\d{4}$',
                                           allow_nulls: bool = True):
    def expr():
        column_normalized = normalise(column)
        matches_valid_date_regex = column_normalized.rlike(date_regex)
        convert_to_date = to_date(column_normalized, date_format).isNotNull()
        condition = matches_valid_date_regex & convert_to_date

        if allow_nulls:
            return column_normalized.isNull() | condition
        else:
            return column_normalized.isNotNull() & condition

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.DATETIME_IS_VALID_WITH_REGEX_EPMA)


def column_value_valid_nhs_number_rule(code: str, table_name: str, column: Union[str, Column],
                                       allow_nulls: bool = True) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records where a given attribute's valid does not represent a valid
    NHS number

    Args:
        code (str): The unique code of the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated
        allow_nulls(bool) : Set value false to not allow nulls, default value is true

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """

    def expr():
        column_normalized = normalise(column)
        condition_udf = udf(is_valid_nhs_number, BooleanType())(column_normalized.cast(StringType()))

        if allow_nulls:
            return column_normalized.isNull() | condition_udf
        return column_normalized.isNotNull() & condition_udf

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.COLUMN_VALUE_VALID_NHS_NUMBER_RULE)


def date_is_valid_rule(
        code: str,
        table_name: str,
        column: Union[str, Column],
        date_format: str = 'yyyy-MM-dd',
        check_24_hour_clock=False,
        allow_nulls: bool = True
) -> 'ValidationRule':
    def expr():
        column_normalized = normalise(column)
        condition = to_date(column_normalized, date_format).isNotNull()

        if check_24_hour_clock:
            condition = (~column_normalized.rlike(r'[pm|am|PM|AM]+')) & condition

        if allow_nulls:
            return column_normalized.isNull() | condition
        else:
            return column_normalized.isNotNull() & condition

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.DATE_IS_VALID)


def column_is_decimal_precision_rule(code: str, table_name: str, column: Union[str, Column], precision: int,
                                     allow_nulls: bool = True) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records which are not coercible to a defined precision (and scale)
    length

    Please Note: Due to limitations in Spark DecimalType, if a user specified an integer precision of larger than 19
    digits, then the casting would fail, even though the regex underlying this function would be satisfied.

    Args:
        code (str): The unique code of the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated
        precision (int): The maximum number of digits in the integer part of the decimal
        allow_nulls(bool) :  Set value false to not allow nulls, default value is true

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    pattern = r'^\d{{1,{}}}(\.\d+)?$'.format(precision)
    return column_format_rule(code, table_name, column, pattern, allow_nulls,
                              rule_type=ValidationRuleType.COLUMN_IS_DECIMAL_PRECISION)


def uniqueness_rule(
        code: str, table_name: str,
        unique_columns: AbstractSet[Union[str, Column]], nulls_are_unique=True,
        rule_type: ValidationRuleType = ValidationRuleType.UNIQUENESS_RULE
) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records which feature equivalent sets of key columns

    Args:
        rule_type:
        code (str): The unique code for the validation rule
        table_name (str): The name of the table to be validated
        unique_columns (AbstractSet[Union[str, Column]]): The columns to be treated as keys for the purpose of
            validation
        nulls_are_unique (bool): flag to indicate whether to regard nulls as unique in the window function

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """

    def expr():
        columns = [normalise(column) for column in unique_columns]
        if nulls_are_unique:
            columns = [coalesce(column, UUID()) for column in columns]

        return count('*').over(Window.partitionBy(*columns)) == 1

    return ValidationRule(code, table_name, expr, rule_type=rule_type)


def _is_default_property_valid_anytime(validation_udf: Callable[[str, Union[str, int, date, datetime]], bool],
                                       code: str, property_column: Union[str, Column], property_table: Table,
                                       default_codes: List[str], allow_nulls: bool,
                                       rule_type: ValidationRuleType = None):
    required_joins = set()

    def expr():
        id_col = normalise(property_column)

        if allow_nulls:
            return id_col.isNull() | validation_udf(default_codes)(id_col.cast(StringType()))

        return id_col.isNotNull() & validation_udf(default_codes)(id_col.cast(StringType()))

    return ValidationRule(code, property_table.name, expr, *required_joins, rule_type=rule_type)


def _is_default_property_valid(
        validation_udf: Callable[[str, Union[str, int, date, datetime]], bool],
        code: str, property_column: Union[str, Column], property_table: Table,
        default_codes: List[str], allow_nulls: bool, date_column: Union[str, Column] = None,
        allow_successions=True, date_table: Table = None, header_table: Table = None, date_pattern: str = '',
        rule_type: ValidationRuleType = None
):
    required_joins = set()
    if date_table:
        required_joins = property_table.get_required_joins(date_table, header_table)

    def expr():
        normalised_property_column = normalise(property_column)
        date_norm = normalise(date_column) if date_column else lit(None)
        validation_expr = when(
            (date_norm.isNotNull() & ~date_norm.rlike(date_pattern)),
            lit(True)
        ).otherwise(
            validation_udf(default_codes, allow_successions)(
                normalised_property_column.cast(StringType()),
                to_date(date_norm, DASHED_YYYYMMDD).cast(StringType())
            )
        )

        if allow_nulls:
            return normalised_property_column.isNull() | validation_expr

        return normalised_property_column.isNotNull() & validation_expr

    return ValidationRule(code, property_table.name, expr, *required_joins, rule_type=rule_type)


def _is_valid_property_at_point_in_time(validation_udf: Callable[[str, Union[str, int, date, datetime]], bool],
                                        code: str, property_column: Union[str, Column], property_table: Table,
                                        allow_nulls: bool, date_column: Union[str, Column] = None,
                                        date_table: Table = None, header_table: Table = None,
                                        date_pattern: str = '',
                                        rule_type: ValidationRuleType = None,
                                        case_sensitive: bool = False):
    required_joins = set()
    if date_table:
        required_joins = property_table.get_required_joins(date_table, header_table)

    def expr():
        normalised_property_column = normalise(property_column)
        validation_args = [normalised_property_column]

        if date_column:
            date_norm = normalise(date_column)
            validation_args.append(to_date(date_norm, DASHED_YYYYMMDD))
            validation_expr = when(
                (date_norm.isNotNull() & ~date_norm.rlike(date_pattern)),
                lit(True)
            ).otherwise(validation_udf(*validation_args))
        else:
            validation_expr = validation_udf(*validation_args)

        if case_sensitive:
            if not date_column:
                validation_args.append(lit(None))
            validation_args.append(lit(True))
            validation_expr = validation_udf(*validation_args)

        if allow_nulls:
            return normalised_property_column.isNull() | validation_expr

        return normalised_property_column.isNotNull() & validation_expr

    return ValidationRule(code, property_table.name, expr, *required_joins, rule_type=rule_type)


def date_comparison(comp_operator: Union[le, ge], offset: DateDelta = DateDelta(),
                    additional_date_check_expr: Callable[..., Column] = lambda *_: lit(True)) -> Callable:
    """
    Create a comparison operator suitable for date comparisons used with column_comparison_rule

    Args:
        comp_operator: one of operator.le, operator.ge, direction of comparison
        offset (DateDelta): added to the second date before comparison
        additional_date_check_expr (column) : additional date field checks
    Returns:
        function of type (first_date_column, second_date_column) -> bool_column
    """
    if comp_operator not in (le, ge):
        raise NotImplementedError("operator {} not supported".format(comp_operator))

    if offset.days:
        return lambda first_date, second_date: (
                additional_date_check_expr(first_date, second_date) |
                comp_operator(to_date(first_date, DASHED_YYYYMMDD),
                              date_add(to_date(second_date, DASHED_YYYYMMDD), offset.days))
        )
    elif offset.months:
        return lambda first_date, second_date: (
                additional_date_check_expr(first_date, second_date) |
                comp_operator(to_date(first_date, DASHED_YYYYMMDD),
                              add_months(to_date(second_date, DASHED_YYYYMMDD), offset.months))
        )
    else:
        return lambda first_date, second_date: (
                additional_date_check_expr(first_date, second_date) |
                comp_operator(to_date(first_date, DASHED_YYYYMMDD), to_date(second_date, DASHED_YYYYMMDD))
        )


def column_value_not_in_set_rule(code: str, table_name: str, column: Union[str, Column],
                                 restricted_values: AbstractSet[Any], allow_nulls: bool = True,
                                 requirement: Callable[..., Column] = lambda: lit(True)) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records where a given attribute's value is not a member of a given
    set of values

    Args:
        code (str): The unique code of the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated
        restricted_values (AbstractSet[Any]): The forbidden set of values for this column
        allow_nulls(bool) : Set value false to not allow nulls, default value is true
        requirement (Callable[..., Column]): The side_condition that must be satisfied to validate the record

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """

    def expr():
        column_normalized = normalise(column)
        condition = ~column_normalized.isin(restricted_values)
        if allow_nulls:
            return column_normalized.isNull() | condition & requirement()
        return column_normalized.isNotNull() & condition & requirement()

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.COLUMN_VALUE_NOT_IN_SET_RULE)


def column_value_in_set_rule(code: str, table_name: str, column: Union[str, Column],
                             acceptable_values: Iterable[Any], allow_nulls: bool = True) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records where a given attribute's value is not a member of a given
    set of values

    Args:
        code (str): The unique code of the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated
        acceptable_values (AbstractSet[Any]): The acceptable set of values for this column
        allow_nulls(bool) : Set value false to not allow nulls, default value is true

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """

    def expr():
        column_normalized = normalise(column)
        condition = column_normalized.isin(acceptable_values)
        if allow_nulls:
            return column_normalized.isNull() | condition
        return column_normalized.isNotNull() & condition

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.COLUMN_VALUE_IN_SET_RULE)


def column_is_numeric_rule(code: str, table_name: str, column: Union[str, Column], min_len: int = None,
                           max_len: int = None, allow_negative: bool = False, allow_decimals: bool = False,
                           allow_nulls: bool = True) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records which are not numeric, optionally of a constrained length

    Args:
        code (str): The unique code of the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated
        min_len (int): The minimum length of the string to be accepted
        max_len (int): The maximum length of the string to be accepted
        allow_decimals (bool): Allow decimal values, dot (.) used as a separator
        allow_negative (bool): Allow negative values
        allow_nulls (bool) : Set value false to not allow nulls, default value is true

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    pattern_raw = r'[\d.]' if allow_decimals else r'\d'
    pattern = _repeating_pattern(pattern_raw, min_len=min_len, max_len=max_len)
    if allow_negative:
        pattern = '^-?{}'.format(pattern[1:])
    return column_format_rule(
        code, table_name, column, pattern, allow_nulls, rule_type=ValidationRuleType.COLUMN_IS_NUMERIC)


def column_is_alphanumeric_rule(code: str, table_name: str,
                                column: Union[str, Column], min_len: int = None,
                                max_len: int = None, allow_nulls: bool = True,
                                allow_special_chars: bool = True) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records which are not alphanumeric, optionally of
    a constrained length

    Args:
        code (str): The unique code of the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated
        min_len (int): The minimum length of the string to be accepted
        max_len (int): The maximum length of the string to be accepted
        allow_nulls(bool) :  Set value false to not allow nulls, default value is true
        allow_special_chars(bool): Set value False to not allow special characters. Defaults to True.

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    pattern = _repeating_pattern('.' if allow_special_chars else r'\w',
                                 min_len=min_len, max_len=max_len)
    return column_format_rule(code, table_name, column, pattern, allow_nulls,
                              rule_type=ValidationRuleType.COLUMN_IS_ALPHANUMERIC)


def _repeating_pattern(repeating_group: str, min_len: int = None, max_len: int = None) -> str:
    if min_len is None and max_len is None:
        quantifier = "*"
    else:
        lower_bound = min_len or 0
        upper_bound = max_len if max_len is not None else ""

        quantifier = "{{{},{}}}".format(lower_bound, upper_bound)
    return r"^{repeating_group}{quantifier}$".format(repeating_group=repeating_group, quantifier=quantifier)


def column_format_rule(code: str, table_name: str, column: Union[str, Column],
                       expected_format: str, allow_nulls: bool = True,
                       rule_type: ValidationRuleType = ValidationRuleType.COLUMN_FORMAT_RULE) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records where a given attribute does not match a regular expression

    Args:
        code (str): The unique code for the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated
        expected_format (str): The regular expression that the column is expected to match
        allow_nulls(bool) :  Set value false to not allow nulls, default value is true
        rule_type: type of ValidationRuleType default to COLUMN_FORMAT_RULE

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """

    def expr():
        column_normalized = normalise(column)
        condition = column_normalized.rlike(expected_format)

        if allow_nulls:
            return column_normalized.isNull() | condition
        return column_normalized.isNotNull() & condition

    return ValidationRule(code, table_name, expr, rule_type=rule_type)


def placeholder_rule(code: str, table_name: str, reason: str = None):
    """
    Retrieve a ValidationRule which never fails, but serves as a placeholder for validations implemented elsewhere

    Args:
        code (str): The unique code for the validation rule
        table_name (str): The name of the table to be validated
        reason (str): Informative string to indicate the place of the real validation

    Returns:
        ValidationRule: A rule instance representing the given parameters
    """
    return ValidationRule(code, table_name, lambda: lit(True))


def column_not_blank_rule(code: str, table_name: str, column: Union[str, Column]) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records where a given attribute is null or empty

    Args:
        code (str): The unique code for the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated
    Returns:
        ValidationRule: A rule instance representing the given parameters
    """

    def expr():
        return normalise(column).isNotNull() & (trim(normalise(column).cast('string')) != lit(""))

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.COLUMN_NOT_BLANK_RULE)


def related_column_not_blank_rule(code: str, table_name: str, column: Union[str, Column],
                                  related_column: Union[str, Column]) -> 'ValidationRule':
    """
    Retrieve a ValidationRule which invalidates records if both a column and its sibling column are null or empty.

    Args:
        code (str): The unique code for the validation rule
        table_name (str): The name of the table to be validated
        column (Union[str, Column]): The table column to be validated
        related_column (Union[str, Column]): An additional related column to be validated
    Returns:
        ValidationRule: A rule instance representing the given parameters
    """

    def expr():
        lhs_col = normalise(column)
        rhs_col = normalise(related_column)
        lhs_expr = lhs_col.isNotNull() & (trim(lhs_col.cast('string')) != lit(""))
        rhs_expr = rhs_col.isNotNull() & (trim(rhs_col.cast('string')) != lit(""))
        return lhs_expr | rhs_expr

    return ValidationRule(code, table_name, expr, rule_type=ValidationRuleType.COLUMN_NOT_BLANK_RULE)


def get_left_anti_join_dataframe(
    reference_df: DataFrame,
    reference_column: str,
    target_df: DataFrame,
    target_column: str,
    extra_columns_from_reference_df: List[str] = []
) -> DataFrame:
    """
    Retrieves a reference_column values from reference_df which don't have any entry in target_column in target_df

    Args:
        reference: (DataFrame) Left side dataframe(reference_df), where check needs to happen
        reference_column: (str) column name to join on from the left dataframe
        target: (DataFrame) Right side dataframe(target_df), where comparison needs to happen
        target_column: (str) column name to join on from the right dataframe
    Returns:
        DataFrame: Returns column values from reference_df with no matching records from target_df
    """
    assert reference_column in reference_df.columns, f'There is no column {reference_column} in dataframe {reference_df}'
    assert target_column in target_df.columns, f'There is no column {target_column} in dataframe {target_df}'
    reference_left_antijoin_df = (reference_df
                                .join(target_df,
                                        reference_df[reference_column] == target_df[target_column],
                                        how = "left_anti")
                                        .select(reference_df[reference_column], *extra_columns_from_reference_df)
                                        )

    return reference_left_antijoin_df
