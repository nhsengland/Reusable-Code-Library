from datetime import datetime
from enum import Enum
from typing import AbstractSet, Tuple, Callable, Sequence, FrozenSet, Optional

from pyspark.sql import Column
from pyspark.sql.functions import col, lit, concat, array, coalesce, struct, create_map
from pyspark.sql.types import StringType

from dsp.common.relational import JoinChain, TableField
from dsp.datasets.common import Fields as Common, DQMessageType
from dsp.dq_files.output import Fields as DQFields

DASHED_YYYYMMDD = 'yyyy-MM-dd'


class ValidationRuleType(Enum):
    """
    Attribute specifying the source of a validation rule type
    """

    # Populated
    COLUMN_NOT_BLANK_RULE = "column_not_blank_rule"
    COLUMN_NOT_BLANK_RULE_WITH_CONDITION = "column_not_blank_rule_with_condition"
    COLUMN_IS_NOT_NULL_RULE = "column_is_not_null_rule"
    COLUMN_IS_NULL_RULE = "column_is_null_rule"
    RELATED_COLUMN_IS_POPULATED_RULE = "related_column_is_populated_rule"

    # Uniqueness
    UNIQUENESS_RULE = "uniqueness_rule"
    UNIQUENESS_RULE_WITH_CONDITION = "uniqueness_rule_with_condition"
    CROSS_TABLE_UNIQUENESS_RULE = "cross_table_uniqueness_rule"
    SINGLETON_RULE = "singleton_rule"

    # Format
    COLUMN_FORMAT_RULE = "column_format_rule"
    COLUMN_IS_ALPHANUMERIC = "column_is_alphanumeric"
    COLUMN_IS_NUMERIC = "column_is_numeric"
    COLUMN_IS_NUMERIC_WITH_MIN_INCLUSIVE = "column_is_numeric_with_min_inclusive"
    COLUMN_IS_DECIMAL_PRECISION = "column_is_decimal_precision"
    COLUMN_IS_DECIMAL_SPECIFIED = "column_is_decimal_specified"

    # Value in set
    COLUMN_VALUE_IN_SET_RULE = "column_value_in_set_rule"
    COLUMN_VALUE_IN_SET_WITH_PREDICATE_RULE = "column_value_in_set_with_predicate_rule"
    COLUMN_VALUE_NOT_IN_SET_RULE = "column_value_not_in_set_rule"
    ORG_ID_HAS_ORG_ROLE_IN_SET = "org_id_has_org_role_in_set"

    # Dates and times comparison
    DATE_ON_OR_AFTER_DATE_RULE = "date_on_or_after_date_rule"
    DATE_BEFORE_OR_ON_DATE_RULE = "date_before_or_on_date_rule"
    DATE_INSIDE_OF_REPORTING_PERIOD_RULE = "date_inside_of_reporting_period_rule"
    DATETIME_BEFORE_OR_ON_DATETIME_RULE = "datetime_before_or_on_datetime_rule"
    TIME_BEFORE_ON_AFTER_RULE = "time_before_on_after_rule"
    CHECK_DATE_IS_BEFORE_CURRENT_DATE = "check_date_is_before_current_date"
    MARK_IF_DATE_IS_INSIDE_REPORTING_PERIOD = "mark_if_date_is_inside_reporting_period"

    # Dates and times validity
    DATE_IS_VALID = "date_is_valid"
    IS_VALID_DATE_COLUMN_RULE = "is_valid_date_column_rule"
    IS_VALID_TIME_COLUMN_RULE = "is_valid_time_column_rule"
    IS_VALID_DATETIME_COLUMN_RULE = "is_valid_datetime_column_rule"
    DATETIME_IS_VALID_WITH_REGEX_EPMA = "datetime_is_valid_with_regex_epma"

    # Valid orgs
    IS_VALID_GP_PRACTICE_CODE = "is_valid_gp_practice_code"
    IS_VALID_GP_PRACTICE_CODE_OR_NULL = "is_valid_gp_practice_code_or_null"
    IS_VALID_POSTCODE = "is_valid_postcode"
    IS_VALID_POSTCODE_OR_NULL = "is_valid_postcode_or_null"
    IS_VALID_SCHOOL = "is_valid_school"
    IS_VALID_ORGANISATION_IDENTIFIER = "is_valid_organisation_identifier"
    IS_VALID_ORGANISATION_IDENTIFIER_OR_NULL = "is_valid_organisation_identifier_or_null"
    IS_VALID_SITE_ID_OR_NULL = "is_valid_site_id_or_null"
    IS_VALID_DEFAULT_ORG = "is_valid_default_org"
    IS_VALID_DEFAULT_ORG_OR_NULL = "is_valid_default_org_or_null"
    IS_VALID_DEFAULT_ORG_OR_NULL_ANYTIME = "is_valid_default_org_or_null_anytime"

    # Valid codes
    IS_VALID_ASSESSMENT_SCALE_CODE_RULE = "is_valid_assessment_scale_code_rule"
    IS_VALID_FIND_CODE_RULE = "is_valid_find_code_rule"
    IS_VALID_DIAG_CODE_RULE = "is_valid_diag_code_rule"
    IS_VALID_SITUATION_CODE_RULE = "is_valid_situation_code_rule"
    IS_VALID_ASSESSMENT_SCALE_SCORE_RULE = "is_valid_assessment_scale_score_rule"
    IS_VALID_SCORE_FORMAT_FOR_SNOMED_CODE = "is_valid_score_format_for_snomed_code"
    IS_VALID_CLINICAL_CODE_RULE = "is_valid_clinical_code_rule"
    IS_VALID_CLUSTER_TOOL_CODE_RULE = "is_valid_cluster_tool_code_rule"
    IS_VALID_CLUSTER_TOOL_SCORE_RULE = "is_valid_cluster_tool_score_rule"
    IS_VALID_SPECIALISED_MENTAL_HEALTH_SERVICE_CATEGORY_CODE_OR_NULL = "is_valid_specialised_mental_health_service_category_code_or_null"
    IS_VALID_PROC_CODE_WITH_PROC_STATUS_RULE = "is_valid_procedure_code_with_procedure_status_rule"

    # Specific entities
    COLUMN_VALUE_VALID_NHS_NUMBER_RULE = "column_value_valid_nhs_number_rule"
    VALID_SNOMED_CT_RULE = "valid_snomed_ct_rule"

    # General
    REFERENTIAL_INTEGRITY_RULE = "referential_integrity_rule"
    REJECT_ALL_RULE = "reject_all_rule"
    COLUMN_COMPARISON_RULE = "column_comparison_rule"
    VALUES_ARE_EQUAL_RULE = "values_are_equal_rule"


class ValidationRule:
    """
    Class specifying a validation to be applied to a dataframe registered to a Spark session
    """
    def __init__(
            self, code: str, table_name: str, condition: Callable[[], Column],
            *required_joins: JoinChain, rule_type=None):
        """
        Args:
            code (str): The unique code for this validation
            table_name (str): The name of the table to apply validations to, as registered to the current Spark session
            condition (Column): A boolean Column expression representing the validation
            required_joins (JoinChain): Optional parameters to join to another table within the Spark session
        """
        self.code = code
        self.table_name = table_name
        self._condition = condition
        self.required_joins = required_joins
        self._type = rule_type

    @property
    def type(self):
        return self._type

    @property
    def condition(self) -> Callable[[], Column]:
        return lambda: coalesce(self._condition(), lit(False))

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "ValidationRule(code={code}, table_name={table_name}, condition={condition}, " \
               "required_joins={required_joins})".format(code=repr(self.code),
                                                         table_name=repr(self.table_name),
                                                         condition=repr(self.condition),
                                                         required_joins=repr(self.required_joins))


class ValidationSeverity:
    """
    Class containing constants representing the severity of validation failures
    """
    WARNING = 0
    SEV_ONE_WARNING = 0
    REJECT_RECORD = 1
    REJECT_GROUP = 2
    REJECT_FILE = 3


class ValidationDetail:
    """
    Class containing the human-relevant details of a validation
    """
    _SEVERITY_TO_MESSAGE_STRING = {
        ValidationSeverity.WARNING: 'Warning',
        ValidationSeverity.SEV_ONE_WARNING: 'Warning',
        ValidationSeverity.REJECT_RECORD: 'Record rejected',
        ValidationSeverity.REJECT_GROUP: 'Group rejected',
        ValidationSeverity.REJECT_FILE: 'File rejected',
    }

    _SEVERITY_TO_MESSAGE_TYPE = {
        ValidationSeverity.WARNING: DQMessageType.Warning,
        ValidationSeverity.SEV_ONE_WARNING: DQMessageType.Warning,
        ValidationSeverity.REJECT_RECORD: DQMessageType.Error,
        ValidationSeverity.REJECT_GROUP: DQMessageType.Error,
        ValidationSeverity.REJECT_FILE: DQMessageType.Error,
    }

    SEVERITY_TO_ACTION = {
        ValidationSeverity.REJECT_RECORD: 'reject_record',
        ValidationSeverity.REJECT_GROUP: 'reject_record',
        ValidationSeverity.REJECT_FILE: 'reject_file',
    }

    def __init__(self, severity: int, message: str, fields: Sequence[TableField] = tuple(),
                 required_joins: AbstractSet[JoinChain] = frozenset()):
        """
        Args:
            severity: The severity of the validation, intended to be a value from ValidationSeverity
            message: The human-readable message for this validation
            fields: The fields whose values are to be reported alongside instances of this validation
            required_joins: The joins required to other tables to populate this validation message
        """
        self._severity = severity
        self._message = message
        self._fields = tuple(fields)
        self._required_joins = frozenset(required_joins)

    @property
    def severity(self) -> int:
        """
        Returns:
            The severity of this validation, a value from ValidationSeverity
        """
        return self._severity

    @property
    def action(self) -> str:
        """
        Returns:
            The action for this validation
        """
        return self.SEVERITY_TO_ACTION[self.severity]

    @property
    def message(self) -> str:
        """
        Returns:
            The human-readable message for this validation
        """
        return self._message

    @property
    def message_type(self) -> str:
        """
        Returns:
            The message type for this validation
        """
        return self._SEVERITY_TO_MESSAGE_TYPE[self.severity]

    @property
    def fields(self) -> Tuple[TableField, ...]:
        """
        Returns:
            The fields whose values are to be reported alongside instances of this validation
        """
        return self._fields

    @property
    def required_joins(self) -> FrozenSet[JoinChain]:
        """
        Returns:
            The joins required to other tables to populate this validation message
        """
        return self._required_joins

    def to_column(self, table_name: Optional[str], code: str) -> Column:
        """
        Retrieve a pyspark Column to report this validation as part of a dataframe

        Args:
            table_name (str): The name of the table this validation targets
            code (str): The code of this validation

        Returns:
            A column expression to build a row for this validation
        """
        return struct(
            lit(table_name).alias(DQFields.TABLE),
            col('RowNumber').astype(StringType()).alias(DQFields.ROW_INDEX),
            lit(None).alias(Common.ATTRIBUTE),  # @TODO: Retrieve from validation detail
            array([lit(f.qualified_name) for f in self.fields]).alias(Common.FIELDS),
            lit(code).alias(Common.CODE),
            self._to_validation_message().alias(Common.MESSAGE),
            self._to_dq_fields().alias(DQFields.FIELD_VALUES),
            lit(self._SEVERITY_TO_MESSAGE_TYPE[self.severity]).alias(Common.TYPE),
            lit(self.SEVERITY_TO_ACTION.get(self.severity)).alias(DQFields.ACTION),
            lit(datetime.utcnow()).alias(DQFields.DQ_TS),
        )

    def _to_validation_message(self) -> Column:
        return concat(lit(self._SEVERITY_TO_MESSAGE_STRING[self.severity]), lit(" - "), lit(self.message))

    def _to_dq_fields(self) -> Column:
        def to_dq_field_key(table_field: TableField) -> Column:
            if table_field.uid and table_field.data_dictionary_name:
                return concat(lit(table_field.uid), lit(' '), lit(table_field.data_dictionary_name))
            if table_field.data_dictionary_name:
                return lit(table_field.data_dictionary_name)
            if table_field.uid:
                return lit(table_field.uid)
            return lit(table_field.qualified_name)

        return create_map([column
                           for field in self.fields
                           for column in (to_dq_field_key(field), col(field.qualified_name).astype(StringType()))
                           ])


class Validation:
    """
    Compound class for coupling a validation rule with its reporting details
    """

    def __init__(self, rule: ValidationRule, detail: ValidationDetail):
        """
        Args:
            rule: The validation rule
            detail: The rule's corresponding details
        """
        self._rule = rule
        self._detail = detail

    @property
    def code(self) -> str:
        """
        Returns:
            The code of the validation
        """
        return self.rule.code

    @property
    def rule(self) -> ValidationRule:
        """
        Returns:
            The validation rule
        """
        return self._rule

    @property
    def detail(self) -> ValidationDetail:
        """
        Returns:
            The details of this validation
        """
        return self._detail

    def to_column(self) -> Column:
        """
        Retrieve a pyspark Column to report this validation as part of a dataframe

        Returns:
            A column expression to build a row for this validation
        """
        return self.detail.to_column(self.rule.table_name, self.code)


class _RulePrecondition:
    """
    Key describing the prerequisites for the application of a validation rule
    """

    def __init__(self, table_name: str, *required_joins: JoinChain):
        """
        Args:
            table_name (str): The name of the table to be validated
            *required_joins (JoinChain): The details of joining to a foreign table for validation, if necessary
        """
        self.table_name = table_name
        self.required_joins = frozenset(required_joins)

    def __eq__(self, other):
        return isinstance(other, _RulePrecondition) \
               and self.table_name == other.table_name \
               and self.required_joins == other.required_joins

    def __lt__(self, other):
        if self.table_name != other.table_name:
            return self.table_name < other.table_name

        return sorted(self.required_joins) < sorted(other.required_joins)

    def __hash__(self):
        return hash((self.table_name, self.required_joins))

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "_RulePrecondition(table_name={table_name}, required_joins={required_joins})".format(
            table_name=repr(self.table_name), required_joins=repr(self.required_joins))

    @staticmethod
    def from_rule(rule: ValidationRule) -> '_RulePrecondition':
        """
        Derive the preconditions from a rule

        Args:
            rule (ValidationRule): The rule to retrieve preconditions from

        Returns:
            _RulePrecondition: The preconditions of the given rule
        """
        return _RulePrecondition(rule.table_name, *rule.required_joins)
