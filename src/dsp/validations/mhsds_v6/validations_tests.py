import pytest
from pyspark.sql import SparkSession
from typing import List

from dsp.datasets.common import Fields as Common
from dsp.datasets.definitions.mhsds_v6.submission_constants import ALL_TABLES
from dsp.datasets.pipelines.mhsds_v6 import SUBMITTED_TABLES, LoadMHSDSV6MetadataStage
from dsp.datasets.validations.mhsds_v6.validation_functions import enrich_validation_rules
from dsp.datasets.validations.mhsds_v6.validation_messages import _VALIDATION_DETAILS
from dsp.datasets.validations.mhsds_v6.validations import DATAFRAME_VALIDATIONS, REFERENTIAL_INTEGRITY_VALIDATIONS
from dsp.validation.validation_rules import ValidationRuleType
# DO NOT DELETE REQUIRED FOR PYTEST FIXTURE
from dsp.validation.validations_tests_helper import filtered_validation_rules, initialise_unit_test_pipeline, \
    get_dataframes, get_test_file_path, run_validations_unit_test


def test_validations_have_messages():
    for validations_list in (DATAFRAME_VALIDATIONS, REFERENTIAL_INTEGRITY_VALIDATIONS):
        for validation_rule in validations_list:
            assert validation_rule.code in _VALIDATION_DETAILS


def test_validation_codes_match_tables():
    for validation_rule in (*DATAFRAME_VALIDATIONS, *REFERENTIAL_INTEGRITY_VALIDATIONS):
        validation_code = validation_rule.code
        table_code = validation_code[:6]
        if table_code == 'MHSREJ':
            continue
        table = ALL_TABLES[table_code]
        assert table.name == validation_rule.table_name, \
            "Rule {} should be targeting {} but is targeting {}" \
                .format(validation_code, table.name, validation_rule.table_name)


def test_referential_integrity_rules_are_valid():
    # once a table has appeared in the right, it cannot appear as right for referential integrity rules.
    # for exception see below
    rights = []
    for rule in REFERENTIAL_INTEGRITY_VALIDATIONS:
        join_parameters = rule.required_joins[0].join_parameters
        left = join_parameters.left_table
        right = join_parameters.right_table
        rights.append(right)

        # MHS00220 and MHS00145 validate referential integrity between MHS002 and MHS001 on LocalPatientId in both
        # directions, as do MHS00146 and MHS10101 between MHS101 and MHS001, as do MHS50558 and MHS51505 between MHS505 and MHS515. As such, the second of each pair will
        # cause the valid check to fail despite the fact that, being bidirectional, they are valid
        if rule.code not in ['MHS10101', 'MHS00220', 'MHS51505']:
            assert left not in rights

    """
    In no_errors_input, field AssToolCompTimestamp has a value that is one minute after RP Start, and so passes 
    validation MHS60610 for this field being inside RP. 

    However, I have set this field to have timezone offset +1 hour, so if Timestamp fields end up being converted to UTC
    (which we don't want), this field will fail MHS60610, as in UTC it is 23:01 the day before RP Start. When Timestamp
    fields are validated against RP dates, we want to ignore the offset and just validate the date and time as given. 

    "AssToolCompTimestamp":"2000-05-04T00:00:01+01:00" does not and should not fail
    validation MHS60610 that wants it to be inside RP (ie not before "ReportingPeriodStartDate":"2000-05-04").
    """


@pytest.mark.parametrize(["input_file", "expected", "dataset", "error_count", "rule_types"], [
    ('no_errors_input.json', 'no_errors_expected.json', 'mhsds_v6',
     0, [e for e in ValidationRuleType]),

    ("invalid_nhs_number_input.json", "invalid_nhs_number_expected.json", "mhsds_v6",
     2, [ValidationRuleType.COLUMN_VALUE_VALID_NHS_NUMBER_RULE]),

    ('is_valid_datetime_column_input.json', 'is_valid_datetime_column_expected.json', 'mhsds_v6',
     7, [ValidationRuleType.IS_VALID_DATETIME_COLUMN_RULE]),

    ('column_not_blank_input.json', 'column_not_blank_expected.json', 'mhsds_v6',
     161, [ValidationRuleType.COLUMN_NOT_BLANK_RULE]),
])
def test_column_rules_a(
        spark: SparkSession, temp_dir: str, rule_types: List[ValidationRuleType], error_count: int,
        input_file: str, expected: str, dataset: str):
    _test_column_rules(spark, temp_dir, rule_types, error_count, input_file, expected, dataset)


@pytest.mark.parametrize(["input_file", "expected", "dataset", "error_count", "rule_types"], [
    ('referential_integrity_input.json', 'referential_integrity_expected.json', 'mhsds_v6',
     67, [ValidationRuleType.REFERENTIAL_INTEGRITY_RULE]),

    ('invalid_entry_input.json', 'invalid_entry_expected.json', 'mhsds_v6',
     65, [ValidationRuleType.COLUMN_VALUE_IN_SET_RULE,
          ValidationRuleType.COLUMN_VALUE_VALID_NHS_NUMBER_RULE,
          ValidationRuleType.IS_VALID_DATE_COLUMN_RULE,
          ValidationRuleType.COLUMN_COMPARISON_RULE,
          ValidationRuleType.IS_VALID_POSTCODE_OR_NULL,
          ValidationRuleType.COLUMN_FORMAT_RULE,
          ValidationRuleType.IS_VALID_DEFAULT_ORG,
          ValidationRuleType.IS_VALID_DATETIME_COLUMN_RULE,
          ValidationRuleType.IS_VALID_ASSESSMENT_SCALE_CODE_RULE,
          ValidationRuleType.IS_VALID_CLUSTER_TOOL_CODE_RULE,
          ValidationRuleType.VALUES_ARE_EQUAL_RULE]),

])
def test_column_rules_b(
        spark: SparkSession, temp_dir: str, rule_types: List[ValidationRuleType], error_count: int,
        input_file: str, expected: str, dataset: str):
    _test_column_rules(spark, temp_dir, rule_types, error_count, input_file, expected, dataset)


@pytest.mark.parametrize(["input_file", "expected", "dataset", "error_count", "rule_types"], [
    ("reject_all_input.json", 'reject_all_expected.json', 'mhsds_v6',
     1, [ValidationRuleType.REJECT_ALL_RULE]),

    ('snomed_ct_input.json', 'snomed_ct_expected.json', 'mhsds_v6',
     7, [ValidationRuleType.VALID_SNOMED_CT_RULE]),

    ('column_value_in_set_input.json', 'column_value_in_set_expected.json', 'mhsds_v6',
     111, [ValidationRuleType.COLUMN_VALUE_IN_SET_RULE]),

    ('is_valid_clinical_code_input.json', 'is_valid_clinical_code_expected.json', 'mhsds_v6',
     6, [ValidationRuleType.IS_VALID_CLINICAL_CODE_RULE]),

    ('is_valid_date_column_input.json', 'is_valid_date_column_expected.json', 'mhsds_v6',
     100, [ValidationRuleType.IS_VALID_DATE_COLUMN_RULE]),

    ('column_is_numeric_input.json', 'column_is_numeric_expected.json', 'mhsds_v6',
     18, [ValidationRuleType.COLUMN_IS_NUMERIC, ValidationRuleType.COLUMN_IS_DECIMAL_SPECIFIED]),

    ('column_is_alphanumeric_input.json', 'column_is_alphanumeric_expected.json', 'mhsds_v6',
     262, [ValidationRuleType.COLUMN_IS_ALPHANUMERIC]),
])
def test_column_rules_c(
        spark: SparkSession, temp_dir: str, rule_types: List[ValidationRuleType], error_count: int,
        input_file: str, expected: str, dataset: str):
    _test_column_rules(spark, temp_dir, rule_types, error_count, input_file, expected, dataset)


@pytest.mark.parametrize(["input_file", "expected", "dataset", "error_count", "rule_types"], [
    ('date_before_or_on_input.json', 'date_before_or_on_expected.json', 'mhsds_v6',
     126, [ValidationRuleType.DATE_BEFORE_OR_ON_DATE_RULE]),

    ('date_on_or_after_input.json', 'date_on_or_after_expected.json', 'mhsds_v6',
     93, [ValidationRuleType.DATE_ON_OR_AFTER_DATE_RULE]),

    ("valid_score_input.json", "valid_score_expected.json", 'mhsds_v6',
     3, [ValidationRuleType.IS_VALID_ASSESSMENT_SCALE_SCORE_RULE,
         ValidationRuleType.IS_VALID_CLUSTER_TOOL_SCORE_RULE]),
])
def test_column_rules_d(
        spark: SparkSession, temp_dir: str, rule_types: List[ValidationRuleType], error_count: int,
        input_file: str, expected: str, dataset: str):
    _test_column_rules(spark, temp_dir, rule_types, error_count, input_file, expected, dataset)


@pytest.mark.parametrize(["input_file", "expected", "dataset", "error_count", "rule_types"], [
    ("valid_org_id_input.json", 'valid_org_id_expected.json', 'mhsds_v6',
     20, [ValidationRuleType.IS_VALID_ORGANISATION_IDENTIFIER_OR_NULL,
          ValidationRuleType.IS_VALID_DEFAULT_ORG_OR_NULL,
          ValidationRuleType.IS_VALID_SITE_ID_OR_NULL,
          ValidationRuleType.IS_VALID_SCHOOL]),

    ("valid_assessment_scale_code_input.json", "valid_assessment_scale_code_expected.json", 'mhsds_v6',
     3, [ValidationRuleType.IS_VALID_ASSESSMENT_SCALE_CODE_RULE]),

    ("column_format_rule_input.json", "column_format_rule_expected.json", 'mhsds_v6',
     3, [ValidationRuleType.COLUMN_FORMAT_RULE]),
])
def test_column_rules_e(
        spark: SparkSession, temp_dir: str, rule_types: List[ValidationRuleType], error_count: int,
        input_file: str, expected: str, dataset: str):
    _test_column_rules(spark, temp_dir, rule_types, error_count, input_file, expected, dataset)


@pytest.mark.parametrize(["input_file", "expected", "dataset", "error_count", "rule_types"], [
    ("before_datetime_input.json", "before_datetime_expected.json", 'mhsds_v6',
     1, [ValidationRuleType.DATETIME_BEFORE_OR_ON_DATETIME_RULE]),

    ('column_comparison_input.json', 'column_comparison_expected.json', 'mhsds_v6',
     16, [ValidationRuleType.COLUMN_COMPARISON_RULE,
          ValidationRuleType.VALUES_ARE_EQUAL_RULE]),

    ('singleton_input.json', 'singleton_expected.json', 'mhsds_v6',
     2, [ValidationRuleType.SINGLETON_RULE]),

    ('uniqueness_input.json', 'uniqueness_expected.json', 'mhsds_v6',
     82, [ValidationRuleType.UNIQUENESS_RULE, ValidationRuleType.UNIQUENESS_RULE_WITH_CONDITION]),
])
def test_column_rules_f(
        spark: SparkSession, temp_dir: str, rule_types: List[ValidationRuleType], error_count: int,
        input_file: str, expected: str, dataset: str):
    _test_column_rules(spark, temp_dir, rule_types, error_count, input_file, expected, dataset)


def _test_column_rules(
        spark: SparkSession, temp_dir: str, rule_types: List[ValidationRuleType], error_count: int,
        input_file: str, expected: str, dataset: str
):
    dataframes = get_dataframes(
        spark=spark,
        input_file=get_test_file_path(dataset, input_file),
        convert_to_datetime=True,
        tables=ALL_TABLES.values(),
    )

    pipeline = initialise_unit_test_pipeline(
        load_metadata_stage=LoadMHSDSV6MetadataStage(),
        validation_rule_data=dataframes,
        dataframe_validations=enrich_validation_rules(*filtered_validation_rules(
            rule_types, DATAFRAME_VALIDATIONS)),
        referential_validations=enrich_validation_rules(
            *filtered_validation_rules(rule_types, REFERENTIAL_INTEGRITY_VALIDATIONS)),
        flowed_dataframes=SUBMITTED_TABLES,
        referential_passthrough_dataframes={*SUBMITTED_TABLES, Common.DQ}
    )

    run_validations_unit_test(spark, dataset, pipeline, temp_dir, expected, error_count)
