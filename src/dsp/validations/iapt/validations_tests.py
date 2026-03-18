import pytest
from pyspark.sql import SparkSession
from typing import List

from dsp.datasets.pipelines.iapt import SUBMITTED_TABLES, LoadIAPTMetadataStage
from dsp.datasets.validations.iapt.validations import DATAFRAME_VALIDATIONS, REFERENTIAL_INTEGRITY_VALIDATIONS
from dsp.datasets.common import Fields as Common
from dsp.datasets.validations.iapt.validations import ALL_TABLES
from dsp.datasets.validations.iapt.validation_functions import enrich_validation_rules
from dsp.validation.validation_rules import ValidationRuleType
from dsp.validation.validations_tests_helper import filtered_validation_rules, get_dataframes, get_test_file_path, \
    run_validations_unit_test, initialise_unit_test_pipeline
from shared.constants import DS


@pytest.mark.parametrize(["rule_types", "error_count", "input_file", "expected"], [
    ([ValidationRuleType.COLUMN_NOT_BLANK_RULE,
      ValidationRuleType.COLUMN_NOT_BLANK_RULE_WITH_CONDITION], 70, 'blank_rule_input.json', 'blank_rule_expected.json'),
    ([ValidationRuleType.COLUMN_IS_ALPHANUMERIC,
      ValidationRuleType.COLUMN_IS_NUMERIC,
      ValidationRuleType.COLUMN_VALUE_IN_SET_RULE,
      ValidationRuleType.IS_VALID_ORGANISATION_IDENTIFIER_OR_NULL,
      ValidationRuleType.IS_VALID_POSTCODE_OR_NULL,
      ValidationRuleType.IS_VALID_GP_PRACTICE_CODE_OR_NULL,
      ValidationRuleType.IS_VALID_SITE_ID_OR_NULL], 149,
     'format_and_value_in_set_input.json', 'format_and_value_in_set_expected.json'),
    ([ValidationRuleType.COLUMN_VALUE_VALID_NHS_NUMBER_RULE,
      ValidationRuleType.COLUMN_FORMAT_RULE,
      ValidationRuleType.COLUMN_COMPARISON_RULE,
      ValidationRuleType.DATE_BEFORE_OR_ON_DATE_RULE,
      ValidationRuleType.DATE_ON_OR_AFTER_DATE_RULE,
      ValidationRuleType.DATE_INSIDE_OF_REPORTING_PERIOD_RULE,
      ValidationRuleType.IS_VALID_DATE_COLUMN_RULE,
      ValidationRuleType.IS_VALID_TIME_COLUMN_RULE], 109,
     'file_date_value_input.json',
     'file_date_value_expected.json'),
    ([ValidationRuleType.COLUMN_COMPARISON_RULE,
      ValidationRuleType.VALUES_ARE_EQUAL_RULE,
      ValidationRuleType.IS_VALID_DATETIME_COLUMN_RULE], 24,
     'column_comparison_rule_input.json',
     'column_comparison_rule_expected.json'),
    ([ValidationRuleType.IS_VALID_ASSESSMENT_SCALE_CODE_RULE,
      ValidationRuleType.IS_VALID_ASSESSMENT_SCALE_SCORE_RULE,
      ValidationRuleType.IS_VALID_SCORE_FORMAT_FOR_SNOMED_CODE], 11,
     'valid_assessment_scale_code_input.json', 'valid_assessment_scale_code_expected.json'),
    ([ValidationRuleType.IS_VALID_CLINICAL_CODE_RULE], 3,
     'valid_clinical_code_input.json', 'valid_clinical_code_expected.json'),
    ([ValidationRuleType.VALID_SNOMED_CT_RULE], 4,
     'valid_snomed_ct_input.json', 'valid_snomed_ct_expected.json'),
    ([ValidationRuleType.UNIQUENESS_RULE,
      ValidationRuleType.UNIQUENESS_RULE_WITH_CONDITION], 52,
     'uniqueness_rule_input.json', 'uniqueness_rule_expected.json'),
    ([ValidationRuleType.REFERENTIAL_INTEGRITY_RULE], 21,
     'referential_integrity_rule_input.json', 'referential_integrity_rule_expected.json')
])
def test_column_rules(
        spark: SparkSession, temp_dir: str,
        rule_types: List[ValidationRuleType], error_count: int, input_file: str, expected: str):

    dataset = DS.IAPT
    dataframes = get_dataframes(
        spark=spark,
        input_file=get_test_file_path(dataset, input_file),
        convert_to_datetime=True, tables=ALL_TABLES.values()
    )

    pipeline = initialise_unit_test_pipeline(
        validation_rule_data=dataframes,
        load_metadata_stage=LoadIAPTMetadataStage(),
        dataframe_validations=enrich_validation_rules(*filtered_validation_rules(rule_types, DATAFRAME_VALIDATIONS)),
        referential_validations=enrich_validation_rules(
            *filtered_validation_rules(rule_types, REFERENTIAL_INTEGRITY_VALIDATIONS)),
        flowed_dataframes=SUBMITTED_TABLES,
        referential_passthrough_dataframes={*SUBMITTED_TABLES, Common.DQ}
    )

    run_validations_unit_test(spark, dataset, pipeline, temp_dir, expected, error_count)