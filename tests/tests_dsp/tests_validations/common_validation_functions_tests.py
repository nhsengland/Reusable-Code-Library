import operator
import pytest
from datetime import datetime
from typing import List, Tuple, Union, Optional, Dict

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import concat, array, col, when, size, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, Row, IntegerType, ArrayType, BooleanType

from dsp.common.regex_patterns import POSTCODE_PATTERN, YYYY_MM_DD_PATTERN
from dsp.common.relational import Table, Field, JoinChain, JoinParameters, TableField
from nhs_reusable_code_library.resuable_codes.spark_helpers import empty_array, to_struct_type, to_struct_field
from dsp.shared.constants import DS

from dsp.datasets.common import Fields as Common, DQMessageType
from dsp.datasets.definitions.mhsds.mhsds_v5.submission_constants import MHS601, MHS009, MHS008
from dsp.dq_files.dq.schema import DQ_MESSAGE_WITH_TIMESTAMP_SCHEMA
from dsp.validations.iapt.validation_functions import uniqueness_rule_iapt_carecontact

from dsp.validations.validation_rules import ValidationRule, _RulePrecondition, ValidationDetail, ValidationSeverity
from dsp.validations.common_validation_functions import (
    apply_rules,
    apply_referential_integrity_rules,
    column_value_in_set_with_predicate_rule,
    column_is_decimal_specified_rule,
    is_valid_gp_practice_code_or_null_rule,
    is_valid_school_or_null_rule,
    is_valid_postcode_or_null_rule,
    is_valid_organisation_identifier_or_null_ignoring_succession_rule,
    is_valid_organisation_identifier_or_null_rule,
    is_valid_organisation_identifier_rule,
    is_valid_site_id_or_null_rule,
    is_valid_site_id_or_null_ignoring_succession_rule,
    is_valid_default_org_or_null_anytime_rule,
    is_valid_default_org_or_null_rule,
    is_valid_snomed_ct_rule,
    cross_table_uniqueness_rule,
    uniqueness_with_condition_rule,
    singleton_rule,
    column_is_null_rule,
    column_is_not_null_rule,
    related_column_is_populated_rule,
    reject_all_rule,
    referential_integrity_rule,
    date_is_valid_rule,
    column_value_valid_nhs_number_rule,
    column_is_decimal_precision_rule,
    uniqueness_rule,
    column_value_not_in_set_rule,
    column_value_in_set_rule,
    column_is_numeric_rule,
    column_is_alphanumeric_rule,
    column_format_rule,
    column_not_blank_rule,
    column_comparison_rule,
    is_valid_date_column_rule,
    is_valid_time_column_rule,
    date_before_or_on_date_rule,
    date_on_or_after_date_rule,
    get_rows_without_consecutive_gp_registrations,
    datetime_before_or_on_datetime_rule,
    date_inside_of_reporting_period_rule,
    is_intersection_between_column_list_and_literal_list,
    values_are_equal_rule,
    is_valid_diag_code_rule,
    referential_integrity_using_is_valid_record_rule,
    is_valid_record,
    related_column_not_blank_rule
)


@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


def test_is_valid_time_column_rule(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("StartDate", TimestampType(), True)

    ])

    record = [
        ('0', None),
        ('1', datetime(2018, 2, 12, 21, 41, 16, 270386))
    ]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, is_valid_time_column_rule('code', 'testTable', 'StartDate'))

    validate_row = Row("ID", "StartDate", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', None, []),
        validate_row('1', datetime(2018, 2, 12, 21, 41, 16, 270386), ["code"])
    ]


def test_is_valid_date_column_rule(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("StartDate", TimestampType(), True)

    ])

    record = [('0', datetime(2018, 2, 12, 00, 00, 00, 00000)),
              ('1', datetime(2018, 2, 12, 21, 41, 16, 270386)),
              ('2', None),
              ('3', datetime(1818, 2, 12, 00, 00, 00, 00000))]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, is_valid_date_column_rule('code', 'testTable', 'StartDate'))

    validate_row = Row("ID", "StartDate", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', datetime(2018, 2, 12, 0, 0), []),
        validate_row('1', datetime(2018, 2, 12, 21, 41, 16, 270386), ["code"]),
        validate_row('2', None, []),
        validate_row('3', datetime(1818, 2, 12, 00, 00, 00, 00000), ["code"])
    ]


def test_date_before_or_on_date_rule_datetimes(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("EventDate", TimestampType(), True),
        StructField("ReportingPeriodEndDate", TimestampType(), True)
    ])

    record = [('0', datetime(2018, 2, 12), datetime(2017, 2, 12)),
              ('1', datetime(2018, 2, 12), datetime(2018, 2, 12)),
              ('2', datetime(2017, 2, 12), datetime(2018, 2, 12)),
              ('3', None, datetime(2017, 2, 12)),
              ('4', datetime(2018, 2, 12), None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    table = Table('testTable',
                  Field('EventDate', datetime),
                  Field('ReportingPeriodEndDate', datetime))

    apply_rules(spark, date_before_or_on_date_rule('code', table['EventDate'], table['ReportingPeriodEndDate']))

    validate_row = Row("ID", "EventDate", "ReportingPeriodEndDate", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', datetime(2018, 2, 12), datetime(2017, 2, 12), ['code']),
        validate_row('1', datetime(2018, 2, 12), datetime(2018, 2, 12), []),
        validate_row('2', datetime(2017, 2, 12), datetime(2018, 2, 12), []),
        validate_row('3', None, datetime(2017, 2, 12), []),
        validate_row('4', datetime(2018, 2, 12), None, []),
    ]


def test_date_before_or_on_date_rule_strings(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("EventDate", StringType(), True),
        StructField("ReportingPeriodEndDate", StringType(), True)
    ])

    record = [('0', 'potato', 'chips'),
              ('1', "2016-01-01", "2016-01-01"),
              ('2', "2016-01-01", "2016-03-01"),
              ('3', "2016-03-01", "2016-01-01"),
              ('4', None, "2016-01-01"),
              ('5', "2016-01-01", None),
              ]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    table = Table('testTable',
                  Field('EventDate', str),
                  Field('ReportingPeriodEndDate', str))

    header = Table('Header',
                   Field('OrgID', str))

    apply_rules(spark, date_before_or_on_date_rule('code', table['EventDate'], table['ReportingPeriodEndDate'], header))

    validate_row = Row("ID", "EventDate", "ReportingPeriodEndDate", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', 'potato', 'chips', []),
        validate_row('1', "2016-01-01", "2016-01-01", []),
        validate_row('2', "2016-01-01", "2016-03-01", []),
        validate_row('3', "2016-03-01", "2016-01-01", ["code"]),
        validate_row('4', None, "2016-01-01", []),
        validate_row('5', "2016-01-01", None, []),
    ]


def test_date_on_or_after_date_rule_datetimes(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("EventDate", TimestampType(), True),
        StructField("ReportingPeriodEndDate", TimestampType(), True)
    ])

    record = [('0', datetime(2018, 2, 12), datetime(2017, 2, 12)),
              ('1', datetime(2018, 2, 12), datetime(2018, 2, 12)),
              ('2', datetime(2017, 2, 12), datetime(2018, 2, 12)),
              ('3', None, datetime(2017, 2, 12)),
              ('4', datetime(2018, 2, 12), None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    table = Table('testTable',
                  Field('EventDate', datetime),
                  Field('ReportingPeriodEndDate', datetime))

    header = Table('Header',
                   Field('OrgID', str))

    apply_rules(spark, date_on_or_after_date_rule('code', table['EventDate'], table['ReportingPeriodEndDate'], header))

    validate_row = Row("ID", "EventDate", "ReportingPeriodEndDate", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', datetime(2018, 2, 12), datetime(2017, 2, 12), []),
        validate_row('1', datetime(2018, 2, 12), datetime(2018, 2, 12), []),
        validate_row('2', datetime(2017, 2, 12), datetime(2018, 2, 12), ["code"]),
        validate_row('3', None, datetime(2017, 2, 12), []),
        validate_row('4', datetime(2018, 2, 12), None, []),
    ]


def test_date_on_or_after_date_rule_strings(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("EventDate", StringType(), True),
        StructField("ReportingPeriodEndDate", StringType(), True)
    ])

    record = [('0', 'potato', 'chips'),
              ('1', "2016-01-01", "2016-01-01"),
              ('2', "2016-01-01", "2016-03-01"),
              ('3', "2016-03-01", "2016-01-01"),
              ('4', None, "2016-01-01"),
              ('5', "2016-01-01", None),
              ]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    table = Table('testTable',
                  Field('EventDate', str),
                  Field('ReportingPeriodEndDate', str))

    header = Table('Header',
                   Field('OrgID', str))

    apply_rules(spark, date_on_or_after_date_rule('code', table['EventDate'], table['ReportingPeriodEndDate'], header))

    validate_row = Row("ID", "EventDate", "ReportingPeriodEndDate", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', 'potato', 'chips', []),
        validate_row('1', "2016-01-01", "2016-01-01", []),
        validate_row('2', "2016-01-01", "2016-03-01", ["code"]),
        validate_row('3', "2016-03-01", "2016-01-01", []),
        validate_row('4', None, "2016-01-01", []),
        validate_row('5', "2016-01-01", None, []),
    ]


def test_datetime_before_or_on_datetime_rule(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("DateTimeDatSetCreate", TimestampType(), True),
        StructField("SubmittedTimestamp", TimestampType(), True)
    ])

    record = [('0', datetime(2018, 2, 12, 0, 0, 0), datetime(2018, 2, 12, 0, 0, 0)),
              ('1', datetime(2018, 2, 12, 12, 34, 56), datetime(2018, 2, 12, 12, 34, 55)),
              ('2', datetime(2017, 2, 12, 12, 34, 56), datetime(2018, 2, 12, 12, 34, 58)),
              ('3', None, datetime(2017, 2, 12, 0, 0, 0)),
              ('4', datetime(2018, 2, 12, 0, 0, 0), None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    table = Table('testTable',
                  Field('DateTimeDatSetCreate', datetime),
                  Field('SubmittedTimestamp', datetime))

    header = Table('Header',
                   Field('OrgID', str))

    apply_rules(spark,
                datetime_before_or_on_datetime_rule('code', table['DateTimeDatSetCreate'], table['SubmittedTimestamp'],
                                                    header))

    validate_row = Row("ID", "DateTimeDatSetCreate", "SubmittedTimestamp", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', datetime(2018, 2, 12, 0, 0, 0), datetime(2018, 2, 12, 0, 0, 0), []),
        validate_row('1', datetime(2018, 2, 12, 12, 34, 56), datetime(2018, 2, 12, 12, 34, 55), ['code']),
        validate_row('2', datetime(2017, 2, 12, 12, 34, 56), datetime(2018, 2, 12, 12, 34, 58), []),
        validate_row('3', None, datetime(2017, 2, 12, 0, 0, 0), []),
        validate_row('4', datetime(2018, 2, 12, 0, 0, 0), None, []),
    ]


@pytest.mark.parametrize(
    "is_string, scenarios",
    [
        (  # Base desired Logic Tests
                False,
                [  # (ID       , GP_ID, Start_date          , End_date,         ,rejected, warned), should_warn
                    # All registrations concurrent
                    (("LPID001", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), False, False), False),
                    (("LPID001", "GP2", datetime(2020, 2, 2), datetime(2020, 3, 1), False, False), False),
                    (("LPID001", "GP3", datetime(2020, 3, 2), None, False, False), False),
                    # End date with non consecutive start date
                    (("LPID002", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), False, False), True),
                    (("LPID002", "GP2", datetime(2020, 2, 3), datetime(2020, 3, 1), False, False), False),
                    (("LPID002", "GP3", datetime(2020, 3, 2), None, False, False), False),
                    # No end date with following start date and end date with no following record
                    (("LPID003", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), False, False), False),
                    (("LPID003", "GP2", datetime(2020, 2, 2), None, False, False), False),
                    (("LPID003", "GP3", datetime(2020, 3, 1), datetime(2020, 3, 30), False, False), True),
                    # Two unset end dates
                    (("LPID004", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), False, False), False),
                    (("LPID004", "GP2", datetime(2020, 2, 2), None, False, False), False),
                    (("LPID004", "GP3", datetime(2020, 3, 1), None, False, False), False),
                    # all registrations concurrent but same GP_ID in a dataset that requires them to be distinct
                    (("LPID005", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), False, False), True),
                    (("LPID005", "GP1", datetime(2020, 2, 2), datetime(2020, 3, 1), False, False), False),
                    (("LPID005", "GP3", datetime(2020, 3, 2), None, False, False), False),
                    # allow non consecutive orgid_gp codes that are the same
                    (("LPID006", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), False, False), False),
                    (("LPID006", "GP2", datetime(2020, 2, 2), datetime(2020, 3, 1), False, False), False),
                    (("LPID006", "GP1", datetime(2020, 3, 2), None, False, False), False),
                    # end date with non consecutive start date and same GP_ID
                    (("LPID007", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), False, False), True),
                    (("LPID007", "GP1", datetime(2020, 2, 3), datetime(2020, 3, 1), False, False), False),
                    (("LPID007", "GP3", datetime(2020, 3, 2), None, False, False), False),
                    # Registration chain concurrent but following record has been rejected
                    (("LPID008", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), False, False), True),
                    (("LPID008", "GP2", datetime(2020, 2, 2), None, True, False), False),
                    # Registration chain concurrent but beginning record has been rejected
                    (("LPID009", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), True, False), False),
                    (("LPID009", "GP2", datetime(2020, 2, 2), None, False, False), False),
                    # Registration chain not concurrent and beginning record has been rejected
                    (("LPID010", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), True, False), True),
                    (("LPID010", "GP2", datetime(2020, 3, 2), None, False, False), False),
                    # Registration chain not concurrent and both record have been rejected
                    (("LPID011", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), True, False), True),
                    (("LPID011", "GP2", datetime(2020, 2, 3), None, True, False), False),
                    # Warnings don't affect logic, rejections do
                    (("LPID012", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), False, False), False),
                    (("LPID012", "GP2", datetime(2020, 2, 2), None, False, False), False),
                    (("LPID013", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), False, True), False),
                    (("LPID013", "GP2", datetime(2020, 2, 2), None, False, True), False),
                    (("LPID014", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), False, False), True),
                    (("LPID014", "GP2", datetime(2020, 2, 2), None, True, False), False),
                    (("LPID015", "GP1", datetime(2020, 1, 1), datetime(2020, 2, 1), False, True), True),
                    (("LPID015", "GP2", datetime(2020, 2, 2), None, True, True), False),
                ],
        ),
        (  # Same tests repeated with Strings as dates for datasets that accept strings as date fields
                True,
                [
                    # All registrations concurrent
                    (("LPID001", "GP1", "2020-01-01", "2020-02-01", False, False), False),
                    (("LPID001", "GP2", "2020-02-02", "2020-03-01", False, False), False),
                    (("LPID001", "GP3", "2020-03-02", None, False, False), False),
                    # End date with non consecutive start date
                    (("LPID002", "GP1", "2020-01-01", "2020-02-01", False, False), True),
                    (("LPID002", "GP2", "2020-02-03", "2020-03-01", False, False), False),
                    (("LPID002", "GP3", "2020-03-02", None, False, False), False),
                    # No end date with following start date and end date with no following record
                    (("LPID003", "GP1", "2020-01-01", "2020-02-01", False, False), False),
                    (("LPID003", "GP2", "2020-02-02", None, False, False), False),
                    (("LPID003", "GP3", "2020-03-01", "2020-03-30 ", False, False), True),
                    # Two unset end dates
                    (("LPID004", "GP1", "2020-01-01", "2020-02-01", False, False), False),
                    (("LPID004", "GP2", "2020-02-02", None, False, False), False),
                    (("LPID004", "GP3", "2020-03-01", None, False, False), False),
                    # all registrations concurrent but same GP_ID in a dataset that requires them to be distinct
                    (("LPID005", "GP1", "2020-01-01", "2020-02-01", False, False), True),
                    (("LPID005", "GP1", "2020-02-02", "2020-03-01", False, False), False),
                    (("LPID005", "GP3", "2020-03-02", None, False, False), False),
                    # allow non consecutive orgid_gp codes that are the same
                    (("LPID006", "GP1", "2020-01-01", "2020-02-01", False, False), False),
                    (("LPID006", "GP2", "2020-02-02", "2020-03-01", False, False), False),
                    (("LPID006", "GP1", "2020-03-02", None, False, False), False),
                    # end date with non consecutive start date and same GP_ID
                    (("LPID007", "GP1", "2020-01-01", "2020-02-01", False, False), True),
                    (("LPID007", "GP1", "2020-02-03", "2020-03-01", False, False), False),
                    (("LPID007", "GP3", "2020-03-02", None, False, False), False),
                    # Registration chain concurrent but following record has been rejected
                    (("LPID008", "GP1", "2020-01-01", "2020-02-01", False, False), True),
                    (("LPID008", "GP2", "2020-02-02", None, True, False), False),
                    # Registration chain concurrent but beginning record has been rejected
                    (("LPID009", "GP1", "2020-01-01", "2020-02-01", True, False), False),
                    (("LPID009", "GP2", "2020-02-02", None, False, False), False),
                    # Registration chain not concurrent and beginning record has been rejected
                    (("LPID010", "GP1", "2020-01-01", "2020-02-01", True, False), True),
                    (("LPID010", "GP2", "2020-03-02", None, False, False), False),
                    # Registration chain not concurrent and both record have been rejected
                    (("LPID011", "GP1", "2020-01-01", "2020-02-01", True, False), True),
                    (("LPID011", "GP2", "2020-02-03", None, True, False), False),
                    # Warnings don't affect logic, rejections do
                    (("LPID012", "GP1", "2020-01-01", "2020-02-01", False, False), False),
                    (("LPID012", "GP2", "2020-02-02", None, False, False), False),
                    (("LPID013", "GP1", "2020-01-01", "2020-02-01", False, True), False),
                    (("LPID013", "GP2", "2020-02-02", None, False, True), False),
                    (("LPID014", "GP1", "2020-01-01", "2020-02-01", False, False), True),
                    (("LPID014", "GP2", "2020-02-02", None, True, False), False),
                    (("LPID015", "GP1", "2020-01-01", "2020-02-01", False, True), True),
                    (("LPID015", "GP2", "2020-02-02", None, True, True), False),
                ],
        ),
        (
                # Tests for string-specific formatting errors
                True,
                [
                    # Registration Chain not concurrent and first record has invalid start_date
                    # We can still determine concurrency here as we have a valid end_date-start_date pair
                    (("LPID001", "GP1", "invalid2020-01-01", "2020-02-01", False, False), True),
                    (("LPID001", "GP2", "2020-02-03", None, False, False), False),
                    # Beginning record in chain has an invalid end date
                    # However we can not generate a warning for it as its end date is not in a format we can understand
                    (("LPID002", "GP1", "2020-02-02", "01-03-2020", False, False), False),
                    (("LPID002", "GP2", "2020-03-03", None, False, False), False),
                    # We can however generate a warning if it is rejected for any other reason
                    (("LPID003", "GP1", "invalid2020-02-02", "2020-03-01", True, False), True),
                    (("LPID003", "GP2", "2020-03-03", None, False, False), False),
                    # Check invalid date handling (DSARM-2476)
                    (("LPID004", "GP1", "20219-03-03", None, False, False), False),
                ],
        ),
    ],
    ids=[
        "Base desired Logic Tests",
        "Same tests repeated with Strings as dates for datasets that accept strings as date fields",
        "Tests for string-specific formatting errors",
    ]
)
def test_get_rows_without_consecutive_gp_registrations(
        is_string: bool,
        scenarios: Union[
            List[Tuple[Tuple[str, str, Optional[datetime], Optional[datetime], bool, bool], bool]],
            List[Tuple[Tuple[str, str, Optional[str], Optional[str], bool, bool], bool]],
        ],
        spark: SparkSession,
):
    reject_validation = ValidationDetail(ValidationSeverity.REJECT_RECORD, "This record has been Rejected")
    warning_validation = ValidationDetail(ValidationSeverity.WARNING, "This record has been Warned")

    schema = StructType(
        [
            StructField("RowNumber", IntegerType(), True),
            StructField("Test_ID", StringType(), True),
            StructField("Test_GP_OrgID", StringType(), True),
            StructField("Test_Start_Date", StringType() if is_string else TimestampType(), True),
            StructField("Test_End_Date", StringType() if is_string else TimestampType(), True),
            StructField("has_been_rejected", BooleanType(), True),
            StructField("has_been_warned", BooleanType(), True),
        ]
    )

    expected_warnings = []
    records = []
    for i, (record, should_warn) in enumerate(scenarios):
        records.append((i, *record))
        if should_warn:
            expected_warnings.append(i)

    assert len(set(expected_warnings)) == len(
        expected_warnings
    ), "duplicate row number, test cases may be badly written"

    df = spark.createDataFrame(records, schema)
    # Initialise DQ Column
    df = df.withColumn(Common.DQ, empty_array(DQ_MESSAGE_WITH_TIMESTAMP_SCHEMA))
    # Add Entries to DQ Column
    df = (
        df.withColumn(
            Common.DQ,
            concat(
                Common.DQ,
                when(
                    col("has_been_rejected") == True, array(reject_validation.to_column("TEST_TABLE", "TEST_CODE_REJ"))
                ).otherwise(empty_array(DQ_MESSAGE_WITH_TIMESTAMP_SCHEMA)),
            ),
        )
            .withColumn(
            Common.DQ,
            concat(
                Common.DQ,
                when(
                    col("has_been_warned") == True, array(warning_validation.to_column("TEST_TABLE", "TEST_CODE_WAR"))
                ).otherwise(empty_array(DQ_MESSAGE_WITH_TIMESTAMP_SCHEMA)),
            ),
        )
            .drop(col("has_been_rejected"))
            .drop(col("has_been_warned"))
    )

    rows_with_warnings = get_rows_without_consecutive_gp_registrations(
        df,
        "Test_ID",
        "Test_GP_OrgID",
        "Test_Start_Date",
        "Test_End_Date",
        "RowNumber",
    )

    assert rows_with_warnings == set(expected_warnings)


def test_date_inside_of_reporting_period_rule(spark: SparkSession):
    table_schema = StructType([
        StructField("ID", StringType(), True),
        StructField("EventDate", TimestampType(), True)
    ])

    table_record = [
        ('0', datetime(2017, 4, 30)),
        ('1', datetime(2017, 5, 1)),
        ('2', datetime(2017, 5, 12)),
        ('3', datetime(2017, 5, 31)),
        ('4', datetime(2017, 6, 1)),
    ]

    spark.createDataFrame(table_record, table_schema).createOrReplaceTempView("testTable")

    table = Table('testTable', Field('EventDate', datetime))

    header_schema = StructType([
        StructField("ReportingPeriodStartDate", TimestampType(), True),
        StructField("ReportingPeriodEndDate", TimestampType(), True)
    ])

    header_record = [(datetime(2017, 5, 1), datetime(2017, 5, 31))]

    spark.createDataFrame(header_record, header_schema).createOrReplaceTempView("testHeader")

    header = Table('testHeader', Field('ReportingPeriodStartDate', datetime), Field('ReportingPeriodEndDate', datetime))

    apply_rules(spark,
                date_inside_of_reporting_period_rule(
                    'code',
                    table['EventDate'],
                    header['ReportingPeriodStartDate'],
                    header['ReportingPeriodEndDate'],
                    header)
                )

    validate_row = Row("ID", "EventDate", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', datetime(2017, 4, 30), ['code']),
        validate_row('1', datetime(2017, 5, 1), []),
        validate_row('2', datetime(2017, 5, 12), []),
        validate_row('3', datetime(2017, 5, 31), []),
        validate_row('4', datetime(2017, 6, 1), ['code']),
    ]


def test_date_inside_of_reporting_period_rule_with_timestamp_strings(spark: SparkSession):
    table_schema = StructType([
        StructField("ID", StringType(), True),
        StructField("EventTimestamp", StringType(), True)
    ])

    table_record = [
        ('0', "2017-04-30T12:00:00Z"),
        ('1', "2017-05-01T12:00:00Z"),
        ('2', "2017-05-12T12:00:00Z"),
        ('3', "2017-05-31T12:00:00Z"),
        ('4', "2017-06-01T12:00:00Z"),
        ('5', "2017-05-01T00:00:00+01:00"),
        ('6', "2017-05-01T01:01:01+02:00"),
        ('7', "2017-06-01T00:00:00+01:00"),
        ('8', "2017-06-01T01:01:01+02:00"),
    ]

    spark.createDataFrame(table_record, table_schema).createOrReplaceTempView("testTable")

    table = Table('testTable', Field('EventTimestamp', str))

    header_schema = StructType([
        StructField("ReportingPeriodStartDate", TimestampType(), True),
        StructField("ReportingPeriodEndDate", TimestampType(), True)
    ])

    header_record = [(datetime(2017, 5, 1), datetime(2017, 5, 31))]

    spark.createDataFrame(header_record, header_schema).createOrReplaceTempView("testHeader")

    header = Table('testHeader', Field('ReportingPeriodStartDate', datetime), Field('ReportingPeriodEndDate', datetime))

    apply_rules(spark,
                date_inside_of_reporting_period_rule(
                    'code',
                    table['EventTimestamp'],
                    header['ReportingPeriodStartDate'],
                    header['ReportingPeriodEndDate'],
                    header)
                )

    validate_row = Row("ID", "EventTimestamp", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', '2017-04-30T12:00:00Z', ['code']),
        validate_row('1', '2017-05-01T12:00:00Z', []),
        validate_row('2', '2017-05-12T12:00:00Z', []),
        validate_row('3', '2017-05-31T12:00:00Z', []),
        validate_row('4', '2017-06-01T12:00:00Z', ['code']),
        validate_row('5', '2017-05-01T00:00:00+01:00', []),
        validate_row('6', '2017-05-01T01:01:01+02:00', []),
        validate_row('7', '2017-06-01T00:00:00+01:00', ['code']),
        validate_row('8', '2017-06-01T01:01:01+02:00', ['code']),
    ]


def test_is_intersection_between_column_list_and_literal_list(spark: SparkSession):
    df = spark.createDataFrame([
        ("0", ["Adams", "Brown", "Chandra"]),
        ("1", ["Dawson", "Ebert"]),
        ("2", []),
        ("3", ["Flanagan", "Kaur", "Trujillo", "Gill"]),
    ], StructType([
        StructField("ID", StringType()),
        StructField("Doctors", ArrayType(StringType()))
    ]))

    senior_doctors = ["Adams", "Kaur", "Trujillo"]

    df = df.withColumn("Senior_doctor_is_present",
                       is_intersection_between_column_list_and_literal_list("Doctors", senior_doctors))

    assert df.select("ID", "Senior_doctor_is_present").collect() == [
        Row(ID='0', Senior_doctor_is_present=True),
        Row(ID='1', Senior_doctor_is_present=False),
        Row(ID='2', Senior_doctor_is_present=False),
        Row(ID='3', Senior_doctor_is_present=True),
    ]

    df = spark.createDataFrame([
        ("4", ["Adams", "Brown", "Chandra"]),
        ("5", ["Dawson", "Ebert"]),
        ("6", []),
    ], StructType([
        StructField("ID", StringType()),
        StructField("Doctors", ArrayType(StringType()))
    ]))

    senior_doctors = []

    df = df.withColumn("Senior_doctor_is_present",
                       is_intersection_between_column_list_and_literal_list("Doctors", senior_doctors))

    assert df.select("ID", "Senior_doctor_is_present").collect() == [
        Row(ID='4', Senior_doctor_is_present=False),
        Row(ID='5', Senior_doctor_is_present=False),
        Row(ID='6', Senior_doctor_is_present=False),
    ]


def test_values_are_equal_rule(spark: SparkSession):
    spark.createDataFrame([
        (0, 123),
        (1, 123),
        (2, 123),
        (3, None),
        (4, None),
    ], ["ID", "Table_1_Value"]).createOrReplaceTempView("Table_1")

    spark.createDataFrame([
        (0, 123),
        (1, 321),
        (2, None),
        (3, 123),
        (4, None),
    ], ["ID", "Table_2_Value"]).createOrReplaceTempView("Table_2")

    table_2 = Table("Table_2", Field("ID", int), Field("Table_2_Value", int))
    table_1 = Table("Table_1", Field("ID", int, foreign=table_2["ID"]), Field("Table_1_Value", str))

    apply_rules(spark, values_are_equal_rule("code", table_1['Table_1_Value'], table_2['Table_2_Value'], DS.MHSDS_V1_TO_V5_AS_V6))

    ValidatedRow = Row("ID", "Table_1_Value", Common.DQ)

    assert sorted(spark.table("Table_1").collect(), key=lambda r: r[0]) == [
        ValidatedRow(0, 123, []),
        ValidatedRow(1, 123, ["code"]),
        ValidatedRow(2, 123, ["code"]),
        ValidatedRow(3, None, ["code"]),
        ValidatedRow(4, None, []),
    ]


def test_is_valid_diag_code_rule(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("DiagSchemeInUse", StringType(), True),
        StructField("PrevDiag", StringType(), True)

    ])

    # DiagSchemeInUse 02 is icd_10_format (alphanumeric and length 4-6)
    # DiagSchemeInUse 06 is snomed_ct_format (numeric and length 6-18)

    record = [
        ('0', '02', None),  # Passes if either is None.
        ('1', '02', "ant"),  # Too short.
        ('2', '02', "idises"),
        ('3', '02', "tablishmentarianism"),  # Too long.
        ('4', '06', None),  # Passes if either is None.
        ('5', '06', "222"),  # Too short and failing snomed validation.
        ('6', '06', "446173005"),
        ('7', '06', "2222222222222222222"),  # Too long and failing snomed validation.
        ('8', '06', "alphanumeric"),  # Not numeric.
        ('9', None, "tablishmentarianism"),  # Passes if either is None.
    ]

    spark.createDataFrame(record, schema).createOrReplaceTempView("MHS601MedHistPrevDiag")

    apply_rules(spark,
                is_valid_diag_code_rule(
                    'MHS60112',
                    MHS601['DiagSchemeInUse'],
                    MHS601['PrevDiag'],
                    DS.MHSDS_V1_TO_V5_AS_V6)
                )

    validate_row = Row("ID", "DiagSchemeInUse", "PrevDiag", Common.DQ)

    assert sorted(spark.table("MHS601MedHistPrevDiag").collect(), key=lambda r: r[0]) == [
        validate_row('0', '02', None, []),
        validate_row('1', '02', "ant", ["MHS60112"]),
        validate_row('2', '02', "idises", []),
        validate_row('3', '02', "tablishmentarianism", ["MHS60112"]),
        validate_row('4', '06', None, []),
        validate_row('5', '06', "222", ["MHS60112"]),
        validate_row('6', '06', "446173005", []),
        validate_row('7', '06', "2222222222222222222", ["MHS60112"]),
        validate_row('8', '06', "alphanumeric", ["MHS60112"]),
        validate_row('9', None, "tablishmentarianism", []),
    ]


def test_uniqueness_rule(spark: SparkSession):
    spark.createDataFrame([
        (1, 1, "Mushroom Kingdom 1"),
        (1, 2, "Underground 1"),
        (2, 1, "Desert 1"),
        (2, 2, "Underground 2"),
        (2, 2, "Desert 2"),
    ], ["World", "Stage", "Name"]).createOrReplaceTempView("test")

    apply_rules(spark, uniqueness_rule("code", "test", {"World", "Stage"}))

    validated_row = Row("World", "Stage", "Name", Common.DQ)

    # Operation does not preserve order - does this matter?
    assert sorted(spark.table("test").collect()) == sorted([
        validated_row(1, 1, "Mushroom Kingdom 1", []),
        validated_row(1, 2, "Underground 1", []),
        validated_row(2, 1, "Desert 1", []),
        validated_row(2, 2, "Underground 2", ["code"]),
        validated_row(2, 2, "Desert 2", ["code"])
    ])


def test_uniqueness_rule_with_nulls_not_allowed(spark: SparkSession):
    spark.createDataFrame([
        (1, 1, "Mushroom Kingdom 1"),
        (1, 2, "Underground 1"),
        (2, 1, "Desert 1"),
        (None, 2, "Underground 2"),
        (None, 2, "Desert 2"),
    ], ["World", "Stage", "Name"]).createOrReplaceTempView("test")

    apply_rules(spark, uniqueness_rule("code", "test", {"World", "Stage"}, nulls_are_unique=False))

    validated_row = Row("World", "Stage", "Name", Common.DQ)

    # Operation does not preserve order - does this matter?
    assert sorted(spark.table("test").collect(), key=lambda x: x.Name) == sorted([
        validated_row(1, 1, "Mushroom Kingdom 1", []),
        validated_row(1, 2, "Underground 1", []),
        validated_row(2, 1, "Desert 1", []),
        validated_row(None, 2, "Underground 2", ["code"]),
        validated_row(None, 2, "Desert 2", ["code"])
    ], key=lambda x: x.Name)


def test_uniqueness_rule_with_nulls(spark: SparkSession):
    spark.createDataFrame([
        (1, 1, "Mushroom Kingdom 1"),
        (1, 2, "Underground 1"),
        (2, 1, "Desert 1"),
        (None, 2, "Underground 2"),
        (None, 2, "Desert 2"),
    ], ["World", "Stage", "Name"]).createOrReplaceTempView("test")

    apply_rules(spark, uniqueness_rule("code", "test", {"World", "Stage"}, nulls_are_unique=True))

    validated_row = Row("World", "Stage", "Name", Common.DQ)

    # Operation does not preserve order - does this matter?
    assert sorted(spark.table("test").collect(), key=lambda x: x.Name) == sorted([
        validated_row(1, 1, "Mushroom Kingdom 1", []),
        validated_row(1, 2, "Underground 1", []),
        validated_row(2, 1, "Desert 1", []),
        validated_row(None, 2, "Underground 2", []),
        validated_row(None, 2, "Desert 2", [])
    ], key=lambda x: x.Name)


def test_uniqueness_rule_with_condition(spark: SparkSession):
    spark.createDataFrame([
        (1, 1, "One-A"),
        (1, 1, "One-B"),
        (2, 1, "Two-A"),
        (2, None, "Two-B"),
        (3, 1, "Three-A"),
        (3, None, "Three-B"),
        (3, None, "Three-C"),
    ], ["X", "Y", "Z"]).createOrReplaceTempView("test")

    apply_rules(
        spark,
        uniqueness_with_condition_rule(
            "code", "test", ("X", "Y"), lambda: col('Y').isNull(), nulls_are_unique=False
        )
    )

    validated_row = Row("X", "Y", "Z", Common.DQ)
    expected = sorted(
        [
            validated_row(1, 1, "One-A", []),
            validated_row(1, 1, "One-B", []),
            validated_row(2, 1, "Two-A", []),
            validated_row(2, None, "Two-B", []),
            validated_row(3, 1, "Three-A", []),
            validated_row(3, None, "Three-B", ["code"]),
            validated_row(3, None, "Three-C", ["code"]),
        ],
        key=lambda x: x.Z)

    actual = sorted(spark.table("test").collect(), key=lambda x: x.Z)

    # Operation does not preserve order - does this matter?
    assert actual == expected


def test_uniqueness_rule_iapt_carecontact(spark: SparkSession):
    spark.createDataFrame([
        (1, 1, "test1", "5"),
        (1, 1, "test1", "6"),
        (2, 2, "test2", "5"),
        (2, 2, "test2", "7"),
        (3, 3, "test3", "7"),
        (3, 3, "test3", "7"),
        (4, 4, "test4", "1"),
        (4, 4, "test4", "8"),
        (1, 1, "test5", "5"),
        (1, 1, "test5", "5"),
        (1, 1, "test6", "6"),
        (1, 1, "test6", "6"),
        (1, 1, "test7", "5"),
        (1, 1, "test7", "6"),
        (1, 1, "test7", "7"),
        (1, 1, "test8", "5"),
        (1, 1, "test8", "5"),
        (1, 1, "test8", "7"),
        (1, 1, "test8", "7"),
        (2, 1, "test9", "5"),
        (1, 1, "test9", "5"),

    ], ["W", "X", "Y", "Z"]).createOrReplaceTempView("test")

    apply_rules(
        spark,
        uniqueness_rule_iapt_carecontact(
            "code", "test", ("W", "X", "Y"), "Z"
        )
    )

    validated_row = Row("W", "X", "Y", "Z", Common.DQ)
    expected = sorted(
        [
            validated_row(1, 1, "test1", "5", ["code"]),
            validated_row(1, 1, "test1", "6", ["code"]),
            validated_row(2, 2, "test2", "5", []),
            validated_row(2, 2, "test2", "7", []),
            validated_row(3, 3, "test3", "7", []),
            validated_row(3, 3, "test3", "7", []),
            validated_row(4, 4, "test4", "1", []),
            validated_row(4, 4, "test4", "8", []),
            validated_row(1, 1, "test5", "5", ["code"]),
            validated_row(1, 1, "test5", "5", ["code"]),
            validated_row(1, 1, "test6", "6", ["code"]),
            validated_row(1, 1, "test6", "6", ["code"]),
            validated_row(1, 1, "test7", "5", ["code"]),
            validated_row(1, 1, "test7", "6", ["code"]),
            validated_row(1, 1, "test7", "7", ["code"]),
            validated_row(1, 1, "test8", "5", ["code"]),
            validated_row(1, 1, "test8", "5", ["code"]),
            validated_row(1, 1, "test8", "7", ["code"]),
            validated_row(1, 1, "test8", "7", ["code"]),
            validated_row(2, 1, "test9", "5", []),
            validated_row(1, 1, "test9", "5", []),
        ],
        key=lambda x: (x["X"], x["Y"]))

    assert sorted(spark.table("test").collect(), key=lambda x: (x["X"], x["Y"])) == expected


def test_uniqueness_rule_with_condition_nulls_unique(spark: SparkSession):
    spark.createDataFrame([
        (1, 1, "One-A"),
        (1, 1, "One-B"),
        (None, 1, "One-C"),
        (None, 1, "One-D"),
        (2, 2, "Two-A"),
        (2, 2, "Two-B")

    ], ["X", "Y", "Z"]).createOrReplaceTempView("test")

    apply_rules(
        spark,
        uniqueness_with_condition_rule(
            "code", "test", ("X", "Y"), lambda: col('Y') == lit(1)
        )
    )

    validated_row = Row("X", "Y", "Z", Common.DQ)
    expected = sorted(
        [
            validated_row(1, 1, "One-A", ["code"]),
            validated_row(1, 1, "One-B", ["code"]),
            validated_row(None, 1, "One-C", []),
            validated_row(None, 1, "One-D", []),
            validated_row(2, 2, "Two-A", []),
            validated_row(2, 2, "Two-B", [])
        ],
        key=lambda x: x.Z)

    # Operation does not preserve order - does this matter?
    assert sorted(spark.table("test").collect(), key=lambda x: x.Z) == expected


def test_uniqueness_rule_with_condition_nulls_not_unique(spark: SparkSession):
    spark.createDataFrame([
        (1, 1, "One-A"),
        (1, 1, "One-B"),
        (None, 1, "One-C"),
        (None, 1, "One-D"),
        (2, 2, "Two-A"),
        (2, 2, "Two-B")

    ], ["X", "Y", "Z"]).createOrReplaceTempView("test")

    apply_rules(
        spark,
        uniqueness_with_condition_rule(
            "code", "test", ("X", "Y"), lambda: col('Y') == lit(1), nulls_are_unique=False
        )
    )

    validated_row = Row("X", "Y", "Z", Common.DQ)
    expected = sorted(
        [
            validated_row(1, 1, "One-A", ["code"]),
            validated_row(1, 1, "One-B", ["code"]),
            validated_row(None, 1, "One-C", ["code"]),
            validated_row(None, 1, "One-D", ["code"]),
            validated_row(2, 2, "Two-A", []),
            validated_row(2, 2, "Two-B", [])
        ],
        key=lambda x: x.Z)

    # Operation does not preserve order - does this matter?
    assert sorted(spark.table("test").collect(), key=lambda x: x.Z) == expected


def test_reject_all_rule(spark: SparkSession):
    spark.createDataFrame([("Desert", "United"),
                           ("Labela", "Kingdom")],
                          ["code", "table_name"]).createOrReplaceTempView("test")
    apply_rules(spark, reject_all_rule("code", "test"))

    validated_row = Row("code", "table_name", Common.DQ)

    assert sorted(spark.table('test').collect()) == sorted([
        validated_row("Labela", "Kingdom", ["code"]),
        validated_row("Desert", "United", ["code"]),
    ])


def test_cross_table_uniqueness_rule(spark: SparkSession):
    table_1 = Table("table_1", Field("RowID", int), Field("UUID", int))
    table_2 = Table("table_2", Field("RowID", int), Field("UID", int))
    spark.createDataFrame([
        (1, 123,),
        (2, 234,),
        (3, 345,),
        (4, None,),
    ], ["RowID", "UUID"]).createOrReplaceTempView("table_1")

    spark.createDataFrame([
        (1, 12300,),
        (2, 34500,),
        (3, 234,),
        (4, None)
    ], ["RowID", "UID"]).createOrReplaceTempView("table_2")

    apply_rules(spark, cross_table_uniqueness_rule("code", table_1, table_2,
                                                   table_1['UUID'], table_2['UID']))

    validated_row = Row("RowID", "UUID", Common.DQ)

    # Operation does not preserve order - does this matter?
    assert sorted(spark.table("table_1").collect()) == sorted([
        validated_row(1, 123, []),
        validated_row(2, 234, ['code']),
        validated_row(3, 345, []),
        validated_row(4, None, []),
    ])


def test_singleton_rule(spark: SparkSession):
    spark.createDataFrame([
        ('row one',),
        ('row two',),
    ], ["Name"]).createOrReplaceTempView("test")

    apply_rules(spark, singleton_rule("code", "test"))

    validated_row = Row("Name", Common.DQ)

    # Operation does not preserve order - does this matter?
    assert sorted(spark.table("test").collect()) == sorted([
        validated_row("row one", ["code"]),
        validated_row("row two", ["code"]),
    ])


def test_referential_integrity_rule_simple(spark: SparkSession):
    left_table = Table("left", Field("ID", int))
    right_table = Table("right", Field("ID", int, foreign=left_table["ID"]))

    schema = StructType([
        StructField('ID', IntegerType()),
        StructField('DQ', ArrayType(StringType())),
    ])
    spark.createDataFrame([
        (1, []),  # a match
        (2, []),  # DQ failure on the foreign table
        (4, []),  # no match
    ], schema).alias("left").createOrReplaceTempView("left")
    spark.createDataFrame([
        (1, []),  # a match
        (2, ["code"]),  # DQ failure on the foreign table
        (3, [])  # no match, but should not be reported - ref integrity rules are not symmetric
    ], schema).alias("right").createOrReplaceTempView("right")

    apply_referential_integrity_rules(spark, referential_integrity_rule("code", left_table, right_table,
                                                                        lambda: (size(Common.DQ) == 0,)))

    validated_row = Row("ID", Common.DQ)

    assert sorted(spark.table("left").collect()) == sorted([
        validated_row(1, []),
        validated_row(2, ["code"]),
        validated_row(4, ["code"])
    ])


def test_referential_integrity_rule_three_levels(spark: SparkSession):
    middle_table = Table("middle", Field("ID", int))
    left_table = Table("left", Field("ID", int, foreign=middle_table["ID"]))
    right_table = Table("right", Field("ID", int, foreign=middle_table["ID"]))

    schema = StructType([
        StructField('ID', IntegerType()),
        StructField('DQ', ArrayType(StringType())),
    ])
    spark.createDataFrame([
        (1, []),  # OK
        (2, []),  # DQ on right table -> fail
        (3, []),  # missing relation between middle and right
        (4, []),  # missing relation between left and middle
    ], schema).alias("left").createOrReplaceTempView("left")
    spark.createDataFrame([
        (1, []),  # OK
        (2, []),  # DQ on right table -> fail
        (3, []),  # missing relation between middle and right
    ], schema).alias("middle").createOrReplaceTempView("middle")
    spark.createDataFrame([
        (1, []),  # OK
        (2, ["code"]),  # DQ on right table -> fail
        (4, []),  # missing relation between left and middle
    ], schema).alias("right").createOrReplaceTempView("right")

    apply_referential_integrity_rules(
        spark,
        referential_integrity_rule(
            "code", middle_table, right_table, lambda: (size(Common.DQ) == 0,)
        ),
        referential_integrity_rule(
            "code", left_table, middle_table, lambda: (size(Common.DQ) == 0,)
        )
    )

    validated_row = Row("ID", Common.DQ)

    assert sorted(spark.table("left").collect()) == sorted([
        validated_row(1, []),
        validated_row(2, ["code"]),
        validated_row(3, ["code"]),
        validated_row(4, ["code"])
    ])


def test_referential_and_non_referential(spark: SparkSession):
    schema_left_before_validations = StructType([
        StructField('ID', IntegerType()),
    ])
    schema_right_before_validations = StructType([
        StructField('ID', IntegerType()),
        StructField('non_null', StringType())
    ])

    left_table = Table("left", Field("ID", int))
    right_table = Table("right", Field("ID", int, foreign=left_table["ID"]), Field("non_null", str))

    spark.createDataFrame([
        (1,),  # OK
        (2,)  # integrity fail, because DQ on the right table
    ], schema_left_before_validations).alias("left").createOrReplaceTempView("left")
    spark.createDataFrame([
        (1, 'Not null'),  # OK
        (2, None)  # integrity fail, because DQ on the right table
    ], schema_right_before_validations).alias("right").createOrReplaceTempView("right")

    # here, both tables will be augmented with DQ fields
    apply_rules(spark, column_is_not_null_rule("code", "right", "non_null"))

    apply_rules(
        spark,
        referential_integrity_rule(
            "code", left_table, right_table, lambda: (size(Common.DQ) == 0,)
        )
    )

    validated_row = Row("ID", Common.DQ)

    assert sorted(spark.table("left").collect()) == sorted([
        validated_row(1, []),
        validated_row(2, ["code"]),
    ])


def test_column_is_not_null_rule(spark: SparkSession):
    spark.createDataFrame([
        (0, 'Present'),
        (1, None),
        (2, "")
    ], ["ID", "Optional"]).createOrReplaceTempView("test")

    apply_rules(spark, column_is_not_null_rule("code", "test", "Optional"))

    validated_row = Row("ID", "Optional", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "Present", []),
        validated_row(1, None, ["code"]),
        validated_row(2, "", [])
    ]


def test_column_is_null_rule(spark: SparkSession):
    spark.createDataFrame([
        (0, 'Present'),
        (1, None),
        (2, "")
    ], ["ID", "Optional"]).createOrReplaceTempView("test")

    apply_rules(spark, column_is_null_rule("code", "test", "Optional"))

    validated_row = Row("ID", "Optional", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "Present", ["code"]),
        validated_row(1, None, []),
        validated_row(2, "", ["code"])
    ]


def test_column_format_rules_postcode(spark: SparkSession):
    spark.createDataFrame([
        (0, 'A1 1AA'),
        (1, 'A11 1AA'),
        (2, 'AA1 1AA'),
        (3, 'A1A 1AA'),
        (4, 'AA1A 1AA'),
        (5, 'AA11AA'),
        (6, 'AA1  1AA'),
        (7, 'AA1 11A')
    ], ["ID", "POSTCODE"]).createOrReplaceTempView("test_postcode")
    apply_rules(spark, column_format_rule(
        "code", "test_postcode", "POSTCODE", POSTCODE_PATTERN, allow_nulls=True))

    validated_row = Row("ID", "POSTCODE", Common.DQ)

    assert spark.table("test_postcode").collect() == [
        validated_row(0, 'A1 1AA', []),
        validated_row(1, 'A11 1AA', []),
        validated_row(2, 'AA1 1AA', []),
        validated_row(3, 'A1A 1AA', []),
        validated_row(4, 'AA1A 1AA', []),
        validated_row(5, 'AA11AA', ['code']),
        validated_row(6, 'AA1  1AA', []),
        validated_row(7, 'AA1 11A', ['code'])
    ]

    TEST_PATTERN = r"^\w{1,2}$"

    spark.createDataFrame([
        (0, 'AA'),
        (1, 'a1'),
        (2, 'x'),
        (3, '123'),
        (4, 'x:')
    ], ["ID", "Value"]).createOrReplaceTempView("test_format")

    apply_rules(spark, column_format_rule(
        "code", "test_format", "Value", TEST_PATTERN))

    validated_row = Row("ID", "Value", Common.DQ)

    assert spark.table("test_format").collect() == [
        validated_row(0, 'AA', []),
        validated_row(1, 'a1', []),
        validated_row(2, 'x', []),
        validated_row(3, '123', ['code']),
        validated_row(4, 'x:', ['code']),
    ]


def test_column_format_rule_allow_nulls(spark: SparkSession):
    spark.createDataFrame([
        (0, "123456"),
        (1, "1234567"),
        (2, None)
    ], ["ID", "Numbers"]).createOrReplaceTempView("test")

    apply_rules(spark, column_format_rule("code", "test", "Numbers", r"^\d{6}$", allow_nulls=True))

    validated_row = Row("ID", "Numbers", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "123456", []),
        validated_row(1, "1234567", ["code"]),
        validated_row(2, None, [])
    ]


def test_column_format_rule_disallow_nulls(spark: SparkSession):
    spark.createDataFrame([
        (0, "123456"),
        (1, "1234567"),
        (2, None)
    ], ["ID", "Numbers"]).createOrReplaceTempView("test")

    apply_rules(spark, column_format_rule("code", "test", "Numbers", r"^\d{6}$", allow_nulls=False))

    validated_row = Row("ID", "Numbers", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "123456", []),
        validated_row(1, "1234567", ["code"]),
        validated_row(2, None, ["code"])
    ]


def test_column_value_in_set_allow_nulls(spark: SparkSession):
    spark.createDataFrame([
        (0, "Yes"),
        (1, "No"),
        (2, "Maybe"),
        (3, None)
    ], ["ID", "Value"]).createOrReplaceTempView("test")

    apply_rules(spark,
                column_value_in_set_rule("code", "test", "Value", {"Yes", "Maybe"}, allow_nulls=True))

    validated_row = Row("ID", "Value", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "Yes", []),
        validated_row(1, "No", ["code"]),
        validated_row(2, "Maybe", []),
        validated_row(3, None, [])
    ]


def test_column_value_in_set_disallow_nulls(spark: SparkSession):
    spark.createDataFrame([
        (0, "Yes"),
        (1, "No"),
        (2, "Maybe"),
        (3, None)
    ], ["ID", "Value"]).createOrReplaceTempView("test")

    apply_rules(spark,
                column_value_in_set_rule("code", "test", "Value", {"Yes", "Maybe"}, allow_nulls=False))

    validated_row = Row("ID", "Value", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "Yes", []),
        validated_row(1, "No", ["code"]),
        validated_row(2, "Maybe", []),
        validated_row(3, None, ["code"])
    ]


def test_column_value_in_set_with_predicate(spark: SparkSession):
    spark.createDataFrame([
        (0, "Value", "PredicateValue"),
        (1, None, "PredicateValue"),
        (2, "Value", None),
        (3, None, None),
        (4, "InvalidValue", "PredicateValue"),
    ], ["ID", "Value", "PredicateValue"]).createOrReplaceTempView("test")

    apply_rules(spark, column_value_in_set_with_predicate_rule(
        "code", "test", "Value", {"Value"}, predicate=lambda: col("PredicateValue").isNotNull()))

    validated_row = Row("ID", "Value", "PredicateValue", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "Value", "PredicateValue", []),
        validated_row(1, None, "PredicateValue", ["code"]),
        validated_row(2, "Value", None, []),
        validated_row(3, None, None, []),
        validated_row(4, "InvalidValue", "PredicateValue", ["code"]),
    ]


def test_column_value_in_set_with_predicate_allow_nulls(spark: SparkSession):
    spark.createDataFrame([
        (0, "Value", "PredicateValue"),
        (1, None, "PredicateValue"),
        (2, "Value", None),
        (3, None, None),
        (4, "InvalidValue", "PredicateValue"),
    ], ["ID", "Value", "PredicateValue"]).createOrReplaceTempView("test")

    apply_rules(spark, column_value_in_set_with_predicate_rule(
        "code", "test", "Value", {"Value"},
        predicate=lambda: col("PredicateValue").isNotNull(),
        allow_nulls=True))

    validated_row = Row("ID", "Value", "PredicateValue", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "Value", "PredicateValue", []),
        validated_row(1, None, "PredicateValue", []),
        validated_row(2, "Value", None, []),
        validated_row(3, None, None, []),
        validated_row(4, "InvalidValue", "PredicateValue", ["code"]),
    ]


def test_column_value_not_in_set_allow_nulls(spark: SparkSession):
    spark.createDataFrame([
        (0, "Yes"),
        (1, "No"),
        (2, "Maybe"),
        (3, None)
    ], ["ID", "Value"]).createOrReplaceTempView("test")

    apply_rules(spark,
                column_value_not_in_set_rule("code", "test", "Value", {"Yes", "Maybe"},
                                             allow_nulls=True))

    validated_row = Row("ID", "Value", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "Yes", ["code"]),
        validated_row(1, "No", []),
        validated_row(2, "Maybe", ["code"]),
        validated_row(3, None, [])
    ]


def test_column_value_not_in_set_disallow_nulls(spark: SparkSession):
    spark.createDataFrame([
        (0, "Yes"),
        (1, "No"),
        (2, "Maybe"),
        (3, None)
    ], ["ID", "Value"]).createOrReplaceTempView("test")

    apply_rules(spark,
                column_value_not_in_set_rule("code", "test", "Value", {"Yes", "Maybe"},
                                             allow_nulls=False))

    validated_row = Row("ID", "Value", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "Yes", ["code"]),
        validated_row(1, "No", []),
        validated_row(2, "Maybe", ["code"]),
        validated_row(3, None, ["code"])
    ]


def test_column_comparison_rule_with_date(spark: SparkSession):
    table_1 = Table("table_1", Field("RowID", int), Field("RPDate", datetime))
    table_2 = Table("table_2", Field("RowID", int, foreign=table_1["RowID"]), Field("DOB", datetime))
    schema1 = StructType([
        StructField("RowID", StringType(), True),
        StructField("RPDate", TimestampType(), True)

    ])
    schema2 = StructType([
        StructField("RowID", StringType(), True),
        StructField("DOB", TimestampType(), True)

    ])
    spark.createDataFrame([
        (1, datetime(2018, 1, 1))], schema1).createOrReplaceTempView("table_1")
    spark.createDataFrame([
        (1, datetime(2018, 1, 2))], schema2).createOrReplaceTempView("table_2")

    apply_rules(
        spark, column_comparison_rule('Code', table_2['DOB'], (table_1['RPDate'],), operator.gt))

    dq_row = Row(Common.DQ)
    assert spark.table('table_2').select(Common.DQ).collect() == [dq_row([])]


Header = Table('Header', Field('HVal', str))
A = Table('A', Field('APK', int), Field('AVal1', str), Field('AVal2', str))
B = Table('B', Field('BPK', int, primary=True), Field('APK', int, foreign=A['APK']), Field('BVal1', str))


@pytest.mark.parametrize(['left_field', 'right_field', 'expected'], [
    (A['AVal1'], A['AVal2'], False),
    (B['BVal1'], A['AVal1'], True),
    (B['BVal1'], A['AVal2'], False),
    (B['BVal1'], Header['HVal'], True),
])
def test_column_comparison_rule(spark: SparkSession, left_field: TableField, right_field: TableField, expected: bool):
    spark.createDataFrame([
        ("Hello",)
    ], ["HVal"]).createOrReplaceTempView("Header")

    spark.createDataFrame([
        (0, "Hello", "World", datetime(2018, 1, 1))
    ], ["APK", "AVal1", "AVal2", "AVal3"]).createOrReplaceTempView("A")

    spark.createDataFrame([
        (0, 0, "Hello", datetime(2018, 1, 1))
    ], ["BPK", "APK", "BVal1", "BVal3"]).createOrReplaceTempView("B")

    apply_rules(spark, column_comparison_rule('Code', left_field, (right_field,), operator.eq,
                                              header_table=Header))

    dq_row = Row(Common.DQ)

    assert spark.table(left_field.table.name).select(Common.DQ).collect() == [dq_row([] if expected else ['Code'])]


def test_valid_nhs_number_rule_allow_nulls(spark: SparkSession):
    spark.createDataFrame([
        (0, 123),
        (1, 4010232137),
        (2, 1234567890),
        (3, None),
        (4, 9686270272),
        (5, 9552062683),
        (6, 9018978914),
        (7, 9018978906),
        (8, 9018978892),
        (9, 9254945013),
        (10, 9254945129),
        (11, 9732552476),
    ], ["ID", "NHSNumber"]).createOrReplaceTempView("test")

    apply_rules(spark, column_value_valid_nhs_number_rule("code", "test", "NHSNumber", allow_nulls=True))

    validated_row = Row("ID", "NHSNumber", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, 123, ["code"]),
        validated_row(1, 4010232137, []),
        validated_row(2, 1234567890, ["code"]),
        validated_row(3, None, []),
        validated_row(4, 9686270272, []),
        validated_row(5, 9552062683, []),
        validated_row(6, 9018978914, []),
        validated_row(7, 9018978906, []),
        validated_row(8, 9018978892, []),
        validated_row(9, 9254945013, []),
        validated_row(10, 9254945129, []),
        validated_row(11, 9732552476, []),
    ]


def test_valid_nhs_number_rule_disallow_nulls(spark: SparkSession):
    spark.createDataFrame([
        (0, 123),
        (1, 4010232137),
        (2, 1234567890),
        (3, None),
    ], ["ID", "NHSNumber"]).createOrReplaceTempView("test")

    apply_rules(spark,
                column_value_valid_nhs_number_rule("code", "test", "NHSNumber", allow_nulls=False))

    validated_row = Row("ID", "NHSNumber", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, 123, ["code"]),
        validated_row(1, 4010232137, []),
        validated_row(2, 1234567890, ["code"]),
        validated_row(3, None, ["code"])
    ]


def test_valid_snomed_code_rule_allow_nulls(spark: SparkSession):
    spark.createDataFrame([
        (0, "961031000000108"),
        (1, "718431003"),
        (2, "Hello world"),
        (3, "123"),
        (4, None)
    ], ["ID", "SNOMED"]).createOrReplaceTempView("test")

    apply_rules(spark, is_valid_snomed_ct_rule(
        "invalid_snomed", 'test', 'SNOMED', min_length=6, max_length=12, allow_nulls=True))

    validated_row = Row("ID", "SNOMED", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "961031000000108", ["invalid_snomed"]),
        validated_row(1, "718431003", []),
        validated_row(2, "Hello world", ["invalid_snomed"]),
        validated_row(3, "123", ["invalid_snomed"]),
        validated_row(4, None, []),
    ]


def test_valid_snomed_code_rule_disallow_nulls(spark: SparkSession):
    too_long_code = str(int(1e57))
    spark.createDataFrame([
        (0, too_long_code),
        (1, "718431003"),
        (2, "Hello world"),
        (3, "123"),
        (4, None),
        (5, "860591000000104:408730004=1035681000000101",),
        (6, "958051000000104:363589002=1035711000000102",),
    ], ["ID", "SNOMED"]).createOrReplaceTempView("test")

    apply_rules(spark, is_valid_snomed_ct_rule("invalid_snomed", 'test', 'SNOMED', allow_nulls=False))

    validated_row = Row("ID", "SNOMED", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, too_long_code, ["invalid_snomed"]),
        validated_row(1, "718431003", []),
        validated_row(2, "Hello world", ["invalid_snomed"]),
        validated_row(3, "123", ["invalid_snomed"]),
        validated_row(4, None, ["invalid_snomed"]),
        validated_row(5, "860591000000104:408730004=1035681000000101", []),
        validated_row(6, "958051000000104:363589002=1035711000000102", []),
    ]


def test_valid_snomed_code_rule_allow_nulls_with_side_condition(spark: SparkSession):
    spark.createDataFrame([
        (0, "961031000000108", "54"),
        (1, "718431003", "47"),
        (2, "Hello world", "04"),
        (3, "123", "06"),
        (4, None, "06")
    ], ["ID", "SNOMED", "SCHEME"]).createOrReplaceTempView("test")

    apply_rules(spark, is_valid_snomed_ct_rule("invalid_snomed", 'test', 'SNOMED', min_length=6,
                                               max_length=12, allow_nulls=True,
                                               side_condition=lambda: col("SCHEME") == lit("06")))

    validated_row = Row("ID", "SNOMED", "SCHEME", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "961031000000108", "54", []),
        validated_row(1, "718431003", "47", []),
        validated_row(2, "Hello world", "04", []),
        validated_row(3, "123", "06", ["invalid_snomed"]),
        validated_row(4, None, "06", []),
    ]


def test_valid_snomed_code_rule_disallow_nulls_with_side_condition(spark: SparkSession):
    spark.createDataFrame([
        (0, "961031000000108", "54"),
        (1, "718431003", "47"),
        (2, "Hello world", "04"),
        (3, "123", "06"),
        (4, None, "06")
    ], ["ID", "SNOMED", "SCHEME"]).createOrReplaceTempView("test")

    apply_rules(spark, is_valid_snomed_ct_rule(
        "invalid_snomed", 'test', 'SNOMED', min_length=6, max_length=12, allow_nulls=False,
        side_condition=lambda: col("SCHEME") == lit("06")))

    validated_row = Row("ID", "SNOMED", "SCHEME", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "961031000000108", "54", []),
        validated_row(1, "718431003", "47", []),
        validated_row(2, "Hello world", "04", []),
        validated_row(3, "123", "06", ["invalid_snomed"]),
        validated_row(4, None, "06", ["invalid_snomed"]),
    ]


def test_valid_post_coordinated_snomed_code(spark: SparkSession):
    spark.createDataFrame([
        (0, "961031000000108"),
        (1, "718431003:363589002=718431003"),
        (2, "Hello world"),
        (3, "123"),
        (4, None)
    ], ["ID", "SNOMED"]).createOrReplaceTempView("test")

    apply_rules(spark, is_valid_snomed_ct_rule(
        "invalid_snomed", 'test', 'SNOMED', min_length=6, max_length=56, allow_nulls=True))

    validated_row = Row("ID", "SNOMED", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "961031000000108", []),
        validated_row(1, "718431003:363589002=718431003", []),
        validated_row(2, "Hello world", ["invalid_snomed"]),
        validated_row(3, "123", ["invalid_snomed"]),
        validated_row(4, None, []),
    ]


def test_invalid_post_coordinated_snomed_code(spark: SparkSession):
    spark.createDataFrame([
        (0, "9610310000001084"),
        (1, "718431003:363589002=718431003"),
        (2, "7184310030:363589002=718431003"),
        (3, "718431003:3635890020=718431003"),
        (4, "718431003:363589002=7184310030"),
        (5, "71843100371843100307184310030:363589002=71843100307184310030")
    ], ["ID", "SNOMED"]).createOrReplaceTempView("test")

    apply_rules(spark, is_valid_snomed_ct_rule(
        "invalid_snomed", 'test', 'SNOMED', min_length=6, max_length=56, allow_nulls=False))

    validated_row = Row("ID", "SNOMED", Common.DQ)

    assert spark.table("test").collect() == [
        validated_row(0, "9610310000001084", ["invalid_snomed"]),
        validated_row(1, "718431003:363589002=718431003", []),
        validated_row(2, "7184310030:363589002=718431003", ["invalid_snomed"]),
        validated_row(3, "718431003:3635890020=718431003", ["invalid_snomed"]),
        validated_row(4, "718431003:363589002=7184310030", ["invalid_snomed"]),
        validated_row(5, "71843100371843100307184310030:363589002=71843100307184310030", ["invalid_snomed"])
    ]


def test_can_produce_arbitrary_rules(spark: SparkSession):
    test1_table = Table("test1", Field("ID", int), Field("Value", bool))
    test2_table = Table("test2", Field("ID", int), Field("Value", bool))

    spark.createDataFrame([
        (0, True),
        (1, False),
        (2, True)
    ], ["ID", "Value"]).createOrReplaceTempView("test1")

    spark.createDataFrame([
        (0, True),
        (1, True)
    ], ["ID", "Value"]).createOrReplaceTempView("test2")

    apply_rules(spark, ValidationRule("code", "test1",
                                      lambda: col("test2.Value").isNull() | col("test1.Value").eqNullSafe(
                                          col("test2.Value")),
                                      JoinChain(JoinParameters(test1_table, test2_table,
                                                               (test1_table["ID"], test2_table["ID"])))))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("test1").collect(), key=lambda r: r[0]) == [
        validated_row(0, True, []),
        validated_row(1, False, ["code"]),
        validated_row(2, True, [])
    ]


def test_is_postcode_valid_or_null(spark: SparkSession):
    spark.createDataFrame([
        (0, 'LS131RE'),
        (1, 'LS131RE'),
        (2, 'LS13 1RE'),
        (3, 'Invalid'),
        (4, None),
        (5, 'LS131RE'),
        (6, 'Invalid'),
        (7, 'ls13 1re'),
        (8, 'xy12 3zz')
    ], ["ID", "Postcode"]).createOrReplaceTempView("postcode_table")

    spark.createDataFrame([
        (0, '1900-01-01'),
        (1, '2018-01-01'),
        (2, '2018-01-01'),
        (3, '2018-01-01'),
        (4, '2018-01-01'),
        (5, None),
        (6, '2018-JAN-01'),
        (7, '2018-01-01'),
        (8, '2018-01-01')
    ], ["ID", "Date"]).createOrReplaceTempView("date_table")

    date_table = Table("date_table", Field("ID", int), Field("Date", str))
    postcode_table = Table("postcode_table", Field("ID", int, foreign=date_table["ID"]), Field("Postcode", str))
    header_table = Table("header_table", Field("A Field", str))

    apply_rules(spark, is_valid_postcode_or_null_rule("code", "Postcode", postcode_table, "Date", date_table,
                                                      header_table, date_pattern=YYYY_MM_DD_PATTERN))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("postcode_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'LS131RE', ["code"]),
        validated_row(1, 'LS131RE', []),
        validated_row(2, 'LS13 1RE', []),
        validated_row(3, 'Invalid', ["code"]),
        validated_row(4, None, []),
        validated_row(5, 'LS131RE', []),
        validated_row(6, 'Invalid', []),
        validated_row(7, 'ls13 1re', ["code"]),
        validated_row(8, 'xy12 3zz', ["code"])
    ]


def test_is_postcode_valid_or_null_with_no_date(spark: SparkSession):
    spark.createDataFrame([
        (0, 'LS131RE'),
        (1, 'Invalid'),
        (2, None)
    ], ["ID", "Postcode"]).createOrReplaceTempView("postcode_table")

    postcode_table = Table("postcode_table", Field("ID", int), Field("Postcode", str))

    apply_rules(spark, is_valid_postcode_or_null_rule("code", "Postcode", postcode_table))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("postcode_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'LS131RE', []),
        validated_row(1, 'Invalid', ["code"]),
        validated_row(2, None, []),
    ]


def test_is_valid_school_or_null(spark: SparkSession):
    spark.createDataFrame([
        (0, 'EE100001'),
        (1, 'EE100001'),
        (2, 'Invalid'),
        (3, None),
        (4, 'EE100001'),
        (5, 'Invalid'),
    ], ["ID", "School"]).createOrReplaceTempView("school_table")

    spark.createDataFrame([
        (0, '1899-01-01'),
        (1, '2018-01-01'),
        (2, '2018-01-01'),
        (3, '2018-01-01'),
        (4, None),
        (5, '2018-JAN-01'),
    ], ["ID", "Date"]).createOrReplaceTempView("date_table")

    date_table = Table("date_table", Field("ID", int), Field("Date", str))
    school_table = Table("school_table", Field("ID", int, foreign=date_table["ID"]), Field("School", str))
    header_table = Table("header_table", Field("A Field", str))

    apply_rules(spark, is_valid_school_or_null_rule("code", "School", school_table, "Date", date_table,
                                                    header_table, YYYY_MM_DD_PATTERN))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("school_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'EE100001', ["code"]),
        validated_row(1, 'EE100001', []),
        validated_row(2, 'Invalid', ["code"]),
        validated_row(3, None, []),
        validated_row(4, 'EE100001', []),
        validated_row(5, 'Invalid', []),
    ]


def test_is_valid_school_or_null_with_no_date(spark: SparkSession):
    spark.createDataFrame([
        (0, 'EE100001'),
        (1, 'Invalid'),
        (2, None),
    ], ["ID", "School"]).createOrReplaceTempView("school_table")

    school_table = Table("school_table", Field("ID", int), Field("School", str))

    apply_rules(spark, is_valid_school_or_null_rule("code", "School", school_table))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("school_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'EE100001', []),
        validated_row(1, 'Invalid', ["code"]),
        validated_row(2, None, []),
    ]


def test_is_valid_default_org_or_null(spark: SparkSession):
    spark.createDataFrame([
        (0, 'R1A'),
        (1, 'INVALID'),
        (2, 'R1A'),
        (3, None),
        (4, 'R1A'),
        (5, 'DEFAULT'),
        (6, 'DEFAULT'),
        (7, 'INVALID'),
    ], ["ID", "Org_Code"]).createOrReplaceTempView("org_code_table")

    spark.createDataFrame([
        (0, '1899-01-01'),
        (1, '2017-01-01'),
        (2, '2017-01-01'),
        (3, '2017-01-01'),
        (4, None),
        (5, None),
        (6, '2017-01-01'),
        (7, '2017-JAN-01'),
    ], ["ID", "Date"]).createOrReplaceTempView("date_table")

    date_table = Table("date_table", Field("ID", int), Field("Date", str))
    org_code_table = Table("org_code_table", Field("ID", int, foreign=date_table["ID"]), Field("Org_Code", str))
    header_table = Table("header_table", Field("A Field", str))

    apply_rules(spark,
                is_valid_default_org_or_null_rule("code", "Org_Code", org_code_table, ['DEFAULT'], "Date",
                                                  date_table, header_table, YYYY_MM_DD_PATTERN))
    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("org_code_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'R1A', ["code"]),
        validated_row(1, 'INVALID', ["code"]),
        validated_row(2, 'R1A', []),
        validated_row(3, None, []),
        validated_row(4, 'R1A', []),
        validated_row(5, 'DEFAULT', []),
        validated_row(6, 'DEFAULT', []),
        validated_row(7, 'INVALID', []),
    ]


def test_is_valid_default_org_or_null_with_no_dates(spark: SparkSession):
    spark.createDataFrame([
        (0, 'DEFAULT'),
        (1, 'INVALID'),
        (2, None),
        (3, 'R1A')
    ], ["ID", "Org_Code"]).createOrReplaceTempView("org_code_table")

    org_code_table = Table("org_code_table", Field("ID", int), Field("Org_Code", str))

    apply_rules(spark, is_valid_default_org_or_null_rule("code", "Org_Code", org_code_table, ['DEFAULT']))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("org_code_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'DEFAULT', []),
        validated_row(1, 'INVALID', ["code"]),
        validated_row(2, None, []),
        validated_row(3, 'R1A', [])
    ]


def test_is_valid_default_org_or_null_anytime_with_no_dates(spark: SparkSession):
    spark.createDataFrame([
        (0, 'DEFAULT'),
        (1, 'INVALID'),
        (2, None),
        (3, 'R1A'),
        (4, 'RW461')
    ], ["ID", "Org_Code"]).createOrReplaceTempView("org_code_table")

    org_code_table = Table("org_code_table", Field("ID", int), Field("Org_Code", str))

    apply_rules(spark, is_valid_default_org_or_null_anytime_rule(
        "code", "Org_Code", org_code_table, ['DEFAULT']))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("org_code_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'DEFAULT', []),
        validated_row(1, 'INVALID', ["code"]),
        validated_row(2, None, []),
        validated_row(3, 'R1A', []),
        validated_row(4, 'RW461', [])
    ]


def test_is_valid_organisation_identifier(spark: SparkSession):
    spark.createDataFrame([
        (0, 'R1A'),
        (1, 'INVALID'),
        (2, 'R1A'),
        (3, None),
        (4, 'R1A'),
        (5, 'RW461'),
        (6, 'INVALID'),
        (7, '01N'),
    ], ["ID", "Org_Code"]).createOrReplaceTempView("org_code_table")

    spark.createDataFrame([
        (0, '1899-01-01'),
        (1, '2017-01-01'),
        (2, '2017-01-01'),
        (3, '2017-01-01'),
        (4, None),
        (5, None),
        (6, '2017-JAN-01'),
        (7, '2017-04-01'),
    ], ["ID", "Date"]).createOrReplaceTempView("date_table")

    date_table = Table("date_table", Field("ID", int), Field("Date", str))
    org_code_table = Table("org_code_table", Field("ID", int, foreign=date_table["ID"]), Field("Org_Code", str))
    header_table = Table("header_table", Field("A Field", str))

    apply_rules(spark, is_valid_organisation_identifier_rule("code", "Org_Code", org_code_table, "Date",
                                                             date_table, header_table, YYYY_MM_DD_PATTERN))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("org_code_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'R1A', ["code"]),
        validated_row(1, 'INVALID', ["code"]),
        validated_row(2, 'R1A', []),
        validated_row(3, None, ["code"]),
        validated_row(4, 'R1A', []),
        validated_row(5, 'RW461', ["code"]),
        validated_row(6, 'INVALID', []),
        validated_row(7, '01N', []),
    ]


def test_is_valid_organisation_identifier_or_null(spark: SparkSession):
    spark.createDataFrame([
        (0, 'R1A'),
        (1, 'INVALID'),
        (2, 'R1A'),
        (3, None),
        (4, 'R1A'),
        (5, 'INVALID'),
        (6, 'r1a'),
        (7, 'x1y'),
        (8, '01N'),
    ], ["ID", "Org_Code"]).createOrReplaceTempView("org_code_table")

    spark.createDataFrame([
        (0, '1899-01-01'),
        (1, '2017-01-01'),
        (2, '2017-01-01'),
        (3, '2017-01-01'),
        (4, None),
        (5, '2017-JAN-01'),
        (6, '2017-01-01'),
        (7, '2017-01-01'),
        (8, '2017-04-01'),
    ], ["ID", "Date"]).createOrReplaceTempView("date_table")

    date_table = Table("date_table", Field("ID", int), Field("Date", str))
    org_code_table = Table("org_code_table", Field("ID", int, foreign=date_table["ID"]), Field("Org_Code", str))
    header_table = Table("header_table", Field("A Field", str))

    apply_rules(spark, is_valid_organisation_identifier_or_null_rule(
        "code", "Org_Code", org_code_table, "Date", date_table, header_table, YYYY_MM_DD_PATTERN))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("org_code_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'R1A', ["code"]),
        validated_row(1, 'INVALID', ["code"]),
        validated_row(2, 'R1A', []),
        validated_row(3, None, []),
        validated_row(4, 'R1A', []),
        validated_row(5, 'INVALID', []),
        validated_row(6, 'r1a', ["code"]),
        validated_row(7, 'x1y', ["code"]),
        validated_row(8, '01N', []),
    ]


def test_is_valid_organisation_identifier_or_null_ignoring_succession(spark: SparkSession):
    spark.createDataFrame([
        (0, 'R1A'),
        (1, 'INVALID'),
        (2, 'R1A'),
        (3, None),
        (4, 'R1A'),
        (5, 'INVALID'),
        (6, 'r1a'),
        (7, 'x1y'),
        (8, '01N'),
    ], ["ID", "Org_Code"]).createOrReplaceTempView("org_code_table")

    spark.createDataFrame([
        (0, '1899-01-01'),
        (1, '2017-01-01'),
        (2, '2017-01-01'),
        (3, '2017-01-01'),
        (4, None),
        (5, '2017-JAN-01'),
        (6, '2017-01-01'),
        (7, '2017-01-01'),
        (8, '2017-04-01'),
    ], ["ID", "Date"]).createOrReplaceTempView("date_table")

    date_table = Table("date_table", Field("ID", int), Field("Date", str))
    org_code_table = Table("org_code_table", Field("ID", int, foreign=date_table["ID"]), Field("Org_Code", str))
    header_table = Table("header_table", Field("A Field", str))

    apply_rules(spark, is_valid_organisation_identifier_or_null_ignoring_succession_rule(
        "code", "Org_Code", org_code_table, "Date", date_table, header_table, YYYY_MM_DD_PATTERN))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("org_code_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'R1A', ["code"]),
        validated_row(1, 'INVALID', ["code"]),
        validated_row(2, 'R1A', []),
        validated_row(3, None, []),
        validated_row(4, 'R1A', []),
        validated_row(5, 'INVALID', []),
        validated_row(6, 'r1a', ["code"]),
        validated_row(7, 'x1y', ["code"]),
        validated_row(8, '01N', ["code"]),
    ]


def test_is_valid_organisation_identifier_with_no_date(spark: SparkSession):
    spark.createDataFrame([
        (0, 'R1A'),
        (1, 'INVALID'),
        (2, None),
    ], ["ID", "Org_Code"]).createOrReplaceTempView("org_code_table")

    org_code_table = Table("org_code_table", Field("ID", int), Field("Org_Code", str))

    apply_rules(spark, is_valid_organisation_identifier_rule("code", "Org_Code", org_code_table))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("org_code_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'R1A', []),
        validated_row(1, 'INVALID', ["code"]),
        validated_row(2, None, ["code"]),
    ]


def test_is_valid_organisation_identifier_or_null_with_no_date(spark: SparkSession):
    spark.createDataFrame([
        (0, 'R1A'),
        (1, 'INVALID'),
        (2, None),
    ], ["ID", "Org_Code"]).createOrReplaceTempView("org_code_table")

    org_code_table = Table("org_code_table", Field("ID", int), Field("Org_Code", str))

    apply_rules(spark, is_valid_organisation_identifier_or_null_rule("code", "Org_Code", org_code_table))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("org_code_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'R1A', []),
        validated_row(1, 'INVALID', ["code"]),
        validated_row(2, None, []),
    ]


def test_is_valid_site_id_or_null(spark: SparkSession):
    valid_site_id = "RH802"
    invalid_site_id = "INVALID"
    valid_default_site_id_1 = "R9998"
    valid_default_site_id_2 = "89999"
    valid_default_site_id_3 = "89997"
    site_id_with_successor = "RR1"

    spark.createDataFrame([
        (0, valid_site_id),
        (1, invalid_site_id),
        (2, valid_default_site_id_1),
        (3, valid_default_site_id_2),
        (4, valid_default_site_id_3),
        (5, None),
        (6, site_id_with_successor),
    ], ["ID", "SiteIDOfTreat"]).createOrReplaceTempView("site_id_table")

    spark.createDataFrame([
        (0, "2017-01-01"),
        (1, "2017-01-01"),
        (2, "2017-01-01"),
        (3, "2017-01-01"),
        (4, "2017-01-01"),
        (5, "2017-01-01"),
        (6, "2018-05-01"),
    ], ["ID", "Date"]).createOrReplaceTempView("date_table")

    date_table = Table("date_table", Field("ID", int), Field("Date", str))
    site_id_table = Table("site_id_table", Field("ID", int, foreign=date_table["ID"]), Field("SiteIDOfTreat", str))
    header_table = Table("header_table", Field("A Field", str))

    apply_rules(spark, is_valid_site_id_or_null_rule("rule applied - site ID is invalid", "SiteIDOfTreat",
                                                     site_id_table, "Date", date_table, header_table,
                                                     YYYY_MM_DD_PATTERN))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("site_id_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, valid_site_id, []),
        validated_row(1, invalid_site_id, ["rule applied - site ID is invalid"]),
        validated_row(2, valid_default_site_id_1, []),
        validated_row(3, valid_default_site_id_2, []),
        validated_row(4, valid_default_site_id_3, []),
        validated_row(5, None, []),
        validated_row(6, site_id_with_successor, []),
    ]


def test_is_valid_site_id_or_null_ignoring_succession(spark: SparkSession):
    valid_site_id = "RR1"
    invalid_site_id = "RR1"
    valid_default_site_id_1 = "R9998"
    valid_default_site_id_2 = "89999"
    valid_default_site_id_3 = "89997"

    spark.createDataFrame([
        (0, valid_site_id),
        (1, invalid_site_id),
        (2, valid_default_site_id_1),
        (3, valid_default_site_id_2),
        (4, valid_default_site_id_3),
        (5, None)
    ], ["ID", "SiteIDOfTreat"]).createOrReplaceTempView("site_id_table")

    spark.createDataFrame([
        (0, "2018-04-01"),
        (1, "2018-05-01"),
        (2, "2018-05-01"),
        (3, "2018-05-01"),
        (4, "2018-05-01"),
        (5, "2018-05-01"),
    ], ["ID", "Date"]).createOrReplaceTempView("date_table")

    date_table = Table("date_table", Field("ID", int), Field("Date", str))
    site_id_table = Table("site_id_table", Field("ID", int, foreign=date_table["ID"]), Field("SiteIDOfTreat", str))
    header_table = Table("header_table", Field("A Field", str))

    apply_rules(
        spark,
        is_valid_site_id_or_null_ignoring_succession_rule(
            "rule applied - site ID is invalid", "SiteIDOfTreat",
            site_id_table, "Date", date_table, header_table,
            YYYY_MM_DD_PATTERN
        )
    )

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("site_id_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, valid_site_id, []),
        validated_row(1, invalid_site_id, ["rule applied - site ID is invalid"]),
        validated_row(2, valid_default_site_id_1, []),
        validated_row(3, valid_default_site_id_2, []),
        validated_row(4, valid_default_site_id_3, []),
        validated_row(5, None, [])
    ]


def test_is_valid_site_id_or_null_with_no_date(spark: SparkSession):
    valid_site_id = "RH802"
    invalid_site_id = "INVALID"
    valid_default_site_id_1 = "R9998"
    valid_default_site_id_2 = "89999"
    valid_default_site_id_3 = "89997"

    spark.createDataFrame([
        (0, valid_site_id),
        (1, invalid_site_id),
        (2, valid_default_site_id_1),
        (3, valid_default_site_id_2),
        (4, valid_default_site_id_3),
        (5, None),
    ], ["ID", "SiteIDOfTreat"]).createOrReplaceTempView("site_id_table")

    site_id_table = Table("site_id_table", Field("ID", int), Field("SiteIDOfTreat", str))

    apply_rules(spark, is_valid_site_id_or_null_rule("rule applied - site ID is invalid",
                                                "SiteIDOfTreat", site_id_table))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("site_id_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, valid_site_id, []),
        validated_row(1, invalid_site_id, ["rule applied - site ID is invalid"]),
        validated_row(2, valid_default_site_id_1, []),
        validated_row(3, valid_default_site_id_2, []),
        validated_row(4, valid_default_site_id_3, []),
        validated_row(5, None, []),
    ]


def test_is_valid_gp_practice_code_or_null(spark: SparkSession):
    spark.createDataFrame([
        (0, 'A81001'),
        (1, 'P12784'),
        (2, 'A81001'),
        (3, None),
        (4, 'a81001'),
        (5, 'Z00660'),
        (6, 'A81001')
    ], ["ID", "GP_Code"]).createOrReplaceTempView("gp_code_table")

    spark.createDataFrame([
        (0, '1899-01-01'),
        (1, '2017-01-01'),
        (2, '2017-01-01'),
        (3, '2017-01-01'),
        (4, None),
        (5, None),
        (6, '2017-JAN-01')
    ], ["ID", "Date"]).createOrReplaceTempView("date_table")

    date_table = Table("date_table", Field("ID", int), Field("Date", str))
    code_table = Table("gp_code_table", Field("ID", int, foreign=date_table["ID"]), Field("GP_Code", str))
    header_table = Table("header_table", Field("A Field", str))

    apply_rules(spark,
                is_valid_gp_practice_code_or_null_rule("code", "GP_Code", code_table, "Date", date_table,
                                                       header_table, YYYY_MM_DD_PATTERN, case_sensitive=True))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("gp_code_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'A81001', ["code"]),
        validated_row(1, 'P12784', ["code"]),
        validated_row(2, 'A81001', []),
        validated_row(3, None, []),
        validated_row(4, 'a81001', ["code"]),
        validated_row(5, 'Z00660', []),
        validated_row(6, 'A81001', [])
    ]


def test_is_valid_gp_practice_code_or_null_no_date_default_case_sensitive(spark: SparkSession):
    spark.createDataFrame([
        (0, 'A81001'),
        (1, 'INVALID'),
        (2, 'a81001'),
        (3, None),
    ], ["ID", "GP_Code"]).createOrReplaceTempView("gp_code_table")

    code_table = Table("gp_code_table", Field("ID", int), Field("GP_Code", str))

    apply_rules(spark, is_valid_gp_practice_code_or_null_rule("code", "GP_Code", code_table))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("gp_code_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'A81001', []),
        validated_row(1, 'INVALID', ["code"]),
        validated_row(2, 'a81001', []),
        validated_row(3, None, []),
    ]

def test_is_valid_gp_practice_code_or_null_no_date_case_sensitive_flag (spark: SparkSession):
    spark.createDataFrame([
        (0, 'A81001'),
        (1, 'a81001'),
        (2, 'INVALID'),
        (3, None),
    ], ["ID", "GP_Code"]).createOrReplaceTempView("gp_code_table")

    code_table = Table("gp_code_table", Field("ID", int), Field("GP_Code", str))

    apply_rules(spark, is_valid_gp_practice_code_or_null_rule("code", "GP_Code", code_table, case_sensitive=True))

    validated_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("gp_code_table").collect(), key=lambda r: r[0]) == [
        validated_row(0, 'A81001', []),
        validated_row(1, 'a81001', ["code"]),
        validated_row(2, 'INVALID', ["code"]),
        validated_row(3, None, []),
    ]


table1 = Table("table1", Field("field1", str))
table2 = Table("table2")
table3 = Table("table3", Field("field1", str))


@pytest.mark.parametrize(["left", "right", "expected"], [
    (_RulePrecondition("table1"), _RulePrecondition("table1"), 0),
    (_RulePrecondition("table1"), _RulePrecondition("table2"), -1),
    (_RulePrecondition("table2"), _RulePrecondition("table1"), 1),
    (_RulePrecondition("table1", JoinChain(JoinParameters(table1, table2))),
     _RulePrecondition("table1", JoinChain(JoinParameters(table1, table2))), 0),
    (_RulePrecondition("table1"), _RulePrecondition("table1", JoinChain(JoinParameters(table1, table2))), -1),
    (_RulePrecondition("table1", JoinChain(JoinParameters(table1, table2))), _RulePrecondition("table1"), 1),
    (_RulePrecondition("table1", JoinChain(JoinParameters(table1, table3))),
     _RulePrecondition("table1",
                       JoinChain(JoinParameters(table1, table3, (table1["field1"], table3["field1"])))), -1)
])
def test_compare_rule_preconditions(left, right, expected):
    def cmp(a, b):
        return (a > b) - (a < b)

    assert cmp(left, right) == expected


def test_is_alphanumeric_allow_nulls(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Value", StringType(), True)

    ])

    record = [('0', '2a'),
              ('1', '222222'),
              ('2', None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, column_is_alphanumeric_rule('code', 'testTable', 'Value', 2, 2, allow_nulls=True))

    validate_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', '2a', []),
        validate_row('1', '222222', ["code"]),
        validate_row('2', None, [])
    ]


def test_is_alphanumeric_disallow_nulls(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Value", StringType(), True)

    ])

    record = [('0', '2a'),
              ('1', '222222'),
              ('2', None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, column_is_alphanumeric_rule('code', 'testTable', 'Value', 2, 2, allow_nulls=False))

    validate_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', '2a', []),
        validate_row('1', '222222', ["code"]),
        validate_row('2', None, ["code"])
    ]


def test_is_alphanumeric_disallow_special_characters(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Value", StringType(), True)

    ])

    record = [
        ('0', '2#'),
        ('1', 'yXhn#9p@'),
        ('2', '1234'),
        ('3', 'yh12'),
    ]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(
        spark,
        column_is_alphanumeric_rule(
            code='code',
            table_name='testTable',
            column='Value',
            allow_nulls=False,
            allow_special_chars=False
        )
    )

    validate_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', '2#', ['code'],),
        validate_row('1', 'yXhn#9p@', ['code'],),
        validate_row('2', '1234', [],),
        validate_row('3', 'yh12', [],),
    ]


def test_is_numeric_allow_negative(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Value", StringType(), True)
    ])

    record = [('0', 'One'),
              ('1', '1'),
              ('2', '-1'),
              ('3', None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, column_is_numeric_rule('code', 'testTable', 'Value',
                                              allow_nulls=False, allow_negative=True))

    validate_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', 'One', ["code"]),
        validate_row('1', '1', []),
        validate_row('2', '-1', []),
        validate_row('3', None, ["code"])
    ], spark.table("testTable").collect()


def test_is_decimal_precision(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Value", StringType(), True)
    ])

    record = [('0', 'One'),
              ('1', '12345.40958730498753098750938'),
              ('2', '2459893.30497'),
              ('3', '15329589324529.0934092'),
              ('4', None),
              ('5', '27.123.456')]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, column_is_decimal_precision_rule('code', 'testTable', 'Value',
                                                        precision=7, allow_nulls=False))

    validate_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', 'One', ["code"]),
        validate_row('1', '12345.40958730498753098750938', []),
        validate_row('2', '2459893.30497', []),
        validate_row('3', '15329589324529.0934092', ["code"]),
        validate_row('4', None, ["code"]),
        validate_row('5', '27.123.456', ["code"])
    ], spark.table("testTable").collect()


def test_is_decimal_specified(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Value", StringType(), True)
    ])

    record = [('0', 'One'),
              ('1', '123.1234'),
              ('2', '12.123'),
              ('3', '1234.1234'),
              ('4', '123.12345'),
              ('5', '1234.12345'),
              ('6', None),
              ('7', "123"),
              ('8', '123.'),
              ('9', '.123')]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, column_is_decimal_specified_rule(
        'code', 'testTable', 'Value', integer_max_digits=3, fraction_max_digits=4, allow_nulls=False))

    validate_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', 'One', ["code"]),
        validate_row('1', '123.1234', []),
        validate_row('2', '12.123', []),
        validate_row('3', '1234.1234', ["code"]),
        validate_row('4', '123.12345', ["code"]),
        validate_row('5', '1234.12345', ["code"]),
        validate_row('6', None, ["code"]),
        validate_row('7', "123", ["code"]),
        validate_row('8', '123.', ["code"]),
        validate_row('9', '.123', ["code"])
    ], spark.table("testTable").collect()

    record = [('0', '13.26'),
              ('1', '1.26'),
              ('2', '9876'),
              ('3', '987.6'),
              ('4', '9.876'),
              ('5', '98.7654321'),
              ('6', '11.'),
              ('7', '11..'),
              ('8', '.11'),
              ('9', '11.11.11'),
              ]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, column_is_decimal_specified_rule(
        'code', 'testTable', 'Value', integer_min_digits=1, integer_max_digits=7, fraction_min_digits=2,
        fraction_max_digits=2, allow_nulls=False))

    validate_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', '13.26', []),
        validate_row('1', '1.26', []),
        validate_row('2', '9876', ["code"]),
        validate_row('3', '987.6', ["code"]),
        validate_row('4', '9.876', ["code"]),
        validate_row('5', '98.7654321', ["code"]),
        validate_row('6', '11.', ["code"]),
        validate_row('7', '11..', ["code"]),
        validate_row('8', '.11', ["code"]),
        validate_row('9', '11.11.11', ["code"])
    ], spark.table("testTable").collect()


def test_is_numeric_allow_decimals(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Value", StringType(), True)

    ])

    record = [('0', 'One'),
              ('1', '1'),
              ('2', '1.0'),
              ('3', None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, column_is_numeric_rule('code', 'testTable', 'Value',
                                              allow_nulls=False, allow_decimals=True))

    validate_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', 'One', ["code"]),
        validate_row('1', '1', []),
        validate_row('2', '1.0', []),
        validate_row('3', None, ["code"])
    ], spark.table("testTable").collect()


def test_is_numeric_allow_nulls(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Value", StringType(), True)

    ])

    record = [('0', 'One'),
              ('1', '1'),
              ('2', '-1'),
              ('3', '1.0'),
              ('4', None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, column_is_numeric_rule('code', 'testTable', 'Value', allow_nulls=True))

    validate_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', 'One', ["code"]),
        validate_row('1', '1', []),
        validate_row('2', '-1', ["code"]),
        validate_row('3', '1.0', ["code"]),
        validate_row('4', None, [])
    ], spark.table("testTable").collect()


def test_in_numeric_disallow_nulls(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Value", StringType(), True)

    ])

    record = [('0', 'One'),
              ('1', '1'),
              ('2', '-1'),
              ('3', '1.0'),
              ('4', None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, column_is_numeric_rule('code', 'testTable', 'Value', allow_nulls=False))

    validate_row = Row("ID", "Value", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', 'One', ["code"]),
        validate_row('1', '1', []),
        validate_row('2', '-1', ["code"]),
        validate_row('3', '1.0', ["code"]),
        validate_row('4', None, ["code"])
    ], spark.table("testTable").collect()


def test_column_not_blank_rule(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Value", StringType(), True)
    ])

    record = [('0', 'One'),
              ('1', ''),
              ('2', '1'),
              ('3', None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, column_not_blank_rule('code', 'testTable', 'Value'))
    validate_row = Row("ID", "Value", Common.DQ)
    print(spark.table("testTable").collect())
    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', 'One', []),
        validate_row('1', '', ['code']),
        validate_row('2', '1', []),
        validate_row('3', None, ['code'])
    ]


def test_related_column_not_blank_rule(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Value", StringType(), True),
        StructField("Value2", StringType(), True),
    ])

    record = [('0', 'One', 'One'),
              ('1', '', ''),
              ('2', '1', ''),
              ('3', None, None),
              ('4', '', '1')]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, related_column_not_blank_rule('code', 'testTable', 'Value', 'Value2'))
    validate_row = Row("ID", "Value", "Value2", Common.DQ)
    print(spark.table("testTable").collect())
    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', 'One', 'One', []),
        validate_row('1', '', '', ['code']),
        validate_row('2', '1', '', []),
        validate_row('3', None, None, ['code']),
        validate_row('4', '', '1', [])
    ]


def test_related_column_is_populated_rule(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Code", StringType(), True),
        StructField("Value", StringType(), True)
    ])

    record = [('0', 'One', 'Two'),
              ('1', '1', "5"),
              ('2', None, "Roy"),
              ('3', None, None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, related_column_is_populated_rule('code', 'testTable', 'Value', 'Code'))
    validate_row = Row("ID", "Code", "Value", Common.DQ)
    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', 'One', 'Two', []),
        validate_row('1', '1', '5', []),
        validate_row('2', None, 'Roy', ["code"]),
        validate_row('3', None, None, [])
    ]


def test_column_value_in_set_with_scheme_rule(spark: SparkSession):
    spark.createDataFrame([
        (0, "Yes", "Y"),
        (1, "No", None),
        (2, "Maybe", "123"),
        (3, None, None),
        (4, "Us", "Roy")
    ], ["ID", "Code", "Value"]).createOrReplaceTempView("test")

    apply_rules(spark,
                column_value_in_set_rule("code", "test", "Code", {"Yes", "Maybe"}, allow_nulls=False))

    validated_row = Row("ID", "Code", "Value", Common.DQ)
    assert spark.table("test").collect() == [
        validated_row(0, "Yes", "Y", []),
        validated_row(1, "No", None, ["code"]),
        validated_row(2, "Maybe", "123", []),
        validated_row(3, None, None, ["code"]),
        validated_row(4, "Us", "Roy", ["code"])
    ]


def test_date_is_valid(spark: SparkSession):
    spark.createDataFrame([
        (0, "2023/12/01 15:30:12"),
        (1, "2023/12/01  15:30:12"),
        (2, " 2023/12/01 15:30:12"),
        (3, "2023/12/01 15:30:12 "),
        (4, "2023/12/01 15:30:12am"),
        (5, "2023/12/01 15:30:12pm"),
        (6, "2023/12/01 15:30:12AM"),
        (7, "2023/12/01 15:30:12PM")
    ], ["id", "value"]).createOrReplaceTempView("test")

    apply_rules(
        spark,
        date_is_valid_rule(
            code="EPMAWSPC2DQ030",
            table_name="test",
            column="value",
            date_format="yyyy/MM/dd",
            allow_nulls=True,
            check_24_hour_clock=True
        )
    )
    validate_row = Row("id", "value", Common.DQ)
    assert spark.table("test").collect() == [
        validate_row(0, "2023/12/01 15:30:12", []),
        validate_row(1, "2023/12/01  15:30:12", []),
        validate_row(2, " 2023/12/01 15:30:12", []),
        validate_row(3, "2023/12/01 15:30:12 ", []),
        validate_row(4, "2023/12/01 15:30:12am", ["EPMAWSPC2DQ030"]),
        validate_row(5, "2023/12/01 15:30:12pm", ["EPMAWSPC2DQ030"]),
        validate_row(6, "2023/12/01 15:30:12AM", ["EPMAWSPC2DQ030"]),
        validate_row(7, "2023/12/01 15:30:12PM", ["EPMAWSPC2DQ030"])
    ]


def test_referential_integrity_rule_using_is_valid_record(spark: SparkSession):
    def dq_with_type(dq_type: Optional[str]) -> \
            Tuple[Optional[str], Optional[str], Optional[str], List[str], str, str, Dict[str, str], str, Optional[
                str], datetime]:
        return None, None, None, [], 'CODE', 'message', {}, dq_type, '', datetime.utcnow()

    spark.createDataFrame([
        ("CPI1", None, None, None, None, None),
        ("CPI2", None, None, None, None, None),
        ("CPI3", None, None, None, None, None),
        ("CPI4", None, None, None, None, None),
    ], to_struct_type(MHS009)).alias(MHS009.name).createOrReplaceTempView(MHS009.name)

    spark.createDataFrame([
        ("CPI1", None, None, None, None, None, None, None, []),
        ("CPI2", None, None, None, None, None, None, None, [dq_with_type(DQMessageType.Error)]),
        ("CPI3", None, None, None, None, None, None, None, [dq_with_type(DQMessageType.Warning)]),
    ], StructType([
        *(to_struct_field(field) for field in MHS008.qualified_fields),
        StructField(Common.DQ, ArrayType(DQ_MESSAGE_WITH_TIMESTAMP_SCHEMA))
    ])).alias(MHS008.name).createOrReplaceTempView(MHS008.name)

    apply_rules(spark, referential_integrity_using_is_valid_record_rule('MHS00901', MHS009, MHS008))

    mhs009_df = spark.table(MHS009.name)
    validated_dataframe = mhs009_df.select(MHS009["CarePlanID"].qualified_name, Common.DQ).collect()
    ValidationRow = Row("CarePlanID", "DQ")
    assert sorted(validated_dataframe) == sorted([
        ValidationRow("CPI1", []),
        ValidationRow("CPI2", ["MHS00901"]),
        ValidationRow("CPI3", []),
        ValidationRow("CPI4", ["MHS00901"])
    ])


def test_is_valid_record(spark: SparkSession):
    def dq_with_type(dq_type: Optional[str]) -> \
            Tuple[Optional[str], Optional[str], Optional[str], List[str], str, str, Dict[str, str], str,
                  Optional[str], datetime]:
        return None, None, None, [], 'CODE', 'message', {}, dq_type, '', datetime.utcnow()

    df = spark.createDataFrame([
        (0, []),
        (1, [dq_with_type(DQMessageType.Warning)]),  # Warning
        (2, [dq_with_type(DQMessageType.Error)]),  # Error
        (3, [dq_with_type(DQMessageType.Warning), dq_with_type(DQMessageType.Error)]),  # Mixed
    ], StructType([
        StructField("ID", IntegerType()),
        StructField(Common.DQ, ArrayType(DQ_MESSAGE_WITH_TIMESTAMP_SCHEMA))
    ]))

    df_with_validity = df.withColumn("Valid", is_valid_record(Common.DQ))

    RowWithValidity = Row("ID", "Valid")

    assert df_with_validity.select("ID", "Valid").collect() == [
        RowWithValidity(0, True),
        RowWithValidity(1, True),
        RowWithValidity(2, False),
        RowWithValidity(3, False),
    ]

