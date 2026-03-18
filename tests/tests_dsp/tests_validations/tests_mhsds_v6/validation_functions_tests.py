from datetime import datetime
from typing import FrozenSet

import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from dsp.common.relational import JoinChain, JoinParameters, Table, Field
from dsp.datasets.common import Fields as Common
from dsp.datasets.definitions.mhsds.mhsds_v6.submission_constants import MHS009, MHS008
from dsp.datasets.models.mhsds_v6 import Header
from dsp.validations.common import DateDelta
from dsp.validations.common_validation_functions import (
    date_before_or_on_date_rule,
    date_on_or_after_date_rule
)
from dsp.validations.mhsds_v6.validation_functions import (
    ValidationSeverity,
    get_required_joins,
    is_valid_assessment_scale_code_rule,
    is_valid_assessment_scale_score_rule,
    is_valid_datetime_column_rule,
    mark_if_date_is_inside_the_reporting_period,
    severity_str
)
from dsp.validations.common_validation_functions import apply_rules


@pytest.mark.parametrize(["severity_code", "expected"], [
    (ValidationSeverity.WARNING, "Warning"),
    (ValidationSeverity.REJECT_RECORD, "Record rejected"),
    (ValidationSeverity.REJECT_GROUP, "Group rejected"),
    (ValidationSeverity.REJECT_FILE, "File rejected")
])
def test_severity_str(severity_code: int, expected: str):
    assert severity_str(severity_code) == expected


@pytest.mark.parametrize(["code", "expected"], [
    ("MHS10104", frozenset()),
    ("MHS00908",
     frozenset({JoinChain(JoinParameters(MHS009, MHS008, (MHS009['CarePlanID'], MHS008['CarePlanID'])))}))
])
def test_get_required_joins(spark: SparkContext, code: str, expected: FrozenSet[JoinChain]):
    assert get_required_joins(code) == expected


def test_is_valid_datetime_column_rule(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("SocPerCircumstanceRecTimestamp", StringType(), True),
    ])

    # '*Timestamp' fields eg 'SocPerCircumstanceRecTimestamp' are
    # StringType and get validated by is_valid_datetime_column_rule.

    # '*Time' fields eg 'ReferClosureTime' are
    # TimestampType and get validated by is_valid_time_column_rule.

    record = [
        ('0', '1880-10-28T17:54:49+00:00'),
        ('1', '1980-10-28T17:54:49+00:00'),
        ('2', '1980-10-28T17:54:49+01:00'),
        ('3', '1980-10-28T17:54:49'),
        ('4', '1980-10-28T17:54:49Z'),
    ]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    apply_rules(spark, is_valid_datetime_column_rule('code', 'testTable', 'SocPerCircumstanceRecTimestamp'))

    validate_row = Row("ID", "SocPerCircumstanceRecTimestamp", Common.DQ)

    actual = sorted(spark.table("testTable").collect(), key=lambda r: r[0])

    assert actual == [
        validate_row('0', '1880-10-28T17:54:49+00:00', ['code']),
        validate_row('1', '1980-10-28T17:54:49+00:00', []),
        validate_row('2', '1980-10-28T17:54:49+01:00', []),
        validate_row('3', '1980-10-28T17:54:49', ['code']),
        validate_row('4', '1980-10-28T17:54:49Z', []),
    ]


def test_is_valid_assessment_scale_code_rule(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("SNOMED", StringType(), True)
    ])

    record = [('0', '1638851000000107'),
              ('1', 'XXXXXXXXXXXXXXX'),
              ('2', None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    table = Table('testTable',
                  Field('SNOMED', str))

    apply_rules(spark, is_valid_assessment_scale_code_rule('code', table['SNOMED']))

    validate_row = Row("ID", "SNOMED", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', '1638851000000107', []),
        validate_row('1', 'XXXXXXXXXXXXXXX', ['code']),
        validate_row('2', None, [])
    ]


def test_is_valid_assessment_scale_score(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("SNOMED", StringType(), True),
        StructField("Score", StringType(), True)

    ])

    record = [
        ('0', '960001000000109', "10"),  # decimal digits allowed is 0 - pass
        ('1', '961031000000108', "10.0"),  # decimal digits allowed is 1 - pass
        ('11', '958051000000104', '10'),  # decimals allowed is 2, but don't have to add em - pass
        ('2', '763264000', "9"), # 9 allowed - pass
        ('3', '718455008', '15.5'), # decimal digits allowed is 0 - Fail
        ('4', '986061000000108', '01'), # leading 0 not allowed -fail
        ('5', '986211000000104', "50.0"),  # score outside upper limit - fail
        ('6', '1323791000000108', '-10'),  # score outside lower limit - fail
        #  These pass this rule but are failed by other rules:
        ('7', None, '10.0'),  # no code provided - pass
        ('8', '1324141000000102', None),  # no score provided - pass
        ('9', 'XXXXXXXXXXXXXXX', "10.0"),  # code not in ass scales list - pass
    ] 

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    table = Table('testTable',
                  Field('SNOMED', str),
                  Field('Score', str))

    apply_rules(spark, is_valid_assessment_scale_score_rule('code', table['SNOMED'], table['Score']))

    validate_row = Row("ID", "SNOMED", "Score", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', '960001000000109', "10", []),
        validate_row('1', '961031000000108', "10.0", []),
        validate_row('11', '958051000000104', '10', []),
        validate_row('2', '763264000', "9", []),
        validate_row('3', '718455008', '15.5', []),
        validate_row('4', '986061000000108', '01', ['code']),
        validate_row('5', '986211000000104', "50.0", ['code']),
        validate_row('6', '1323791000000108', '-10', ['code']),
        validate_row('7', None, '10.0', []),
        validate_row('8', '1324141000000102', None, []),
        validate_row('9', 'XXXXXXXXXXXXXXX', "10.0", []),
    ]


def test_date_on_or_after_date_rule(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("EventDate", TimestampType(), True),
        StructField("ReportingPeriodStartDate", TimestampType(), True)
    ])

    record = [('0', datetime(2018, 2, 12), datetime(2017, 2, 12)),
              ('1', datetime(2018, 2, 12), datetime(2018, 2, 12)),
              ('2', datetime(2017, 2, 12), datetime(2018, 2, 12)),
              ('3', None, datetime(2017, 2, 12)),
              ('4', datetime(2018, 2, 12), None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    table = Table('testTable',
                  Field('EventDate', datetime),
                  Field('ReportingPeriodStartDate', datetime))

    apply_rules(spark, date_on_or_after_date_rule('code', table['EventDate'], table['ReportingPeriodStartDate']))

    validate_row = Row("ID", "EventDate", "ReportingPeriodStartDate", Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', datetime(2018, 2, 12), datetime(2017, 2, 12), []),
        validate_row('1', datetime(2018, 2, 12), datetime(2018, 2, 12), []),
        validate_row('2', datetime(2017, 2, 12), datetime(2018, 2, 12), ['code']),
        validate_row('3', None, datetime(2017, 2, 12), []),
        validate_row('4', datetime(2018, 2, 12), None, []),
    ]


def test_date_before_or_on_date_rule(spark: SparkSession):
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


def test_mark_if_date_is_in_the_reporting_period(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("EventStartDate", TimestampType(), True),
        StructField("EventEndDate", TimestampType(), True),
    ])

    record = [('0', datetime(2017, 2, 12), datetime(2017, 3, 23)),
              ('1', datetime(2017, 5, 23), datetime(2018, 5, 11)),
              ('2', datetime(2018, 2, 12), datetime(2018, 6, 11)),
              ('3', datetime(2018, 9, 13), None),
              ('4', datetime(2019, 1, 1), None),
              ('5', None, None)]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    table = Table('testTable',
                  Field('ID', str),
                  Field('EventStartDate', datetime),
                  Field('EventEndDate', datetime))

    header = Header({
        'ReportingPeriodStartDate': datetime(2018, 1, 1),
        'ReportingPeriodEndDate': datetime(2018, 12, 31),
    })

    spark.createDataFrame([header.as_row(False, False)],
                          Header.get_struct(False, False)).createOrReplaceTempView("MHS000Header")

    apply_rules(spark, mark_if_date_is_inside_the_reporting_period('internal',
                                                                   {table['EventStartDate'], table['EventEndDate']}))

    validate_row = Row("ID", "EventStartDate", 'EventEndDate', Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', datetime(2017, 2, 12), datetime(2017, 3, 23), []),
        validate_row('1', datetime(2017, 5, 23), datetime(2018, 5, 11), ['internal']),
        validate_row('2', datetime(2018, 2, 12), datetime(2018, 6, 11), ['internal']),
        validate_row('3', datetime(2018, 9, 13), None, ['internal']),
        validate_row('4', datetime(2019, 1, 1), None, []),
        validate_row('5', None, None, []),
    ]


def test_mark_if_date_is_in_the_reporting_period_with_offset_day(spark: SparkSession):
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("EventStartDate", TimestampType(), True),
        StructField("EventEndDate", TimestampType(), True),
    ])

    record = [('0', datetime(2017, 2, 12), datetime(2019, 1, 1)),
              ('1', datetime(2017, 2, 12), datetime(2019, 1, 2))]

    spark.createDataFrame(record, schema).createOrReplaceTempView("testTable")

    table = Table('testTable',
                  Field('ID', str),
                  Field('EventStartDate', datetime),
                  Field('EventEndDate', datetime))

    header = Header({
        'ReportingPeriodStartDate': datetime(2018, 1, 1),
        'ReportingPeriodEndDate': datetime(2018, 12, 31),
    })

    spark.createDataFrame([header.as_row(False, False)],
                          Header.get_struct(False, False)).createOrReplaceTempView("MHS000Header")

    apply_rules(spark, mark_if_date_is_inside_the_reporting_period('internal',
                                                                   {table['EventStartDate'], table['EventEndDate']},
                                                                   offset=DateDelta(days=1)))

    validate_row = Row("ID", "EventStartDate", 'EventEndDate', Common.DQ)

    assert sorted(spark.table("testTable").collect(), key=lambda r: r[0]) == [
        validate_row('0', datetime(2017, 2, 12), datetime(2019, 1, 1), ['internal']),
        validate_row('1', datetime(2017, 2, 12), datetime(2019, 1, 2), []),
    ]
