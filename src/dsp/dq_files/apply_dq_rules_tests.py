from datetime import datetime, timedelta
from random import randint

from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

import dsp.pipeline.loading
from dsp.common import canonical_name
from shared.common.test_helpers import basic_metadata
from dsp.dam.dq_errors import DQErrs
from dsp.datasets.common import DQRule, DQMessage, DQMessageType, Fields as Common, UniquenessRule
from dsp.dq.apply_dq_rules import data_dq, uniqueness_dq
from shared.constants import DS, FT


def _string_date_format_check(value):
    if value == 'bad_data':
        return 'Eek Failed'

    return ''


def any_metadata():
    return basic_metadata('any path', DS.DIDS, FT.CSV)


def test_single_dq_check(spark: SparkSession):
    df_raw = spark.createDataFrame(
        [
            Row(i, 'bad_data' if i % 2 == 1 else '2000-01-01') for i in range(10)
        ], StructType([
            StructField('id', IntegerType()),
            StructField('date_of_birth', StringType(), False)
        ])

    )

    df = dsp.pipeline.loading.add_traceability_columns(spark, df_raw, any_metadata())

    dob = 'date_of_birth'
    dob_canonical = canonical_name(dob)
    dq_errs_df, dq_warns_df, dq_pass_df = data_dq(
        df, [DQRule(dob, [dob], True, lambda c: col(c).rlike(r'^\d{4}-(?:0[1-9]|1[0-2])-\d{2}$'),
                    DQErrs.DQ_FMT_yyyyMMdd_dashed)]
    )

    dob_rows = dq_pass_df.select(dq_pass_df.date_of_birth).collect()

    dq_fail_rows = dq_errs_df.collect()

    warnings = dq_warns_df.count()
    assert 0 == warnings

    assert all([row[0] == '2000-01-01' for row in dob_rows])
    assert len(dq_fail_rows) == 5

    for row in dq_fail_rows:
        dq = row[Common.DQ]

        assert 1 == len(dq)

        message = DQMessage(*dq[0])

        assert DQErrs.DQ_FMT_yyyyMMdd_dashed.value
        assert DQMessageType.Error == message.type
        assert dob_canonical == message.attribute
        assert [dob_canonical] == message.fields
        assert DQErrs.DQ_FMT_yyyyMMdd_dashed.label.format(dob_canonical) == message.message


def test_single_typed_dq_check(spark):
    base_date = datetime(2017, 1, 1).date()
    dob = 'date_of_birth'
    dob_canonical = canonical_name(dob)
    event_date = 'event_date'
    event_date_canonical = canonical_name(event_date)

    df_raw = spark.createDataFrame(
        [
            Row(i, (base_date + timedelta(days=randint(0, 30))), (base_date + timedelta(days=randint(0, 30)))) for i in
            range(100)
        ], StructType([
            StructField('id', IntegerType()),
            StructField(dob_canonical, DateType(), False),
            StructField(event_date_canonical, DateType(), False)
        ])
    )

    df = dsp.pipeline.loading.add_traceability_columns(spark, df_raw, any_metadata())

    dq_errs_df, dq_warns_df, dq_pass_df = data_dq(df, [
        DQRule(dob, [dob, event_date], True, lambda c1, c2: col(c1) <= col(c2), DQErrs.DQ_LTE)
    ])

    warnings = dq_warns_df.count()
    assert 0 == warnings

    dq_fail_rows = dq_errs_df.collect()
    dq_pass_rows = dq_pass_df.collect()

    assert len(dq_fail_rows) > 0
    assert len(dq_pass_rows) > 0

    for row in dq_pass_rows:
        assert row[dob_canonical] <= row[event_date_canonical]

    for row in dq_fail_rows:
        dq = row[Common.DQ]

        assert 1 == len(dq)

        message = DQMessage(*dq[0])

        assert DQErrs.DQ_LTE.value
        assert DQMessageType.Error == message.type
        assert dob_canonical == message.attribute
        assert [dob_canonical, event_date_canonical] == message.fields
        assert DQErrs.DQ_LTE.label.format(dob_canonical, event_date_canonical) == message.message


def test_single_dq_check_warning(spark: SparkSession):
    dob = 'date_of_birth'
    dob_canonical = canonical_name(dob)

    df_raw = spark.createDataFrame(
        [
            Row(i, 'bad_data' if i % 2 == 1 else '2000-01-01') for i in range(10)
        ], StructType([
            StructField('id', IntegerType()),
            StructField(dob, StringType(), False)
        ])

    )

    df = dsp.pipeline.loading.add_traceability_columns(spark, df_raw, any_metadata())

    dq_errs_df, dq_warns_df, dq_pass_df = data_dq(
        df, [DQRule(dob, [dob], False, lambda c: col(c).rlike(r'^\d{4}-(?:0[1-9]|1[0-2])-\d{2}$'),
                    DQErrs.DQ_FMT_yyyyMMdd_dashed)]
    )

    dob_rows = dq_pass_df.select(dq_pass_df.date_of_birth).collect()

    dq_rows = dq_warns_df.collect()

    errors = dq_errs_df.count()
    assert 0 == errors

    assert 10 == len(dob_rows)
    assert len(dq_rows) == 5

    for row in dq_rows:
        dq = row[Common.DQ]

        assert 1 == len(dq)

        message = DQMessage(*dq[0])

        assert DQErrs.DQ_FMT_yyyyMMdd_dashed.value
        assert DQMessageType.Warning == message.type
        assert dob_canonical == message.attribute
        assert [dob_canonical] == message.fields
        assert DQErrs.DQ_FMT_yyyyMMdd_dashed.label.format(dob_canonical) == message.message


def test_single_dq_check_warning_or_errors(spark: SparkSession):
    dob = 'date_of_birth'
    dob2 = 'date_of_birth2'
    dob_canonical = canonical_name(dob)
    dob2_canonical = canonical_name(dob2)

    df_raw = spark.createDataFrame(
        [
            Row(i, 'bad_data' if i % 2 == 1 else '2000-01-01', '2000-01-01' if i % 2 == 1 else 'bad_data') for i in
            range(10)
        ], StructType([
            StructField('id', IntegerType()),
            StructField(dob, StringType(), False),
            StructField(dob2, StringType(), False),
        ])

    )

    df = dsp.pipeline.loading.add_traceability_columns(spark, df_raw, any_metadata())

    dq_errs_df, dq_warns_df, dq_pass_df = data_dq(
        df,
        [
            DQRule(dob, [dob], False, lambda c: col(c).rlike(r'^\d{4}-(?:0[1-9]|1[0-2])-\d{2}$'),
                   DQErrs.DQ_FMT_yyyyMMdd_dashed),
            DQRule(dob2, [dob2], True, lambda c: col(c).rlike(r'^\d{4}-(?:0[1-9]|1[0-2])-\d{2}$'),
                   DQErrs.DQ_FMT_yyyyMMdd_dashed)
        ]
    )

    dq_warns = dq_warns_df.collect()
    dq_errs = dq_errs_df.collect()

    assert 5 == len(dq_errs)
    assert 5 == len(dq_warns)

    for row in dq_warns:
        dq = row[Common.DQ]

        assert 1 == len(dq)

        message = DQMessage(*dq[0])

        assert DQErrs.DQ_FMT_yyyyMMdd_dashed.value
        assert DQMessageType.Warning == message.type
        assert dob_canonical == message.attribute
        assert [dob_canonical] == message.fields
        assert DQErrs.DQ_FMT_yyyyMMdd_dashed.label.format(dob_canonical) == message.message

    for row in dq_errs:
        dq = row[Common.DQ]

        assert 1 == len(dq)

        message = DQMessage(*dq[0])

        assert DQErrs.DQ_FMT_yyyyMMdd_dashed.value
        assert DQMessageType.Error == message.type
        assert dob2_canonical == message.attribute
        assert [dob2_canonical] == message.fields
        assert DQErrs.DQ_FMT_yyyyMMdd_dashed.label.format(dob2_canonical) == message.message


def test_single_dq_check_warning_and_errors(spark: SparkSession):
    dob = 'date_of_birth'
    dob2 = 'date_of_birth2'
    dob_canonical = canonical_name(dob)
    dob2_canonical = canonical_name(dob2)

    df_raw = spark.createDataFrame(
        [
            Row(i, 'bad_data' if i % 2 == 1 else '2000-01-01', 'bad_data' if i % 2 == 1 else '2000-01-01') for i in
            range(10)
        ], StructType([
            StructField('id', IntegerType()),
            StructField(dob, StringType(), False),
            StructField(dob2, StringType(), False),
        ])

    )

    df = dsp.pipeline.loading.add_traceability_columns(spark, df_raw, any_metadata())

    dq_errs_df, dq_warns_df, dq_pass_df = data_dq(
        df,
        [
            DQRule(dob, [dob], False, lambda c: col(c).rlike(r'^\d{4}-(?:0[1-9]|1[0-2])-\d{2}$'),
                   DQErrs.DQ_FMT_yyyyMMdd_dashed),
            DQRule(dob2, [dob2], True, lambda c: col(c).rlike(r'^\d{4}-(?:0[1-9]|1[0-2])-\d{2}$'),
                   DQErrs.DQ_FMT_yyyyMMdd_dashed)
        ]
    )

    dq_warns = dq_warns_df.collect()
    dq_errs = dq_errs_df.collect()

    assert 5 == len(dq_errs)
    assert 0 == len(dq_warns)

    for row in dq_errs:
        dq = row[Common.DQ]

        assert 2 == len(dq)

        message = DQMessage(*dq[0])

        assert DQErrs.DQ_FMT_yyyyMMdd_dashed.value
        assert DQMessageType.Error == message.type
        assert dob2_canonical == message.attribute
        assert [dob2_canonical] == message.fields
        assert DQErrs.DQ_FMT_yyyyMMdd_dashed.label.format(dob2_canonical) == message.message

        message = DQMessage(*dq[1])

        assert DQErrs.DQ_FMT_yyyyMMdd_dashed.value
        assert DQMessageType.Warning == message.type
        assert dob_canonical == message.attribute
        assert [dob_canonical] == message.fields
        assert DQErrs.DQ_FMT_yyyyMMdd_dashed.label.format(dob_canonical) == message.message


def test_uniqueness_dq_no_repeated_records(spark: SparkSession):
    df_raw = spark.createDataFrame(
        [
            ['LS1 2AB', 1, 'A'],
            ['LS1 2AB', 2, 'B'],
            ['LS1 2AB', 3, 'C'],
            ['LS1 3CD', 4, 'D'],
        ],
        StructType([
            StructField('postcode', StringType()),
            StructField('count', IntegerType()),
            StructField('code', StringType()),
        ])
    )

    df = dsp.pipeline.loading.add_traceability_columns(spark, df_raw, any_metadata())
    uniqueness_rule = UniquenessRule(['postcode', 'count'], DQErrs.DQ_NOT_UNIQUE)
    uniqueness_errors_df, uniqueness_pass_df = uniqueness_dq(df, uniqueness_rule)
    dq_errs = uniqueness_errors_df.collect()
    dq_pass = uniqueness_pass_df.collect()

    assert 0 == len(dq_errs)
    assert 4 == len(dq_pass)


def test_uniqueness_dq_repeated_records(spark: SparkSession):
    df_raw = spark.createDataFrame(
        [
            ['LS1 2AB', 1, 'A'],
            ['LS1 2AB', 1, 'B'],
            ['LS1 2AB', 2, 'C'],
            ['LS1 3CD', 1, 'D'],
            ['LS1 2AB', None, 'C'],
            ['LS1 2AB', None, 'C'],
            [None, 2, 'C'],
            [None, 2, 'C'],
            [None, None, 'C'],
            [None, None, 'C'],
        ],
        StructType([
            StructField('postcode', StringType()),
            StructField('count', IntegerType()),
            StructField('code', StringType()),
        ])
    )

    df = dsp.pipeline.loading.add_traceability_columns(spark, df_raw, any_metadata())
    uniqueness_rule = UniquenessRule(['postcode', 'count'], DQErrs.DQ_NOT_UNIQUE)
    uniqueness_errors_df, uniqueness_pass_df = uniqueness_dq(df, uniqueness_rule)

    dq_errs = uniqueness_errors_df.collect()
    dq_pass = uniqueness_pass_df.collect()

    assert 2 == len(dq_errs)
    assert 10 == len(dq_pass)

    for row in dq_errs:
        dq = row[Common.DQ]

        assert 1 == len(dq)

        message = DQMessage(*dq[0])

        assert DQErrs.DQ_NOT_UNIQUE.value == message.code
        assert DQMessageType.Error == message.type
        assert 'postcode' == message.attribute
        assert ['postcode', 'count'] == message.fields
        assert DQErrs.DQ_NOT_UNIQUE.label.format('postcode') == message.message


def test_uniqueness_dq_no_rules(spark: SparkSession):
    df_raw = spark.createDataFrame(
        [
            ['LS1 2AB', 1, 'A'],
            ['LS1 2AB', 1, 'B'],
            ['LS1 2AB', 2, 'C'],
            ['LS1 3CD', 1, 'D'],
        ],
        StructType([
            StructField('postcode', StringType()),
            StructField('count', IntegerType()),
            StructField('code', StringType()),
        ])
    )

    df = dsp.pipeline.loading.add_traceability_columns(spark, df_raw, any_metadata())
    # uniqueness_errors_df, uniqueness_pass_df = uniqueness_dq(df, [])
    uniqueness_errors_df, uniqueness_pass_df = uniqueness_dq(df)
    dq_errs = uniqueness_errors_df.collect()
    dq_pass = uniqueness_pass_df.collect()

    assert 0 == len(dq_errs)
    assert 4 == len(dq_pass)
