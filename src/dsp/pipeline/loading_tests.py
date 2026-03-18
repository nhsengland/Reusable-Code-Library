import os
from datetime import datetime
from unittest import mock
from unittest.mock import Mock

from pyspark.sql import SparkSession
from uuid import uuid4

from pyspark.sql.functions import explode

import dsp.pipeline.loading
import csv
from dsp.shared.common.test_helpers import smart_open, basic_metadata
from dsp.datasets.common import Fields as CommonFields
import lxml.etree
import lxml.builder

from dsp.shared.common import format_timestamp
from dsp.shared.constants import PATHS, FT


@mock.patch('dsp.pipeline.loading.current_record_version')
@mock.patch('dsp.pipeline.loading.current_dataset_version')
def test_load_csv_simple_string_timestamp(current_dataset_version_function_mock: Mock,
                                          current_record_version_function_mock: Mock,
                                          spark: SparkSession, temp_dir: str):
    row_count = 1000
    raw_path = os.path.join(temp_dir, PATHS.RAW_DATA)

    with smart_open(raw_path, 'w') as f:
        w = csv.DictWriter(f, ['row_num'])
        w.writeheader()
        for i in range(row_count):
            w.writerow(dict(row_num=i))

    raw_df = spark.read.option("header", "true").option("inferSchema", "false").csv(raw_path)

    typed_df = raw_df.withColumn('row_num', raw_df.row_num.cast('int'))

    submission_id = uuid4().hex
    submission_dt = datetime(2018, 1, 1, 13)

    metadata = basic_metadata(temp_dir, 'fake', FT.CSV, submission_id=submission_id,
                              submitted_timestamp=format_timestamp(submission_dt))

    current_dataset_version_function_mock.return_value = "1"
    current_record_version_function_mock.return_value = 1

    traceable_df = dsp.pipeline.loading.add_traceability_columns(
        spark, typed_df, metadata
    )

    rows = traceable_df.collect()

    assert row_count == len(rows)

    for row in rows:
        metadata = row[CommonFields.META]
        assert row['row_num'] == metadata[CommonFields.RECORD_INDEX]
        assert '{}:{}'.format(submission_id, row['row_num']) == metadata[CommonFields.EVENT_ID]
        assert submission_dt == metadata[CommonFields.EVENT_RECEIVED_TS]
        assert metadata[CommonFields.DATASET_VERSION] == "1"
        assert metadata[CommonFields.RECORD_VERSION] == 1

    current_dataset_version_function_mock.assert_called_once_with('fake')
    current_record_version_function_mock.assert_called_once_with('fake')


@mock.patch('dsp.pipeline.loading.current_record_version')
@mock.patch('dsp.pipeline.loading.current_dataset_version')
def test_load_csv_simple(current_dataset_version_function_mock: Mock, current_record_version_function_mock: Mock,
                         spark: SparkSession, temp_dir: str):
    row_count = 1000
    raw_path = os.path.join(temp_dir, PATHS.RAW_DATA)

    with smart_open(raw_path, 'w') as f:
        w = csv.DictWriter(f, ['row_num'])
        w.writeheader()
        for i in range(row_count):
            w.writerow(dict(row_num=i))

    raw_df = spark.read.option("header", "true").option("inferSchema", "false").csv(raw_path)

    typed_df = raw_df.withColumn('row_num', raw_df.row_num.cast('int'))

    submission_id = uuid4().hex
    submission_dt = datetime(2018, 1, 1, 13)

    metadata = basic_metadata(temp_dir, 'fake', FT.CSV, submission_id=submission_id, submitted_timestamp=submission_dt)

    current_dataset_version_function_mock.return_value = "1"
    current_record_version_function_mock.return_value = 1

    traceable_df = dsp.pipeline.loading.add_traceability_columns(
        spark, typed_df, metadata
    )

    rows = traceable_df.collect()

    assert row_count == len(rows)

    for row in rows:
        metadata = row[CommonFields.META]
        assert row['row_num'] == metadata[CommonFields.RECORD_INDEX]
        assert '{}:{}'.format(submission_id, row['row_num']) == metadata[CommonFields.EVENT_ID]
        assert submission_dt == metadata[CommonFields.EVENT_RECEIVED_TS]
        assert metadata[CommonFields.DATASET_VERSION] == "1"
        assert metadata[CommonFields.RECORD_VERSION] == 1

    current_dataset_version_function_mock.assert_called_once_with('fake')
    current_record_version_function_mock.assert_called_once_with('fake')


@mock.patch('dsp.pipeline.loading.current_record_version')
@mock.patch('dsp.pipeline.loading.current_dataset_version')
def test_load_csv_large(current_dataset_version_function_mock: Mock, current_record_version_function_mock: Mock,
                        spark: SparkSession, temp_dir: str):
    row_count = 100000
    raw_path = os.path.join(temp_dir, PATHS.RAW_DATA)

    with smart_open(raw_path, 'w') as f:
        w = csv.DictWriter(f, ['row_num'])
        w.writeheader()
        for i in range(row_count):
            w.writerow(dict(row_num=i))

    raw_df = spark.read.option("header", "true").option("inferSchema", "false").csv(raw_path)

    typed_df = raw_df.withColumn('row_num', raw_df.row_num.cast('int'))

    submission_id = uuid4().hex
    submission_dt = datetime(2018, 1, 1, 13)

    metadata = basic_metadata(temp_dir, 'fake', FT.CSV, submission_id=submission_id, submitted_timestamp=submission_dt)

    current_dataset_version_function_mock.return_value = "1"
    current_record_version_function_mock.return_value = 1

    traceable_df = dsp.pipeline.loading.add_traceability_columns(
        spark, typed_df, metadata
    )

    rows = traceable_df.collect()

    assert row_count == len(rows)

    for row in rows:
        metadata = row[CommonFields.META]
        assert row['row_num'] == metadata[CommonFields.RECORD_INDEX]
        assert '{}:{}'.format(submission_id, row['row_num']) == metadata[CommonFields.EVENT_ID]
        assert submission_dt == metadata[CommonFields.EVENT_RECEIVED_TS]
        assert metadata[CommonFields.DATASET_VERSION] == "1"
        assert metadata[CommonFields.RECORD_VERSION] == 1

    current_dataset_version_function_mock.assert_called_once_with('fake')
    current_record_version_function_mock.assert_called_once_with('fake')


@mock.patch('dsp.pipeline.loading.current_record_version')
@mock.patch('dsp.pipeline.loading.current_dataset_version')
def test_load_csv_large_downstream_shuffling(current_dataset_version_function_mock: Mock,
                                             current_record_version_function_mock: Mock, spark: SparkSession,
                                             temp_dir: str):
    row_count = 100000
    raw_path = os.path.join(temp_dir, PATHS.RAW_DATA)

    with smart_open(raw_path, 'w') as f:
        w = csv.DictWriter(f, ['row_num'])
        w.writeheader()
        for i in range(row_count):
            w.writerow(dict(row_num=i))

    raw_df = spark.read.option("header", "true").option("inferSchema", "false").csv(raw_path)

    typed_df = raw_df.withColumn('row_num', raw_df.row_num.cast('int'))

    submission_id = uuid4().hex
    submission_dt = datetime(2018, 1, 1, 13)
    metadata = basic_metadata(temp_dir, 'fake', FT.CSV, submission_id=submission_id, submitted_timestamp=submission_dt)

    current_dataset_version_function_mock.return_value = "1"
    current_record_version_function_mock.return_value = 1

    traceable_df = dsp.pipeline.loading.add_traceability_columns(
        spark, typed_df, metadata
    )

    shuffled_df = traceable_df.repartition(spark.sparkContext.defaultParallelism * 10)

    shuffled_df = shuffled_df.repartition(spark.sparkContext.defaultParallelism)

    rows = shuffled_df.collect()

    assert row_count == len(rows)

    for row in rows:
        metadata = row[CommonFields.META]
        assert row['row_num'] == metadata[CommonFields.RECORD_INDEX]
        assert '{}:{}'.format(submission_id, row['row_num']) == metadata[CommonFields.EVENT_ID]
        assert submission_dt == metadata[CommonFields.EVENT_RECEIVED_TS]
        assert metadata[CommonFields.DATASET_VERSION] == "1"
        assert metadata[CommonFields.RECORD_VERSION] == 1

    current_dataset_version_function_mock.assert_called_once_with('fake')
    current_record_version_function_mock.assert_called_once_with('fake')


@mock.patch('dsp.pipeline.loading.current_record_version')
@mock.patch('dsp.pipeline.loading.current_dataset_version')
def test_load_xml_simple(current_dataset_version_function_mock: Mock, current_record_version_function_mock: Mock,
                         spark: SparkSession, temp_dir: str):
    row_count = 1000
    raw_path = os.path.join(temp_dir, PATHS.RAW_DATA)

    E = lxml.builder.ElementMaker()
    root = E.rows
    item = E.item
    row_num = E.row_num

    document = root()

    for i in range(row_count):
        document.append(
            item(row_num(str(i)))
        )

    with smart_open(raw_path, 'w') as f:
        xml = lxml.etree.tostring(document, encoding=str, pretty_print=True)
        f.write(xml)

    raw_df = spark.read.format("com.databricks.spark.xml").option('rowTag', 'item').load(raw_path)

    submission_id = uuid4().hex
    submission_dt = datetime(2018, 1, 1, 13)
    metadata = basic_metadata(temp_dir, 'fake', FT.XML, submission_id=submission_id, submitted_timestamp=submission_dt)

    current_dataset_version_function_mock.return_value = "1"
    current_record_version_function_mock.return_value = 1

    traceable_df = dsp.pipeline.loading.add_traceability_columns(
        spark, raw_df, metadata
    )

    rows = traceable_df.collect()

    assert row_count == len(rows)

    for row in rows:
        metadata = row[CommonFields.META]
        assert row['row_num'] == metadata[CommonFields.RECORD_INDEX]
        assert '{}:{}'.format(submission_id, row['row_num']) == metadata[CommonFields.EVENT_ID]
        assert submission_dt == metadata[CommonFields.EVENT_RECEIVED_TS]
        assert metadata[CommonFields.DATASET_VERSION] == "1"
        assert metadata[CommonFields.RECORD_VERSION] == 1

    current_dataset_version_function_mock.assert_called_once_with('fake')
    current_record_version_function_mock.assert_called_once_with('fake')


@mock.patch('dsp.pipeline.loading.current_record_version')
@mock.patch('dsp.pipeline.loading.current_dataset_version')
def test_load_xml_row_splosion_simple(current_dataset_version_function_mock: Mock,
                                      current_record_version_function_mock: Mock, spark: SparkSession, temp_dir: str):
    row_count = 10000
    raw_path = os.path.join(temp_dir, PATHS.RAW_DATA)

    E = lxml.builder.ElementMaker()
    root = E.rows
    item = E.item
    nested = E.nested
    row_num = E.row_num

    document = root()

    for i in range(row_count):
        if i % 4 != 0:
            continue

        document.append(
            item(
                nested(row_num(str(i))),
                nested(row_num(str(i + 1))),
                nested(row_num(str(i + 2))),
                nested(row_num(str(i + 3))),
            )
        )

    with smart_open(raw_path, 'w') as f:
        xml = lxml.etree.tostring(document, encoding=str, pretty_print=True)
        f.write(xml)

    raw_df = spark.read.format("com.databricks.spark.xml").option('rowTag', 'item').load(raw_path)

    exploded_df = raw_df.withColumn('nested1', explode(raw_df.nested)).selectExpr('nested1.row_num')

    submission_id = uuid4().hex
    submission_dt = datetime(2018, 1, 1, 13)
    metadata = basic_metadata(temp_dir, 'fake', FT.XML, submission_id=submission_id, submitted_timestamp=submission_dt)

    current_dataset_version_function_mock.return_value = "1"
    current_record_version_function_mock.return_value = 1

    traceable_df = dsp.pipeline.loading.add_traceability_columns(
        spark, exploded_df, metadata
    )

    rows = traceable_df.collect()

    assert row_count == len(rows)

    for row in rows:
        metadata = row[CommonFields.META]
        assert row['row_num'] == metadata[CommonFields.RECORD_INDEX]
        assert '{}:{}'.format(submission_id, row['row_num']) == metadata[CommonFields.EVENT_ID]
        assert submission_dt == metadata[CommonFields.EVENT_RECEIVED_TS]
        assert metadata[CommonFields.DATASET_VERSION] == "1"
        assert metadata[CommonFields.RECORD_VERSION] == 1

    current_dataset_version_function_mock.assert_called_once_with('fake')
    current_record_version_function_mock.assert_called_once_with('fake')
