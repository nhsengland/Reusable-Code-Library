from datetime import datetime, date, time
from decimal import Decimal
from typing import Union, Tuple, List, Callable

import pytest
from pyspark import Row
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    DataType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    IntegerType,
)

from dsp.common.relational import Table
from dsp.datasets.definitions.dq.schema import DQ_MESSAGE_SCHEMA
from dsp.datasets.definitions.mhsds_v5.submission_constants import *
from dsp.datasets.definitions.msds.submission_constants import MSD000
from dsp.datasets.fields.dq.output import Fields as DQFields
from dsp.datasets.models.msds import Header
# noinspection PyUnresolvedReferences
from dsp.datasets.models.msds_tests.msds_helper_tests import header  # pylint: disable=unused-import
from dsp.datasets.pipelines.common import ValidateMandatoryTablesNotEmpty, CleanseStage, NotebookPayload
from dsp.datasets.validations.mhsds_v5.validation_messages import get_validation_detail
from dsp.pipeline.test_helpers import run_single_stage
from shared.models import PipelineStatus
from shared.constants import ClusterPools


def populate_table(spark: SparkSession, table: Table, number_of_rows: int = 1,
                   rows: List[Tuple[Union[str, datetime], ...]] = None,
                   df_transformer: Callable[[DataFrame], DataFrame] = lambda x: x) -> DataFrame:
    default_row = []
    fields = []
    for field in table.qualified_fields:
        datatype, value = type_to_datatype(field.data_type)
        default_row.append(value)
        fields.append(StructField(field.name, dataType=datatype))
    schema = StructType(fields)
    df = spark.createDataFrame(
        rows if rows is not None else [tuple(default_row)] * number_of_rows,
        schema
    )
    transformed_df = df_transformer(df)
    transformed_df.createOrReplaceTempView(table.name)
    return transformed_df


def type_to_datatype(type_: type) -> Tuple[DataType, Union[str, datetime, date, int, Decimal]]:
    if type_ is str:
        return StringType(), "23:34:21"  # str is used for timestamps in MSHS
    elif type_ is datetime:
        return TimestampType(), datetime(2015, 4, 12, 23, 34, 21)
    elif type_ is date:
        return DateType(), date(2015, 4, 12)
    elif type_ is Decimal:
        return DecimalType(), Decimal(30.4)
    elif type_ is int:
        return IntegerType(), 42
    elif type_ is time:
        return TimestampType(), datetime(1970, 1, 1, 23, 34, 21)
    raise NotImplementedError(str(type_))


def test_check_for_empty_tables_no_empty_tables(spark: SparkSession, dq_df: DataFrame):
    populate_table(spark, MHS000)
    populate_table(spark, MHS001)
    populate_table(spark, MHS002)
    populate_table(spark, MHS101)

    stage = ValidateMandatoryTablesNotEmpty({
        MHS000: 'MHSREJ002',
        MHS001: 'MHSREJ003',
        MHS002: 'MHSREJ005',
        MHS101: 'MHSREJ004',
    }, ALL_TABLES.values(), get_validation_detail)

    dq_df = stage._check_for_empty_tables(spark, dq_df)

    expected = []

    assert dq_df.select(DQFields.TABLE, DQFields.CODE).collect() == expected


def test_check_for_empty_tables_with_all_tables_empty(spark: SparkSession, dq_df: DataFrame):
    populate_table(spark, MHS000, rows=[])
    populate_table(spark, MHS001, rows=[])
    populate_table(spark, MHS002, rows=[])
    populate_table(spark, MHS101, rows=[])

    stage = ValidateMandatoryTablesNotEmpty({
        MHS000: 'MHSREJ002',
        MHS001: 'MHSREJ003',
        MHS002: 'MHSREJ005',
        MHS101: 'MHSREJ004',
    }, ALL_TABLES.values(), get_validation_detail)

    dq_df = stage._check_for_empty_tables(spark, dq_df)

    row = Row(DQFields.TABLE, DQFields.CODE)
    expected = [
        row('MHS000Header', 'MHSREJ002'),
        row('MHS001MPI', 'MHSREJ003'),
        row('MHS002GP', 'MHSREJ005'),
        row('MHS101Referral', 'MHSREJ004'),
    ]

    assert dq_df.select(DQFields.TABLE, DQFields.CODE).collect() == expected


def test_check_for_empty_tables_with_some_empty_tables(spark: SparkSession, dq_df: DataFrame):
    populate_table(spark, MHS000)
    populate_table(spark, MHS001, rows=[])
    populate_table(spark, MHS002, rows=[])
    populate_table(spark, MHS101)

    stage = ValidateMandatoryTablesNotEmpty({
        MHS000: 'MHSREJ002',
        MHS001: 'MHSREJ003',
        MHS002: 'MHSREJ005',
        MHS101: 'MHSREJ004',
    }, ALL_TABLES.values(), get_validation_detail)

    dq_df = stage._check_for_empty_tables(spark, dq_df)

    row = Row(DQFields.TABLE, DQFields.CODE)
    expected = [
        row('MHS001MPI', 'MHSREJ003'),
        row('MHS002GP', 'MHSREJ005'),
    ]

    assert dq_df.select(DQFields.TABLE, DQFields.CODE).collect() == expected


@pytest.fixture(scope='session')
def dq_df(spark: SparkSession):
    dq_df = spark.createDataFrame([], StructType([
        *DQ_MESSAGE_SCHEMA,
        StructField(DQFields.DQ_TS, TimestampType())
    ]))
    return dq_df


def test_cleanse_stage(
        spark: SparkSession,
        header: dict,
        temp_dir: str,
):
    df_header = spark.createDataFrame([Header(header).as_row(force_derivation=False)],
                                      Header.get_struct(include_derived_attr=False))

    df_header = df_header.withColumn('Version', lit(' 2.0 '))
    df_header = df_header.withColumn('OrgIDProvider', lit('      '))

    summary, context = run_single_stage(spark, CleanseStage, {'working_folder': temp_dir, 'submission_id': 1},
                                        {'MSD000Header': df_header}, tables=[MSD000])

    df_new = context.dataframes

    assert summary.status == PipelineStatus.Success
    header_rec = df_new['MSD000Header'].df.select('Version', 'OrgIDProvider').collect()[0]
    assert header_rec.Version == '2.0'
    assert header_rec.OrgIDProvider is None


def test_notebook_payload_creation():
    table_name = "test_table"
    notebook_path = 'integration-tests/test_notebook_name'
    expected_file = "expected_filename"
    submission_ids = ['test_sub_id1', 'test_sub_id2']
    data_filter = f'META.SUBMISSION_ID IN ({",".join(str(submission_id) for submission_id in submission_ids)})'
    expected_payload = {
        'cluster_pool': ClusterPools.SYSTEM,
        'run_name': 'the contents of the {} table are as expected'.format(table_name),
        'notebook_task': {
            'notebook_path': notebook_path,
            'base_parameters': {
                'expected_file': expected_file,
                'table_name': table_name,
                'data_filter': data_filter
            }
        }
    }
    payload = NotebookPayload(
        notebook_path, 'the contents of the {} table are as expected'.format(table_name), ClusterPools.SYSTEM,
        table_name=table_name, expected_file=expected_file, data_filter=data_filter).__dict__
    assert payload == expected_payload
