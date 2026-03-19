from uuid import uuid4

import pytest
from pyspark.sql import SparkSession, DataFrame

from dsp.shared.pds.pds_loader import pds_loader
from dsp.shared.pds.pds_succession import get_latest_nhs_number, get_all_related_nhs_numbers, \
    get_replaced_by_pds_df
from dsp.shared import local_path


@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


@pytest.fixture()
def pds_df(spark):
    path = local_path('testdata/pds_test_data/pds_succession.csv')
    pds_df = pds_loader(spark, path, uuid4().hex)

    yield pds_df.cache()
    pds_df.unpersist()


@pytest.fixture()
def replaced_by_pds_df(pds_df: DataFrame):
    replaced_by_df = get_replaced_by_pds_df(pds_df)
    yield replaced_by_df
    replaced_by_df.unpersist()


@pytest.mark.parametrize("initial_nhs_no, latest_nhs_number", [
    ('SUCC00000A', 'SUCC00000I'),
    ('SUCC00000B', 'SUCC00000I'),
    ('SUCC00000C', 'SUCC00000I'),
    ('SUCC00000D', 'SUCC00000I'),
    ('SUCC00000E', 'SUCC00000I'),
    ('SUCC00000F', 'SUCC00000I'),
    ('SUCC00000G', 'SUCC00000I'),
    ('SUCC00000H', 'SUCC00000I'),
    ('SUCC00000I', 'SUCC00000I'),
    ('SUCC00000J', 'SUCC00000J'),
])
def test_get_latest_nhs_number(spark: SparkSession, initial_nhs_no: str, latest_nhs_number: str, pds_df: DataFrame):
    assert get_latest_nhs_number(pds_df, initial_nhs_no) == latest_nhs_number


@pytest.mark.parametrize("initial_nhs_no", [
    ('SUCC00000A'),
    ('SUCC00000B'),
    ('SUCC00000C'),
    ('SUCC00000D'),
    ('SUCC00000E'),
    ('SUCC00000F'),
    ('SUCC00000G'),
    ('SUCC00000H'),
    ('SUCC00000I'),
])
def test_succession_from_any_point_in_tree(spark: SparkSession, initial_nhs_no: str, replaced_by_pds_df: DataFrame):
    affected_nhs_nos = get_all_related_nhs_numbers(replaced_by_pds_df, initial_nhs_no)

    assert affected_nhs_nos == {
        'SUCC00000A', 'SUCC00000B', 'SUCC00000C', 'SUCC00000D',
        'SUCC00000E', 'SUCC00000F', 'SUCC00000G', 'SUCC00000H',
        'SUCC00000I'
    }


def test_succession_from_any_point_in_tree_no_related(spark: SparkSession, replaced_by_pds_df: DataFrame):
    initial_nhs_no = 'SUCC00000J'
    affected_nhs_nos = get_all_related_nhs_numbers(replaced_by_pds_df, initial_nhs_no)

    assert affected_nhs_nos == {'SUCC00000J'}
