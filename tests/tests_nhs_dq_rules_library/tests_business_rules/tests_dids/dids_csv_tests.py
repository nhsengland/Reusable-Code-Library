from dsp.shared import local_path
from dsp.datasets.dids.submitted import Order
from dsp.loaders import LoaderRejectionError
from dsp.loaders.dids.dids_csv import csv_loader
from pyspark.sql import SparkSession, DataFrame
import pytest


@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


def test_csv_loader(spark):
    path = local_path('testdata/dids_test_data/dids_without_header.csv')
    dids_df, _ = csv_loader(spark, path, {})
    assert dids_df.schema.names == Order


def test_csv_loader_mismatch(spark):
    path = local_path('testdata/dids_test_data/dids_more_columns.csv')
    with pytest.raises(LoaderRejectionError):
        csv_loader(spark, path, {})
