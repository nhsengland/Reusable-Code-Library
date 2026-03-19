from datetime import date

import pytest
from pyspark import Row
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.types import IntegerType, StructField, StructType, DateType, StringType

from dsp.derive.some_derivation import derive
from dsp.udfs import NHSNumberStatusDescription, AgeAtEvent, AgeBand5Years, AgeBand10Years, \
    AgeBandChildrenInc0, AgeBandInc50to74, AgeBandIncWorkingAge16to60, AgeBandIncWorkingAge16to64


@pytest.fixture()
def derivations_tests_df1(spark: SparkSession) -> DataFrame:
    low_date = date(2000, 1, 1)
    high_date = date(2017, 1, 1)

    df = spark.createDataFrame(
        [
            Row(i, low_date if i % 2 == 1 else high_date, high_date if i % 2 == 1 else low_date, '07')
            for i in range(10)
        ], StructType([
            StructField('id', IntegerType()),
            StructField('date_of_birth', DateType(), False),
            StructField('event_date', DateType(), False),
            StructField('nhs_number_status', StringType(), False),
        ])

    )
    return df


def test_derive_age_at_event(derivations_tests_df1: DataFrame) -> None:
    derivations = [
        ('AGE_AT_EVENT', lambda: AgeAtEvent('date_of_birth', 'event_date'))
    ]

    derived_df = derive(derivations_tests_df1, derivations)

    total_years = derived_df.agg(sum(derived_df.AGE_AT_EVENT).alias('total_years')).collect()[0].total_years

    results = derived_df.collect()
    assert 0 == total_years

    for row in results:

        if row.date_of_birth > row.event_date:
            assert -17 == row.AGE_AT_EVENT
        else:
            assert 17 == row.AGE_AT_EVENT


def test_derive_status_desc(derivations_tests_df1: DataFrame) -> None:
    derivations = [
        ('status_desc', lambda: NHSNumberStatusDescription('nhs_number_status'))
    ]

    derived_df = derive(derivations_tests_df1, derivations)

    results = derived_df.collect()

    for row in results:
        assert 'Number not present and trace not required' == row.STATUS_DESC


def test_deriv_age_band_5(derivations_tests_df1: DataFrame, spark: SparkSession) -> None:
    derivations = [
        ('AGE_AT_EVENT', lambda: AgeAtEvent('date_of_birth', 'event_date')),
        ('age_band_5_years', lambda: AgeBand5Years('AGE_AT_EVENT'))
    ]

    derived_df = derive(derivations_tests_df1, derivations)
    derived_df.createOrReplaceTempView('bob')
    results = derived_df.collect()

    for row in results:
        if row.AGE_AT_EVENT > 0:
            assert '15-19' == row.AGE_BAND_5_YEARS
        else:
            assert 'Age not known' == row.AGE_BAND_5_YEARS


def test_derivations_age_band_10(derivations_tests_df1: DataFrame) -> None:
    derivations = [
        ('AGE_AT_EVENT', lambda: AgeAtEvent('date_of_birth', 'event_date')),
        ('age_band_10_years', lambda: AgeBand10Years('AGE_AT_EVENT'))
    ]

    derived_df = derive(derivations_tests_df1, derivations)

    results = derived_df.collect()

    for row in results:
        if row.AGE_AT_EVENT > 0:
            assert '10-19' == row.AGE_BAND_10_YEARS
        else:
            assert 'Age not known' == row.AGE_BAND_10_YEARS


def test_derivations_children_inc_0(derivations_tests_df1: DataFrame) -> None:
    derivations = [
        ('AGE_AT_EVENT', lambda: AgeAtEvent('date_of_birth', 'event_date')),
        ('age_band_children_inc_0', lambda: AgeBandChildrenInc0('AGE_AT_EVENT'))
    ]

    derived_df = derive(derivations_tests_df1, derivations)

    results = derived_df.collect()

    for row in results:
        if row.AGE_AT_EVENT > 0:
            assert '16 and over' == row.AGE_BAND_CHILDREN_INC_0
        else:
            assert 'Age not known' == row.AGE_BAND_CHILDREN_INC_0


def test_derivations_age_band_inc_50_to_74(derivations_tests_df1: DataFrame) -> None:
    derivations = [
        ('AGE_AT_EVENT', lambda: AgeAtEvent('date_of_birth', 'event_date')),
        ('age_band_inc_50_to_74', lambda: AgeBandInc50to74('AGE_AT_EVENT'))
    ]

    derived_df = derive(derivations_tests_df1, derivations)

    results = derived_df.collect()

    for row in results:
        if row.AGE_AT_EVENT > 0:
            assert '0-18' == row.AGE_BAND_INC_50_TO_74
        else:
            assert 'Age not known' == row.AGE_BAND_INC_50_TO_74


def test_derivations_age_band_inc_wa_16_to_60(derivations_tests_df1: DataFrame) -> None:
    derivations = [
        ('AGE_AT_EVENT', lambda: AgeAtEvent('date_of_birth', 'event_date')),
        ('age_band_inc_wa_16_to_60', lambda: AgeBandIncWorkingAge16to60('AGE_AT_EVENT'))
    ]

    derived_df = derive(derivations_tests_df1, derivations)

    results = derived_df.collect()

    for row in results:
        if row.AGE_AT_EVENT > 0:
            assert '16-60' == row.AGE_BAND_INC_WA_16_TO_60
        else:
            assert 'Age not known' == row.AGE_BAND_INC_WA_16_TO_60


def test_derivations_age_band_inc_wa_16_to_64(derivations_tests_df1: DataFrame) -> None:
    derivations = [
        ('AGE_AT_EVENT', lambda: AgeAtEvent('date_of_birth', 'event_date')),
        ('age_band_inc_wa_16_to_64', lambda: AgeBandIncWorkingAge16to64('AGE_AT_EVENT'))
    ]

    derived_df = derive(derivations_tests_df1, derivations)

    results = derived_df.collect()

    for row in results:
        if row.AGE_AT_EVENT > 0:
            assert '16-64' == row.AGE_BAND_INC_WA_16_TO_64
        else:
            assert 'Age not known' == row.AGE_BAND_INC_WA_16_TO_64
