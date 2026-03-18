
from dsp.datasets.epma.deid.deid_fields import DeidFieldsEPMAWS2
from pyspark.sql import Row
from pyspark.sql import SparkSession

from dsp.datasets.models.epmawspc2 import EPMAWellSkyPrescriptionModel2
from dsp.datasets.models.epmawsad2 import EPMAWellSkyAdministrationModel2
# noinspection PyUnresolvedReferences
from dsp.datasets.models.epma_tests.epma_helper_tests2 import epmawspc2_test_data, epmawsad2_test_data
from dsp.integration.de_identify import PERSON_ID, NHS_NUMBER


def test_de_identify_epmawspc2(spark: SparkSession):
    nhsno1 = '9705526591'

    epmawspc2_data = epmawspc2_test_data()
    epmawspc2_data['NHSorCHINumber'] = nhsno1
    epmawspc2_test = EPMAWellSkyPrescriptionModel2(epmawspc2_data)

    df = spark.createDataFrame([
        epmawspc2_test.as_row(),
    ], EPMAWellSkyPrescriptionModel2.get_struct())

    df_deid = DeidFieldsEPMAWS2().de_id_fields(spark, df)

    expected = [
        Row(clear=nhsno1, pseudo_type=NHS_NUMBER),
    ]

    assert df_deid.count() == len(expected)
    assert df_deid.select('correlation_id').distinct().count() == len(expected)
    assert sorted(df_deid.select('clear', 'pseudo_type').collect()) == sorted(expected)


def test_de_identify_epmawsad2(spark: SparkSession):
    nhsno1 = '9705526597'

    epmawsad2_data = epmawsad2_test_data()
    epmawsad2_data['NHSorCHINumber'] = nhsno1
    epmawsad2_test = EPMAWellSkyAdministrationModel2(epmawsad2_data)

    df = spark.createDataFrame([
        epmawsad2_test.as_row(),
    ], EPMAWellSkyAdministrationModel2.get_struct())

    df_deid = DeidFieldsEPMAWS2().de_id_fields(spark, df)

    expected = [
        Row(clear=nhsno1, pseudo_type=NHS_NUMBER),
    ]

    assert df_deid.count() == len(expected)
    assert df_deid.select('correlation_id').distinct().count() == len(expected)
    assert sorted(df_deid.select('clear', 'pseudo_type').collect()) == sorted(expected)


def test_de_identify_duplicates(spark: SparkSession):
    epmawspc2_data_row1 = epmawspc2_test_data()
    epmawspc2_data_row2 = epmawspc2_test_data()

    dupnhsno = '9705526591'

    epmawspc2_data_row1['NHSorCHINumber'] = dupnhsno
    epmawspc2_test_row1 = EPMAWellSkyPrescriptionModel2(epmawspc2_data_row1).as_row()

    epmawspc2_data_row2['NHSorCHINumber'] = dupnhsno
    epmawspc2_test_row2 = EPMAWellSkyPrescriptionModel2(epmawspc2_data_row2).as_row()

    df = spark.createDataFrame([
        epmawspc2_test_row1, epmawspc2_test_row2
    ], EPMAWellSkyPrescriptionModel2.get_struct())

    df_deid = DeidFieldsEPMAWS2().de_id_fields(spark, df)

    expected = [
        Row(clear=dupnhsno, pseudo_type=NHS_NUMBER),
    ]

    assert df_deid.count() == len(expected)
    assert df_deid.select('correlation_id').distinct().count() == len(expected)
    assert sorted(df_deid.select('clear', 'pseudo_type').collect()) == sorted(expected)


def test_de_identify_epmawsad2_empty_strings(spark: SparkSession):
    # all fields populated

    epmawsad2_data = epmawsad2_test_data()

    epmawsad2_data['NHSorCHINumber'] = ''
    epmawsad2_test = EPMAWellSkyAdministrationModel2(epmawsad2_data)

    df = spark.createDataFrame([
        epmawsad2_test.as_row(),
    ], EPMAWellSkyAdministrationModel2.get_struct())

    df_deid = DeidFieldsEPMAWS2().de_id_fields(spark, df)

    # Person_ID is not set here
    expected = list()

    assert df_deid.count() == len(expected)
    assert df_deid.select('correlation_id').distinct().count() == len(expected)
    assert sorted(df_deid.select('clear', 'pseudo_type').collect()) == sorted(expected)
