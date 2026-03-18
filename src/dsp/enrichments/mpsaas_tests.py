from collections import OrderedDict

from pyspark.sql import SparkSession, DataFrame, Row

from dsp.enrichments.mpsaas import enrich_mpsaas_with_person_id, mpsaas_response_schema
from dsp.datasets.mps.response import Fields as MPSResponseFields
from dsp.datasets.mps.response import Order as MPSResponseOrder
from dsp.datasets.mpsaas.output import Fields as MPSaaSResponseFields
from dsp.datasets.mpsaas.output import Order as MPSaaSResponseOrder
from dsp.integration.mps.mps_schema import mps_response_schema
from dsp.validations.validator import compare_results
from dsp.shared.common import enumlike_values


def create_input_dict() -> dict:
    row_data = [None for field in MPSResponseOrder]
    d = OrderedDict()
    d.update(zip(enumlike_values(MPSResponseFields), row_data))
    return d


def create_result_dict() -> dict:
    row_data = [None for field in MPSaaSResponseOrder]
    d = OrderedDict()
    d.update(zip(enumlike_values(MPSaaSResponseFields), row_data))
    return d


def create_df(spark: SparkSession, d: dict, schema) -> DataFrame:
    return spark.sparkContext.parallelize([d]).map(convert_to_row).toDF(schema)


def convert_to_row(d: dict) -> Row:
    return Row(**OrderedDict(sorted(d.items())))


def test_enrich_mpsaas_with_person_id_when_matched_nhs_number_person_id_populated(spark: SparkSession):
    source_dict = create_input_dict()
    source_dict[MPSResponseFields.UNIQUE_REFERENCE] = 'A'
    source_dict[MPSResponseFields.MATCHED_NHS_NO] = '12345'
    source_dict[MPSResponseFields.ERROR_SUCCESS_CODE] = '00'
    source_dict[MPSResponseFields.MATCHED_ALGORITHM_INDICATOR] = 1

    expected_dict = create_result_dict()

    expected_dict[MPSaaSResponseFields.MATCHED_ALGORITHM_INDICATOR] = 1
    expected_dict[MPSaaSResponseFields.ERROR_SUCCESS_CODE] = '00'
    expected_dict[MPSaaSResponseFields.UNIQUE_REFERENCE] = 'A'
    expected_dict[MPSaaSResponseFields.MATCHED_NHS_NO] = '12345'
    expected_dict[MPSaaSResponseFields.PERSON_ID] = '12345'
    expected_df = create_df(spark, expected_dict, mpsaas_response_schema)

    df = create_df(spark, source_dict, mps_response_schema)

    df_traced = enrich_mpsaas_with_person_id(1, df)

    assert compare_results(df_traced, expected_df, join_columns=[MPSaaSResponseFields.UNIQUE_REFERENCE])


def test_enrich_mpsaas_with_person_id_when_mps_id_person_id_populated(spark: SparkSession):
    source_dict = create_input_dict()
    source_dict[MPSResponseFields.UNIQUE_REFERENCE] = 'A'
    source_dict[MPSResponseFields.MATCHED_NHS_NO] = '0000000000'
    source_dict[MPSResponseFields.MPS_ID] = 'ABC'
    source_dict[MPSResponseFields.ERROR_SUCCESS_CODE] = '00'
    source_dict[MPSResponseFields.MATCHED_ALGORITHM_INDICATOR] = 4

    expected_dict = create_result_dict()
    expected_dict[MPSaaSResponseFields.MATCHED_ALGORITHM_INDICATOR] = 4
    expected_dict[MPSaaSResponseFields.ERROR_SUCCESS_CODE] = '00'
    expected_dict[MPSaaSResponseFields.UNIQUE_REFERENCE] = 'A'
    expected_dict[MPSaaSResponseFields.MATCHED_NHS_NO] = '0000000000'
    expected_dict[MPSaaSResponseFields.MPS_ID] = 'ABC'
    expected_dict[MPSaaSResponseFields.PERSON_ID] = 'ABC'
    expected_df = create_df(spark, expected_dict, mpsaas_response_schema)

    df = create_df(spark, source_dict, mps_response_schema)

    df_traced = enrich_mpsaas_with_person_id(1, df)

    assert compare_results(df_traced, expected_df, join_columns=[MPSaaSResponseFields.UNIQUE_REFERENCE])


def test_enrich_mpsaas_with_person_id_when_unmatched_person_id_populated(spark: SparkSession):
    source_dict = create_input_dict()
    source_dict[MPSResponseFields.UNIQUE_REFERENCE] = 'A'
    source_dict[MPSResponseFields.MATCHED_NHS_NO] = '0000000000'
    source_dict[MPSResponseFields.ERROR_SUCCESS_CODE] = '97'
    source_dict[MPSResponseFields.MATCHED_ALGORITHM_INDICATOR] = 0

    expected_dict = create_result_dict()
    expected_dict[MPSaaSResponseFields.MATCHED_ALGORITHM_INDICATOR] = 0
    expected_dict[MPSaaSResponseFields.UNIQUE_REFERENCE] = 'A'
    expected_dict[MPSaaSResponseFields.MATCHED_NHS_NO] = '0000000000'
    expected_dict[MPSResponseFields.ERROR_SUCCESS_CODE] = '97'
    expected_dict[MPSaaSResponseFields.PERSON_ID] = 'U00005YC1S'
    expected_df = create_df(spark, expected_dict, mpsaas_response_schema)

    df = create_df(spark, source_dict, mps_response_schema)

    df_traced = enrich_mpsaas_with_person_id(1, df)

    assert compare_results(df_traced, expected_df, join_columns=[MPSaaSResponseFields.UNIQUE_REFERENCE])
