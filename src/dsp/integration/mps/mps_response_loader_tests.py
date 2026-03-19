import os
from typing import Tuple, Any

import pytest
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession

from dsp.datasets.fields.mps.response import Fields as MpsResponseFields
from dsp.datasets.fields.mps.response_header import Fields as MpsResponseHeaderFields
from dsp.datasets.loaders import LoaderRejectionError
from dsp.integration.mps.mps_response_loader import csv_loader
from shared import local_path
from shared.common.test_helpers import smart_upload


def test_csv_loader(spark: SparkSession):
    path = local_path('testdata/mps/csv/mps_test_data/correct.csv')
    mps_df = csv_loader(spark, path)

    assert mps_df.count() == 1

    row = mps_df.collect()[0]
    assert row[MpsResponseFields.UNIQUE_REFERENCE] == '1:1'
    assert row[MpsResponseFields.REQ_NHS_NUMBER] == '4766919998'
    assert row[MpsResponseFields.FAMILY_NAME] is None
    assert row[MpsResponseFields.GIVEN_NAME] is None
    assert row[MpsResponseFields.OTHER_GIVEN_NAME] is None
    assert row[MpsResponseFields.GENDER] == '1'
    assert row[MpsResponseFields.DATE_OF_BIRTH] == 19900101
    assert row[MpsResponseFields.DATE_OF_DEATH] is None
    assert row[MpsResponseFields.ADDRESS_LINE1] == '2 Whitehall Quay'
    assert row[MpsResponseFields.ADDRESS_LINE2] is None
    assert row[MpsResponseFields.ADDRESS_LINE3] is None
    assert row[MpsResponseFields.ADDRESS_LINE4] is None
    assert row[MpsResponseFields.ADDRESS_LINE5] is None
    assert row[MpsResponseFields.POSTCODE] == 'LS1 4HR'
    assert row[MpsResponseFields.GP_PRACTICE_CODE] == 'R1A'
    assert row[MpsResponseFields.NHAIS_POSTING_ID] is None
    assert row[MpsResponseFields.AS_AT_DATE] == 20190101
    assert row[MpsResponseFields.LOCAL_PATIENT_ID] == 'LPI001'
    assert row[MpsResponseFields.INTERNAL_ID] is None
    assert row[MpsResponseFields.TELEPHONE_NUMBER] is None
    assert row[MpsResponseFields.MOBILE_NUMBER] is None
    assert row[MpsResponseFields.EMAIL_ADDRESS] is None
    assert row[MpsResponseFields.SENSITIVTY_FLAG] == 'S'
    assert row[MpsResponseFields.MPS_ID] == '8999196674'
    assert row[MpsResponseFields.ERROR_SUCCESS_CODE] == '00'
    assert row[MpsResponseFields.MATCHED_NHS_NO] == '4766919998'
    assert row[MpsResponseFields.MATCHED_ALGORITHM_INDICATOR] == 4
    assert row[MpsResponseFields.MATCHED_CONFIDENCE_PERCENTAGE] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC] == 0
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC] == 0
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_DOB_SCORE_PERC] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_GENDER_SCORE_PERC] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC] == 0


def test_csv_loader_with_header(spark: SparkSession):
    path = local_path('testdata/mps/csv/mps_test_data/correct.csv')
    mps_df, header = csv_loader(spark, path, with_header=True)

    assert header[MpsResponseHeaderFields.RESPONSE_REFERENCE] == 'MPTREQ_20190301000000'
    assert header[MpsResponseHeaderFields.WORKFLOW_ID] == 'DSP'
    assert header[MpsResponseHeaderFields.NO_OF_DATA_RECORDS] == '1'
    assert header[MpsResponseHeaderFields.FILE_RESPONSE_CODE] == '00'

    assert mps_df.count() == 1

    row = mps_df.collect()[0]
    assert row[MpsResponseFields.UNIQUE_REFERENCE] == '1:1'
    assert row[MpsResponseFields.REQ_NHS_NUMBER] == '4766919998'
    assert row[MpsResponseFields.FAMILY_NAME] is None
    assert row[MpsResponseFields.GIVEN_NAME] is None
    assert row[MpsResponseFields.OTHER_GIVEN_NAME] is None
    assert row[MpsResponseFields.GENDER] == '1'
    assert row[MpsResponseFields.DATE_OF_BIRTH] == 19900101
    assert row[MpsResponseFields.DATE_OF_DEATH] is None
    assert row[MpsResponseFields.ADDRESS_LINE1] == '2 Whitehall Quay'
    assert row[MpsResponseFields.ADDRESS_LINE2] is None
    assert row[MpsResponseFields.ADDRESS_LINE3] is None
    assert row[MpsResponseFields.ADDRESS_LINE4] is None
    assert row[MpsResponseFields.ADDRESS_LINE5] is None
    assert row[MpsResponseFields.POSTCODE] == 'LS1 4HR'
    assert row[MpsResponseFields.GP_PRACTICE_CODE] == 'R1A'
    assert row[MpsResponseFields.NHAIS_POSTING_ID] is None
    assert row[MpsResponseFields.AS_AT_DATE] == 20190101
    assert row[MpsResponseFields.LOCAL_PATIENT_ID] == 'LPI001'
    assert row[MpsResponseFields.INTERNAL_ID] is None
    assert row[MpsResponseFields.TELEPHONE_NUMBER] is None
    assert row[MpsResponseFields.MOBILE_NUMBER] is None
    assert row[MpsResponseFields.EMAIL_ADDRESS] is None
    assert row[MpsResponseFields.SENSITIVTY_FLAG] == 'S'
    assert row[MpsResponseFields.MPS_ID] == '8999196674'
    assert row[MpsResponseFields.ERROR_SUCCESS_CODE] == '00'
    assert row[MpsResponseFields.MATCHED_NHS_NO] == '4766919998'
    assert row[MpsResponseFields.MATCHED_ALGORITHM_INDICATOR] == 4
    assert row[MpsResponseFields.MATCHED_CONFIDENCE_PERCENTAGE] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC] == 0
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC] == 0
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_DOB_SCORE_PERC] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_GENDER_SCORE_PERC] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC] == 0


def test_csv_loader_gzipped(spark: SparkSession):
    path = local_path('testdata/mps/csv/mps_test_data/correct.csv.gz')
    mps_df = csv_loader(spark, path)

    assert mps_df.count() == 1

    row = mps_df.collect()[0]
    assert row[MpsResponseFields.UNIQUE_REFERENCE] == '1:1'
    assert row[MpsResponseFields.REQ_NHS_NUMBER] == '4766919998'
    assert row[MpsResponseFields.FAMILY_NAME] is None
    assert row[MpsResponseFields.GIVEN_NAME] is None
    assert row[MpsResponseFields.OTHER_GIVEN_NAME] is None
    assert row[MpsResponseFields.GENDER] == '1'
    assert row[MpsResponseFields.DATE_OF_BIRTH] == 19900101
    assert row[MpsResponseFields.DATE_OF_DEATH] is None
    assert row[MpsResponseFields.ADDRESS_LINE1] == '2 Whitehall Quay'
    assert row[MpsResponseFields.ADDRESS_LINE2] is None
    assert row[MpsResponseFields.ADDRESS_LINE3] is None
    assert row[MpsResponseFields.ADDRESS_LINE4] is None
    assert row[MpsResponseFields.ADDRESS_LINE5] is None
    assert row[MpsResponseFields.POSTCODE] == 'LS1 4HR'
    assert row[MpsResponseFields.GP_PRACTICE_CODE] == 'R1A'
    assert row[MpsResponseFields.NHAIS_POSTING_ID] is None
    assert row[MpsResponseFields.AS_AT_DATE] == 20190101
    assert row[MpsResponseFields.LOCAL_PATIENT_ID] == 'LPI001'
    assert row[MpsResponseFields.INTERNAL_ID] is None
    assert row[MpsResponseFields.TELEPHONE_NUMBER] is None
    assert row[MpsResponseFields.MOBILE_NUMBER] is None
    assert row[MpsResponseFields.EMAIL_ADDRESS] is None
    assert row[MpsResponseFields.SENSITIVTY_FLAG] == 'S'
    assert row[MpsResponseFields.MPS_ID] == '8999196674'
    assert row[MpsResponseFields.ERROR_SUCCESS_CODE] == '00'
    assert row[MpsResponseFields.MATCHED_NHS_NO] == '4766919998'
    assert row[MpsResponseFields.MATCHED_ALGORITHM_INDICATOR] == 4
    assert row[MpsResponseFields.MATCHED_CONFIDENCE_PERCENTAGE] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC] == 0
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC] == 0
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_DOB_SCORE_PERC] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_GENDER_SCORE_PERC] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC] == 0


def test_csv_loader_row_count_mismatch(spark: SparkSession):
    path = local_path('testdata/mps/csv/mps_test_data/row_count_mismatch.csv')
    with pytest.raises(LoaderRejectionError) as excinfo:
        csv_loader(spark, path)
    assert str(excinfo.value) == "The number of rows in the file (1) doesn't match the header (2)."


def test_csv_loader_carriage_return_in_content(spark: SparkSession, s3_temp: Tuple[Any, str]):
    _, s3_uri = s3_temp
    s3_uri = os.path.join(s3_uri, 'data')

    smart_upload(local_path("testdata/mps/csv/mps_test_data/carriage_returns_correct.csv"), s3_uri)

    mps_df = csv_loader(spark, s3_uri)

    assert mps_df.count() == 9
    rows = mps_df.sort(col(MpsResponseFields.UNIQUE_REFERENCE).asc()).collect()

    for i, row in enumerate(rows):
        assert row[MpsResponseFields.ADDRESS_LINE1] == f'{i + 1} Whitehall Quay', row[MpsResponseFields.ADDRESS_LINE1]
        assert row[MpsResponseFields.GP_PRACTICE_CODE] == 'R1A'


def test_csv_loader_carriage_return_in_content_row_count_mismatch(spark: SparkSession, s3_temp: Tuple[Any, str]):
    """Test it still fails even after attempting to fix"""
    _, s3_uri = s3_temp
    s3_uri = os.path.join(s3_uri, 'data')

    smart_upload(local_path("testdata/mps/csv/mps_test_data/carriage_returns_row_count_mismatch.csv"), s3_uri)

    with pytest.raises(LoaderRejectionError) as excinfo:
        csv_loader(spark, s3_uri)
    assert str(excinfo.value) == "The number of rows in the file (9) doesn't match the header (8)."

def test_csv_loader_delimiter_in_content(spark: SparkSession, s3_temp: Tuple[Any, str]):

    _, s3_uri = s3_temp
    s3_uri = os.path.join(s3_uri, 'data')

    smart_upload(local_path("testdata/mps/csv/mps_test_data/content_contains_quoted_delimiter_returns_correct.csv"), s3_uri)

    mps_df = csv_loader(spark, s3_uri)

    assert mps_df.count() == 1

    row = mps_df.collect()[0]

    assert row[MpsResponseFields.UNIQUE_REFERENCE] == '1:1'
    assert row[MpsResponseFields.REQ_NHS_NUMBER] == '4766919998'
    assert row[MpsResponseFields.FAMILY_NAME] is None
    assert row[MpsResponseFields.GIVEN_NAME] is None
    assert row[MpsResponseFields.OTHER_GIVEN_NAME] is None
    assert row[MpsResponseFields.GENDER] == '1'
    assert row[MpsResponseFields.DATE_OF_BIRTH] == 19900101
    assert row[MpsResponseFields.DATE_OF_DEATH] is None
    assert row[MpsResponseFields.ADDRESS_LINE1] == '2, Whitehall Quay'
    assert row[MpsResponseFields.ADDRESS_LINE2] is None
    assert row[MpsResponseFields.ADDRESS_LINE3] is None
    assert row[MpsResponseFields.ADDRESS_LINE4] is None
    assert row[MpsResponseFields.ADDRESS_LINE5] is None
    assert row[MpsResponseFields.POSTCODE] == 'LS1 4HR'
    assert row[MpsResponseFields.GP_PRACTICE_CODE] == 'R1A'
    assert row[MpsResponseFields.NHAIS_POSTING_ID] is None
    assert row[MpsResponseFields.AS_AT_DATE] == 20190101
    assert row[MpsResponseFields.LOCAL_PATIENT_ID] == 'LPI001'
    assert row[MpsResponseFields.INTERNAL_ID] is None
    assert row[MpsResponseFields.TELEPHONE_NUMBER] is None
    assert row[MpsResponseFields.MOBILE_NUMBER] is None
    assert row[MpsResponseFields.EMAIL_ADDRESS] is None
    assert row[MpsResponseFields.SENSITIVTY_FLAG] == 'S'
    assert row[MpsResponseFields.MPS_ID] == '8999196674'
    assert row[MpsResponseFields.ERROR_SUCCESS_CODE] == '00'
    assert row[MpsResponseFields.MATCHED_NHS_NO] == '4766919998'
    assert row[MpsResponseFields.MATCHED_ALGORITHM_INDICATOR] == 4
    assert row[MpsResponseFields.MATCHED_CONFIDENCE_PERCENTAGE] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC] == 0
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC] == 0
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_DOB_SCORE_PERC] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_GENDER_SCORE_PERC] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC] == 0

def test_csv_loader_escaped_quote_in_content(spark: SparkSession, s3_temp: Tuple[Any, str]):
    _, s3_uri = s3_temp
    s3_uri = os.path.join(s3_uri, 'data')

    smart_upload(local_path("testdata/mps/csv/mps_test_data/escaped_quote_in_content_returns_correct.csv"), s3_uri)

    mps_df = csv_loader(spark, s3_uri)

    assert mps_df.count() == 1

    row = mps_df.collect()[0]

    assert row[MpsResponseFields.UNIQUE_REFERENCE] == '1:1'
    assert row[MpsResponseFields.REQ_NHS_NUMBER] == '4766919998'
    assert row[MpsResponseFields.FAMILY_NAME] is None
    assert row[MpsResponseFields.GIVEN_NAME] is None
    assert row[MpsResponseFields.OTHER_GIVEN_NAME] is None
    assert row[MpsResponseFields.GENDER] == '1'
    assert row[MpsResponseFields.DATE_OF_BIRTH] == 19900101
    assert row[MpsResponseFields.DATE_OF_DEATH] is None
    assert row[MpsResponseFields.ADDRESS_LINE1] == '2 "Whitehall" Quay'
    assert row[MpsResponseFields.ADDRESS_LINE2] is None
    assert row[MpsResponseFields.ADDRESS_LINE3] is None
    assert row[MpsResponseFields.ADDRESS_LINE4] is None
    assert row[MpsResponseFields.ADDRESS_LINE5] is None
    assert row[MpsResponseFields.POSTCODE] == 'LS1 4HR'
    assert row[MpsResponseFields.GP_PRACTICE_CODE] == 'R1A'
    assert row[MpsResponseFields.NHAIS_POSTING_ID] is None
    assert row[MpsResponseFields.AS_AT_DATE] == 20190101
    assert row[MpsResponseFields.LOCAL_PATIENT_ID] == 'LPI001'
    assert row[MpsResponseFields.INTERNAL_ID] is None
    assert row[MpsResponseFields.TELEPHONE_NUMBER] is None
    assert row[MpsResponseFields.MOBILE_NUMBER] is None
    assert row[MpsResponseFields.EMAIL_ADDRESS] is None
    assert row[MpsResponseFields.SENSITIVTY_FLAG] == 'S'
    assert row[MpsResponseFields.MPS_ID] == '7999196674'
    assert row[MpsResponseFields.ERROR_SUCCESS_CODE] == '00'
    assert row[MpsResponseFields.MATCHED_NHS_NO] == '4766919998'
    assert row[MpsResponseFields.MATCHED_ALGORITHM_INDICATOR] == 4
    assert row[MpsResponseFields.MATCHED_CONFIDENCE_PERCENTAGE] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC] == 0
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC] == 0
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_DOB_SCORE_PERC] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_GENDER_SCORE_PERC] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC] == 0

def test_csv_loader_escaped_quote_next_to_delimiter_in_content(spark: SparkSession, s3_temp: Tuple[Any, str]):
    _, s3_uri = s3_temp
    s3_uri = os.path.join(s3_uri, 'data')

    smart_upload(local_path("testdata/mps/csv/mps_test_data/escaped_quote_next_to_delimiter_in_content_returns_correct.csv"), s3_uri)

    mps_df = csv_loader(spark, s3_uri)

    assert mps_df.count() == 1

    row = mps_df.collect()[0]

    assert row[MpsResponseFields.UNIQUE_REFERENCE] == '1:1'
    assert row[MpsResponseFields.REQ_NHS_NUMBER] == '4766919998'
    assert row[MpsResponseFields.FAMILY_NAME] is None
    assert row[MpsResponseFields.GIVEN_NAME] is None
    assert row[MpsResponseFields.OTHER_GIVEN_NAME] is None
    assert row[MpsResponseFields.GENDER] == '1'
    assert row[MpsResponseFields.DATE_OF_BIRTH] == 19900101
    assert row[MpsResponseFields.DATE_OF_DEATH] is None
    assert row[MpsResponseFields.ADDRESS_LINE1] == '2 "Whitehall", Quay'
    assert row[MpsResponseFields.ADDRESS_LINE2] is None
    assert row[MpsResponseFields.ADDRESS_LINE3] is None
    assert row[MpsResponseFields.ADDRESS_LINE4] is None
    assert row[MpsResponseFields.ADDRESS_LINE5] is None
    assert row[MpsResponseFields.POSTCODE] == 'LS1 4HR'
    assert row[MpsResponseFields.GP_PRACTICE_CODE] == 'R1A'
    assert row[MpsResponseFields.NHAIS_POSTING_ID] is None
    assert row[MpsResponseFields.AS_AT_DATE] == 20190101
    assert row[MpsResponseFields.LOCAL_PATIENT_ID] == 'LPI001'
    assert row[MpsResponseFields.INTERNAL_ID] is None
    assert row[MpsResponseFields.TELEPHONE_NUMBER] is None
    assert row[MpsResponseFields.MOBILE_NUMBER] is None
    assert row[MpsResponseFields.EMAIL_ADDRESS] is None
    assert row[MpsResponseFields.SENSITIVTY_FLAG] == 'S'
    assert row[MpsResponseFields.MPS_ID] == '8999196674'
    assert row[MpsResponseFields.ERROR_SUCCESS_CODE] == '00'
    assert row[MpsResponseFields.MATCHED_NHS_NO] == '4766919998'
    assert row[MpsResponseFields.MATCHED_ALGORITHM_INDICATOR] == 4
    assert row[MpsResponseFields.MATCHED_CONFIDENCE_PERCENTAGE] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC] == 0
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC] == 0
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_DOB_SCORE_PERC] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_GENDER_SCORE_PERC] == 100
    assert row[MpsResponseFields.ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC] == 0
