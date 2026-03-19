import os
from shutil import copy
from tempfile import TemporaryDirectory

import pytest
from mock import Mock
from pyspark.sql import SparkSession, DataFrame
from pytest import raises

from dsp.integration.mps.mps_schema import mps_response_schema
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.stages.collect_mps_response import CollectMPSResponseStage
from dsp.shared import local_path
from dsp.shared.models import MpsManifestStatus, MpsManifest
from dsp.shared.store.base import ModelNotFound
from dsp.shared.store.mps_manifests import MpsManifestsStore


def test_precondition_met_on_no_unmatched_records():
    spark_mock = Mock(SparkSession)

    unmatched_df_mock = Mock(DataFrame)
    unmatched_df_mock.count.return_value = 0

    context_mock = Mock(PipelineContext)
    context_mock.dataframes = {
        'df_unmatched': DataFrameInfo(unmatched_df_mock)
    }

    assert CollectMPSResponseStage()._preconditions_met(spark_mock, context_mock)


def test_precondition_not_met_when_no_mps_manifest_for_submission_id():
    spark_mock = Mock(SparkSession)

    unmatched_df_mock = Mock(DataFrame)
    unmatched_df_mock.count.return_value = 1

    submission_id = 123
    context_mock = Mock(PipelineContext)
    context_mock.primitives = {
        'submission_id': submission_id
    }
    context_mock.dataframes = {
        'df_unmatched': DataFrameInfo(unmatched_df_mock)
    }

    mps_manifest_store_mock = Mock(MpsManifestsStore)
    mps_manifest_store_mock.get_by_submission_id.side_effect = ModelNotFound

    assert not CollectMPSResponseStage(mps_manifests_store=mps_manifest_store_mock)._preconditions_met(spark_mock,
                                                                                                       context_mock)
    mps_manifest_store_mock.get_by_submission_id.assert_called_once_with(str(submission_id))


@pytest.mark.parametrize(['mps_manifest_status', 'expected'], [
    (MpsManifestStatus.Pending, False),
    (MpsManifestStatus.Success, True)
])
def test_precondition_expected_when_mps_manifest_for_submission_id_has_status(mps_manifest_status: str, expected: bool):
    spark_mock = Mock(SparkSession)

    unmatched_df_mock = Mock(DataFrame)
    unmatched_df_mock.count.return_value = 1

    submission_id = 123
    context_mock = Mock(PipelineContext)
    context_mock.primitives = {
        'submission_id': submission_id
    }
    context_mock.dataframes = {
        'df_unmatched': DataFrameInfo(unmatched_df_mock)
    }

    mps_manifest_mock = Mock(MpsManifest)
    mps_manifest_mock.status = mps_manifest_status

    mps_manifest_store_mock = Mock(MpsManifestsStore)
    mps_manifest_store_mock.get_by_submission_id.return_value = mps_manifest_mock

    assert CollectMPSResponseStage(mps_manifests_store=mps_manifest_store_mock) \
               ._preconditions_met(spark_mock, context_mock) == expected
    mps_manifest_store_mock.get_by_submission_id.assert_called_once_with(str(submission_id))


def test_rejects_response_with_invalid_header(spark: SparkSession):
    with TemporaryDirectory() as tempdir:
        response_folder = os.path.join(tempdir, 'mps', 'response')
        os.makedirs(response_folder)
        copy(
            local_path('testdata/mhsds/accdb/00009_mps_header_wrong/mps/response/00001.csv'),
            os.path.join(response_folder, '00001.csv')
        )

        context_mock = Mock(PipelineContext)
        context_mock.working_folder = tempdir
        context_mock.dataframes = {
            'df_matched': DataFrameInfo(spark.createDataFrame([], mps_response_schema))
        }

        with raises(ValueError):
            CollectMPSResponseStage()._run(spark, context_mock)


def test_collects_mps_responses(spark: SparkSession):
    with TemporaryDirectory() as tempdir:
        response_folder = os.path.join(tempdir, 'mps', 'response')
        os.makedirs(response_folder)

        for index, filename in enumerate(['47669', '00000', '99999']):
            copy(
                local_path('testdata/mhsds/accdb/00012_incorrect_nhs_number/mps/response/{}.csv'.format(filename)),
                os.path.join(response_folder, '0000{}.csv'.format(index))
            )

        copy(
            local_path('testdata/mhsds/accdb/00010_mps_contents_wrong/mps/response/00001.csv'),
            os.path.join(response_folder, '00004.csv')
        )

        context_mock = Mock(PipelineContext)
        context_mock.working_folder = tempdir
        context_mock.dataframes = {
            'df_matched': DataFrameInfo(spark.createDataFrame([], mps_response_schema))
        }

        context = CollectMPSResponseStage()._run(spark, context_mock)

        assert context.dataframes['mps_result'].df.count() == 4
