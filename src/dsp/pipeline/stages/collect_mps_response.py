import os
from functools import reduce
from typing import Any, List, Dict, Set, Tuple, Callable, Optional

from pyspark.sql import SparkSession, DataFrame

from dsp.datasets.mps.response_header import Fields as MpsResponseHeaderFields
from dsp.integration.mps.constants import SUCCESSFUL_RESPONSE_HEADER_CODES
from dsp.integration.mps.generate_mps_file import colon_unique_reference
from dsp.integration.mps.mps_response_loader import csv_loader
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage
from dsp.shared.aws import s3_split_path, s3_bucket
from dsp.shared.models import MpsManifestStatus
from dsp.shared.store.base import ModelNotFound
from dsp.shared.store.mps_manifests import MpsManifestsStore, MpsManifests

HeaderErrorHandler = Callable[[Dict[str, Any]], None]
HeaderErrorHandlerProvider = Callable[[PipelineContext], HeaderErrorHandler]


def default_error_header_handler(header: Dict[str, Any]):
    """
    Default handler for non-success headers maintaining legacy behaviour

    Args:
        header: The header fields received

    Raises:
        ValueError: For all inputs, to fail the invoking pipeline
    """
    error_code = header[MpsResponseHeaderFields.FILE_RESPONSE_CODE]
    raise ValueError(
        'Unacceptable FILE_RESPONSE_CODE in the header - found {} - need to investigate'.format(error_code)
    )


class CollectMPSResponseStage(PipelineStage):
    """
    Collect the response of an MPS request into a single dataframe including the values looked up from local PDS
    """

    name = "mps_collect"

    def __init__(self, matched_dataframe_name: str = "df_matched", unmatched_dataframe_name: str = "df_unmatched",
                 mps_result_dataframe_name: str = "mps_result", mps_manifests_store: MpsManifestsStore = MpsManifests,
                 passthrough_dataframe_names: Set[str] = set(),
                 header_error_handler_provider: HeaderErrorHandlerProvider =
                 lambda _: default_error_header_handler,
                 skip_conditions: Optional[List[Callable]] = None):
        """
        Args:
            matched_dataframe_name: Name of the dataframe containing records already matched from local PDS
            unmatched_dataframe_name: Name of the dataframe containing records not present in local PDS, in a form that
                can be submitted to MPS
            mps_result_dataframe_name: Name  of the dataframe to return the collected MPS results as
            mps_manifests_store: Store providing lookup for MPS manifests for submission IDs
            passthrough_dataframe_names: Dataframes that this stage should expect to receive and output unchanged
            header_error_handler_provider: Curried function which, when invoked with a context for a pipeline, returns a
                handler to deal with headers received with a non-success status code
        """
        super().__init__(name=CollectMPSResponseStage.name,
                         required_input_dataframes={matched_dataframe_name, unmatched_dataframe_name,
                                                    *passthrough_dataframe_names},
                         provided_output_dataframes={mps_result_dataframe_name, *passthrough_dataframe_names})

        self._matched_dataframe_name = matched_dataframe_name
        self._unmatched_dataframe_name = unmatched_dataframe_name
        self._mps_result_dataframe_name = mps_result_dataframe_name
        self._mps_manifests_store = mps_manifests_store
        self._passthrough_dataframe_names = passthrough_dataframe_names
        self._header_error_handler_provider = header_error_handler_provider
        self._skip_conditions = skip_conditions or []

    def _clean_up_cached_dfs(self, context: PipelineContext, mps_response_dataframes: Optional[List] = None):
        if mps_response_dataframes:
            for mps_response_df in mps_response_dataframes:
                mps_response_df.unpersist()

        if self._matched_dataframe_name in context.dataframes:
            context.dataframes[self._matched_dataframe_name].df.unpersist()

        if self._unmatched_dataframe_name in context.dataframes:
            context.dataframes[self._unmatched_dataframe_name].df.unpersist()
    
    def _skip_stage(self, context):
        skip_condition_results = [
            skip_condition(context)
            for skip_condition in self._skip_conditions
        ]
        if any(skip_condition_results):
            return True
        
        return False
        
    def _preconditions_met(self, spark: SparkSession, context: PipelineContext):
        if self._skip_stage(context):
            return True
        
        if not context.dataframes[self._unmatched_dataframe_name].df.count():
            return True
        submission_id = str(context.primitives['submission_id'])

        try:
            mps_manifest = self._mps_manifests_store.get_by_submission_id(submission_id)
        except ModelNotFound:
            return False
        else:
            return mps_manifest.status == MpsManifestStatus.Success

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:

        if not self._skip_stage(context):
            mps_response_dataframes, mps_response_headers = self._find_mps_response_chunks(spark, context)

            try:
                self._validate_headers(mps_response_headers, self._header_error_handler_provider(context))
            except Exception:
                self._clean_up_cached_dfs(context, mps_response_dataframes)
                raise

            all_mps_dataframe = self._combine_mps_response_dataframes(mps_response_dataframes, context)

            self._clean_up_cached_dfs(context, mps_response_dataframes)

            context.dataframes = {
                self._mps_result_dataframe_name: DataFrameInfo(all_mps_dataframe),
                **{passthrough_dataframe_name: context.dataframes[passthrough_dataframe_name]
                   for passthrough_dataframe_name in self._passthrough_dataframe_names}
            }
        return context

    def _combine_mps_response_dataframes(self, mps_response_dataframes: List, context: PipelineContext) -> DataFrame:
        return colon_unique_reference(
            reduce(
                lambda df_left, df_right: df_left.union(df_right),
                mps_response_dataframes,
                context.dataframes[self._matched_dataframe_name].df
            )
        )

    @staticmethod
    def _find_mps_response_chunks(spark: SparkSession, context: PipelineContext) -> Tuple[List, List]:
        if context.working_folder.startswith('s3'):
            scheme, bucket, prefix = s3_split_path(context.working_folder)
            prefix = os.path.join(prefix, 'mps', 'response')
            response_chunks = s3_bucket(bucket).objects.filter(Prefix=prefix)
            dfs_headers = [
                csv_loader(spark, '{}://{}/{}'.format(scheme, bucket, chunk.key), True) for chunk in response_chunks
            ]

        else:
            folder = os.path.join(context.working_folder, 'mps', 'response')
            if not os.path.exists(folder):
                return [], []
            dfs_headers = [
                csv_loader(spark, os.path.join(folder, filename), True) for filename in os.listdir(folder)
            ]

        if dfs_headers:
            return tuple(zip(*dfs_headers))
        return [], []

    @staticmethod
    def _validate_headers(headers: List[Dict], error_header_handler: HeaderErrorHandler = default_error_header_handler):
        for header in headers:
            error_code = header[MpsResponseHeaderFields.FILE_RESPONSE_CODE]
            if error_code not in SUCCESSFUL_RESPONSE_HEADER_CODES:
                error_header_handler(header)
