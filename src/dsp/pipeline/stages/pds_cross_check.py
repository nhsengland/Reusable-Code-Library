from typing import Any, AbstractSet, Optional, Callable, List

from pyspark.sql import SparkSession

from dsp.dam import mps_request
from dsp.integration.mps.cross_check import cross_check
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage


class PDSCrossCheckStage(PipelineStage):
    """
    Fires off a request to PDS aka local trace check, attempts an enrichment from PDS
    and returns an enriched dataframe as well as a dataframe of unmatched Referrals,
    which can potentially be enriched through MPS.
    """

    name = 'pds_cross_check'

    def __init__(
            self,
            trace_dataframe_name: str = "df",
            passthrough_dataframes: AbstractSet[str] = None,
            matched_output_dataframe_name: str = "df_matched",
            unmatched_output_dataframe_name: str = "df_unmatched",
            skip_conditions: Optional[List[Callable]] = None
    ):
        """
        Args:
            trace_dataframe_name (str): Name to give to the enriched dataframe output by this stage; default value "df"
            passthrough_dataframes (AbstractSet[str]): pass through
            matched_output_dataframe_name (str): Name to give to the matched dataframe by this stage;
                                                 default value "df_matched"
            unmatched_output_dataframe_name (str): Name to give to the unmatched dataframe by this stage;
                                                   default value "df_unmatched"
        """
        self._passthrough_dataframes = (
            frozenset(passthrough_dataframes) if passthrough_dataframes is not None else frozenset()
        )
        super().__init__(
            name=PDSCrossCheckStage.name,
            required_input_dataframes={trace_dataframe_name, *self._passthrough_dataframes},
            provided_output_dataframes={
                trace_dataframe_name, matched_output_dataframe_name,
                unmatched_output_dataframe_name, *self._passthrough_dataframes
            }
        )
        self._trace_dataframe_name = trace_dataframe_name
        self._matched_dataframe_name = matched_output_dataframe_name
        self._unmatched_dataframe_name = unmatched_output_dataframe_name
        self._skip_conditions = skip_conditions or []

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        new_context = context.clone()

        skip_condition_results = [
            skip_condition(context, spark, self._trace_dataframe_name, None)
            for skip_condition in self._skip_conditions
        ]
        if not any(skip_condition_results):
            df_records = context.dataframes[self._trace_dataframe_name].df
            df_mps_request = mps_request(context.primitives['dataset_id'])(df_records)
            df_matched, df_unmatched = cross_check(spark, df_mps_request)
            context.dataframes[self._matched_dataframe_name] = DataFrameInfo(df_matched)
            context.dataframes[self._unmatched_dataframe_name] = DataFrameInfo(df_unmatched)

            new_context.dataframes = {
                **{
                    self._trace_dataframe_name: context.dataframes[self._trace_dataframe_name],
                    self._matched_dataframe_name: DataFrameInfo(df_matched),
                    self._unmatched_dataframe_name: DataFrameInfo(df_unmatched)
                },
                **{df: context.dataframes[df] for df in self._passthrough_dataframes}
            }

        return new_context
