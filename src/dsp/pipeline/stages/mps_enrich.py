from typing import AbstractSet, Any, Callable, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from dsp.datasets.mps.response import Fields as MpsResponseFields
from dsp.integration.mps.constants import VALID_RESPONSE_CODES
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage


class MPSEnrichStage(PipelineStage):
    """
    Enriches with both PDS and MPS (coming soon).
    """

    name = 'enrich'

    Enrichment = Callable[[int, DataFrame, DataFrame], DataFrame]

    def __init__(
            self,
            enrichments: List[Enrichment],
            trace_dataframe_name: str = "df",
            mps_result_dataframe_name: str = "mps_result",
            passthrough_dataframes: AbstractSet[str] = None,
            skip_conditions: Optional[List[Callable]] = None
    ):
        """
        Args:
            enrichments: List of enrichments to be applied
            trace_dataframe_name: Name to give to the enriched dataframe output by this stage; default value "df"
            mps_result_dataframe_name: Name of the provided dataframe containing the complete MPS lookup result
            passthrough_dataframes: Names of dataframes to be passed through this stage unchanged
        """
        self._passthrough_dataframes = (
            frozenset(passthrough_dataframes) if passthrough_dataframes is not None else frozenset()
        )
        super().__init__(
            name=MPSEnrichStage.name,
            required_input_dataframes={trace_dataframe_name, mps_result_dataframe_name, *self._passthrough_dataframes},
            provided_output_dataframes={trace_dataframe_name, *self._passthrough_dataframes}
        )
        self._trace_dataframe_name = trace_dataframe_name
        self._mps_result_dataframe_name = mps_result_dataframe_name
        self._enrichments = enrichments
        self._skip_conditions = skip_conditions or []

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        skip_condition_results = [
            skip_condition(context, spark, self._trace_dataframe_name, None)
            for skip_condition in self._skip_conditions
        ]
        if not any(skip_condition_results):
            mps_result_dataframe = context.dataframes[self._mps_result_dataframe_name].df
            self._validate_contents(mps_result_dataframe)

            df_traced = context.dataframes[self._trace_dataframe_name].df
            for enrichment in self._enrichments:
                df_traced = enrichment(context.primitives['submission_id'], df_traced, mps_result_dataframe)

            context.dataframes = {
                self._trace_dataframe_name: DataFrameInfo(df_traced),
                **{df: context.dataframes[df] for df in self._passthrough_dataframes}
            }

        return context

    @staticmethod
    def _validate_contents(dataframe: DataFrame):
        if dataframe.filter(~col(MpsResponseFields.ERROR_SUCCESS_CODE).isin(list(VALID_RESPONSE_CODES))).count():
            raise ValueError('Disallowed codes in the MPS response contents')
