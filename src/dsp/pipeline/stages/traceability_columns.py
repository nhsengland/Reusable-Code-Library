from typing import Any, AbstractSet, Union, Mapping

from pyspark.sql import SparkSession

from dsp.pipeline.loading import add_traceability_columns
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage


class AddTraceabilityColumnsStage(PipelineStage):
    """
    Stage to add traceability columns to a single dataframe
    """
    name = "add_traceability"

    def __init__(
            self, trace_dataframes: Union[AbstractSet[str], Mapping[str, str]] = None,
            passthrough_dataframes: AbstractSet[str] = None
    ):
        """
        Args:
            trace_dataframes (AbstractSet[str]): Dataframes to apply traceability columns to, default {'df'}
            passthrough_dataframes (AbstractSet[str]): Dataframes to passthrough unchanged
        """
        if trace_dataframes is None:
            self._trace_dataframes = {'df': 'df'}
        elif isinstance(trace_dataframes, AbstractSet):
            self._trace_dataframes = {dataframe_name: dataframe_name for dataframe_name in trace_dataframes}
        else:
            self._trace_dataframes = dict(trace_dataframes)
        self._passthrough_dataframes = frozenset(passthrough_dataframes) \
            if passthrough_dataframes is not None else frozenset()
        super().__init__(name=AddTraceabilityColumnsStage.name,
                         required_input_dataframes={*self._trace_dataframes.keys(), *self._passthrough_dataframes},
                         provided_output_dataframes={*self._trace_dataframes.values(), *self._passthrough_dataframes})

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        new_context = context.clone()
        new_context.dataframes = {
            **{
                output_df: DataFrameInfo(
                    add_traceability_columns(spark, context.dataframes[input_df].df, context.to_metadata()))
                for input_df, output_df in self._trace_dataframes.items()},
            **{df: context.dataframes[df] for df in self._passthrough_dataframes}
        }

        return new_context
