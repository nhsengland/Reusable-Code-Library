from typing import Any

from pyspark.sql import SparkSession

from dsp.id import canonicalise_field_names
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage


class CanonicaliseFieldNamesStage(PipelineStage):
    """
    Legacy port of canonicalise_field_names stage from Pipeline 1.0
    """
    name = "canonicalise"

    def __init__(self, input_dataframe_name: str = "df", output_dataframe_name: str = "df"):
        """
        Args:
            input_dataframe_name (str): Name of the dataframe input to this stage to canonicalise; default value "df"
            output_dataframe_name (str): Name to give to the dataframe output by this stage; default value "df"
        """
        super().__init__(name=CanonicaliseFieldNamesStage.name, required_input_dataframes={input_dataframe_name},
                         provided_output_dataframes={output_dataframe_name})
        self._input_dataframe_name = input_dataframe_name
        self._output_dataframe_name = output_dataframe_name

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        data_frame = context.dataframes[self._input_dataframe_name].df
        canonicalised_data_frame = canonicalise_field_names(data_frame, context.to_metadata())
        new_context = context.clone()
        new_context.dataframes = {self._output_dataframe_name: DataFrameInfo(canonicalised_data_frame)}
        return new_context
