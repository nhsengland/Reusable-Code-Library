import os
from typing import Any

from pyspark.sql import SparkSession

from dsp.pipeline.models import PipelineContext
from dsp.pipeline.pipeline import PipelineStage
from dsp.shared.constants import PATHS


class SGSSDataWriterStage(PipelineStage):
    """
    Writes the input DataFrame into the submission's PATHS.OUT in Parquet format
    """
    name = "sgss_data_writer"

    def __init__(self, input_dataframe_name: str):
        super().__init__(
            name=self.name,
            required_input_dataframes={input_dataframe_name},
            provided_output_dataframes={input_dataframe_name}
        )
        self._input_dataframe_name = input_dataframe_name

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        output_directory = os.path.join(context.working_folder, PATHS.OUT)

        df = context.dataframes[self._input_dataframe_name].df
        df.write.parquet(output_directory)

        return context
