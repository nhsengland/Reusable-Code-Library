from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage


class TrimColumnContentsStage(PipelineStage):
    """
    Trims the contents of all the columns within the DataFrame
    """
    name = "trim_column_contents"

    def __init__(self, input_dataframe_name: str):
        super().__init__(
            name=self.name,
            required_input_dataframes={input_dataframe_name},
            provided_output_dataframes={input_dataframe_name},
        )
        self._input_dataframe_name = input_dataframe_name

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        df = context.dataframes[self._input_dataframe_name].df

        for col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))

        context.dataframes[self._input_dataframe_name] = DataFrameInfo(df)

        return context
