from typing import Any, Callable, Tuple, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import trim, col

from dsp.common import canonical_name
from dsp.dam import strip_whitespace_fields, extra_field_cleansing
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage


class CleanseDataFrameStage(PipelineStage):
    """
    Legacy port of cleansing stage from Pipeline 1.0
    """
    name = "cleanse"

    def __init__(self, input_dataframe_name: str = "df", output_dataframe_name: str = "df"):
        """
        Args:
            input_dataframe_name (str): Name of the dataframe input to this stage to cleanse; default value "df"
            output_dataframe_name (str): Name to give to the dataframe output by this stage; default value "df"
        """
        super().__init__(name=CleanseDataFrameStage.name, required_input_dataframes={input_dataframe_name},
                         provided_output_dataframes={output_dataframe_name})
        self._input_dataframe_name = input_dataframe_name
        self._output_dataframe_name = output_dataframe_name

    @staticmethod
    def strip_whitespace(df: DataFrame, strip_fields: List[str]) -> DataFrame:
        strip_fields = [canonical_name(f) for f in strip_fields]
        for name in df.schema.names:
            c_name = canonical_name(name)
            if c_name not in strip_fields:
                continue
            df = df.withColumn(name, trim(col(name)))
        return df

    @staticmethod
    def apply_extra_cleansing(df: DataFrame, extra_cleansing: List[Tuple[str, Callable]]) -> DataFrame:
        for col_name, cleansing_function in extra_cleansing:
            c_name = canonical_name(col_name)
            df = df.withColumn(c_name, cleansing_function(c_name))
        return df

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        dataset_id = context.primitives['dataset_id']
        data_frame = context.dataframes[self._input_dataframe_name].df
        data_frame_no_whitespace = self.strip_whitespace(data_frame, strip_whitespace_fields(dataset_id))
        data_frame_cleansed = self.apply_extra_cleansing(data_frame_no_whitespace, extra_field_cleansing(dataset_id))
        new_context = context.clone()
        new_context.dataframes = {self._output_dataframe_name: DataFrameInfo(data_frame_cleansed)}
        return new_context
