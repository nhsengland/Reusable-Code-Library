from typing import Any, AbstractSet, Optional

from pyspark.sql import SparkSession

from dsp.datasets.sgss.ingestion.sgss.config import MPS_INCLUDE_FILTER
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage


class SGSSFilterMPSIncludeStage(PipelineStage):
    """
    Filters out rows that will not be traceable via MPS.
    """
    name = "sgss_filter_mps_include"

    def __init__(self, input_dataframe_name: str, output_dataframe_name: str,
                 passthrough_dataframes: Optional[AbstractSet[str]] = None):
        super().__init__(
            name=self.name,
            required_input_dataframes={input_dataframe_name},
            provided_output_dataframes={output_dataframe_name, *passthrough_dataframes},
        )
        self._input_dataframe_name = input_dataframe_name
        self._output_dataframe_name = output_dataframe_name

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        df = context.dataframes[self._input_dataframe_name].df

        df = df.filter(MPS_INCLUDE_FILTER)

        context.dataframes[self._output_dataframe_name] = DataFrameInfo(df)

        return context
