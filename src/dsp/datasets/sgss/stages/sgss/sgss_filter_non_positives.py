from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from dsp.datasets.sgss.ingestion.sgss.config import Fields, POSITIVE_ORGANISM_SPECIES_NAME
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage


class SGSSFilterNonPositivesStage(PipelineStage):
    """
    Filters out rows that contain a non-positive result
    """
    name = "sgss_filter_non_positives"

    def __init__(self, input_dataframe_name: str, output_dataframe_name: str):
        super().__init__(
            name=self.name,
            required_input_dataframes={input_dataframe_name},
            provided_output_dataframes={output_dataframe_name},
        )
        self._input_dataframe_name = input_dataframe_name
        self._output_dataframe_name = output_dataframe_name

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        df = context.dataframes[self._input_dataframe_name].df

        df = df.filter(col(Fields.Organism_Species_Name) == lit(POSITIVE_ORGANISM_SPECIES_NAME))

        context.dataframes[self._output_dataframe_name] = DataFrameInfo(df)

        return context
