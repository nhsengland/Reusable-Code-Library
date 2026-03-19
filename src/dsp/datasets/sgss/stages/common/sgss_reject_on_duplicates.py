from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from dsp.datasets.sgss.ingestion.sgss.config import Fields
from dsp.pipeline.models import PipelineContext
from dsp.pipeline.pipeline import PipelineStage, PipelineRejection


class SGSSRejectOnDuplicatesStage(PipelineStage):
    """
    Rejects the submission being processed if there are any duplicate CDR_Specimen_Request_SK identifiers within the
    same submission.
    """
    name = "sgss_reject_on_duplicates"

    def __init__(self, input_dataframe_name: str):
        super().__init__(
            name=self.name,
            required_input_dataframes={input_dataframe_name},
            provided_output_dataframes={input_dataframe_name}
        )
        self._input_dataframe_name = input_dataframe_name

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        df = context.dataframes[self._input_dataframe_name].df

        unique_id_key = Fields.CDR_Specimen_Request_SK

        df_duplicates = df.select(unique_id_key).groupBy(unique_id_key).count() \
            .where(col('count') > 1).drop('count')

        if df_duplicates.count() > 0:
            raise ValueError(f'Submission contains duplicate {unique_id_key} values.')

        return context
