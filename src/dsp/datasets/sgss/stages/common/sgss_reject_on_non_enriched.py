from typing import Any

from pyspark.sql import SparkSession

from dsp.datasets.sgss.ingestion.sgss.config import MPS_INCLUDE_FILTER, Fields
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage, PipelineRejection


class SGSSRejectOnNonEnrichedStage(PipelineStage):
    """
    Rejects the submission being processed if any of the records we have looked up against PDS and MPS do not contain
    a Person_ID assigned to them. Additionally, substitutes Person_ID in the records with a blank string where
    it is set to null.
    """
    name = "sgss_reject_on_non_enriched"

    def __init__(self, input_dataframe_name: str, output_dataframe_name: str):
        super().__init__(
            name=self.name,
            required_input_dataframes={input_dataframe_name},
            provided_output_dataframes={output_dataframe_name}
        )
        self._input_dataframe_name = input_dataframe_name
        self._output_dataframe_name = output_dataframe_name

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        df = context.dataframes[self._input_dataframe_name].df

        total_count = df.where(MPS_INCLUDE_FILTER).count()

        if total_count == 0:
            # TODO: It doesn't make sense to me that this takes place *after* all the other stages have taken place
            # rather than upon applying the filter initially in SGSSFilterMPSIncludeStage. It could be that this
            # intentionally takes place after the ProjectStage so that we have traceability of the submission
            # output in Parquet form.
            raise ValueError('No records eligible for PDS or MPS lookup.')

        enriched_count = df.where(f'{Fields.PERSON_ID} is not null').count()
        if total_count != enriched_count:
            raise ValueError(f'Person_ID not present in records eligible for enrichment. '
                             f'Total count: {total_count}, enriched count: {enriched_count}')

        df_null_person_id_to_blank_string = df.fillna({Fields.PERSON_ID: ''})

        context.dataframes[self._output_dataframe_name] = DataFrameInfo(df_null_person_id_to_blank_string)

        return context
