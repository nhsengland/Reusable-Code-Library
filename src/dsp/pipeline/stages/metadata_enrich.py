from typing import Any, AbstractSet

from pyspark.sql import SparkSession

from dsp.dam import meta_enrichments
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage


class MetadataEnrichStage(PipelineStage):
    """
    Enrich with Metadata.
    """

    name = 'enrich'

    def __init__(
            self,
            enrich_dataframe_name: str = "df",
            passthrough_dataframes: AbstractSet[str] = None
    ):
        """
        Args:
            enrich_dataframe_name (str): Name to give to the enriched dataframe output by this stage; default value "df"
            passthrough_dataframes (AbstractSet[str]): pass through
        """
        self._passthrough_dataframes = (
            frozenset(passthrough_dataframes) if passthrough_dataframes is not None else frozenset()
        )
        super().__init__(
            name=MetadataEnrichStage.name,
            required_input_dataframes={
                enrich_dataframe_name, *self._passthrough_dataframes
            },
            provided_output_dataframes={
                enrich_dataframe_name, *self._passthrough_dataframes
            }
        )
        self._enrich_dataframe_name = enrich_dataframe_name

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        enrich_df = context.dataframes[self._enrich_dataframe_name].df
        ds_enrichments = meta_enrichments(context.primitives['dataset_id'])
        for enrichment in ds_enrichments:
            enrich_df = enrichment(spark, enrich_df, context.to_metadata())

        new_context = context.clone()
        new_context.dataframes = dict(
            **{self._enrich_dataframe_name: DataFrameInfo(enrich_df)},
            **{dataframe_name: context.dataframes[dataframe_name] for dataframe_name in self._passthrough_dataframes}
        )
        return new_context


