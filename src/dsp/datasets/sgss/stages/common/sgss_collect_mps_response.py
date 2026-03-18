from functools import reduce
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from dsp.datasets.sgss.ingestion.sgss.config import Fields
from dsp.integration.mps.generate_mps_file import colon_unique_reference
from dsp.pipeline.models import PipelineContext
from dsp.pipeline.stages.collect_mps_response import CollectMPSResponseStage


class SGSSCollectMPSResponseStage(CollectMPSResponseStage):
    name = 'sgss_collect_mps'

    def _combine_mps_response_dataframes(self, mps_response_dataframes: List, context: PipelineContext) -> DataFrame:
        return colon_unique_reference(
            reduce(
                lambda df_left, df_right: df_left.union(df_right.withColumn(Fields.PdsCrossCheckCondition, lit('MPS'))),
                mps_response_dataframes,
                context.dataframes[self._matched_dataframe_name].df
            )
        )
