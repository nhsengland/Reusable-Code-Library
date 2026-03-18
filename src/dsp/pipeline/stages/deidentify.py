from typing import AbstractSet, Any, Callable

from pyspark import Broadcast
from pyspark.sql import SparkSession, DataFrame

from dsp.datasets.models.deid import SCHEMA as DEID_SCHEMA
from dsp.pipelines.deidentify.deidentify import DeidFields
from dsp.pipeline.models import PipelineContext
from dsp.pipeline.pipeline import PipelineStage
from dsp.shared.store.deid_jobs import DeIdJobs


class DeIdentifyStage(PipelineStage):
    """
        De-Identification of sensitive data
    """

    name = 'de-identify'

    def __init__(self, de_identified_dataframe_name: str,
                 deid_fields: DeidFields = None,
                 passthrough_dataframes: AbstractSet[str] = None,
                 deid_required: Callable[[PipelineContext, SparkSession], bool] = lambda context, spark: True,
                 resume_deid_submission: bool = False,
                 complete_after_retokenisation: bool = False
                 ):
        self._deidentified_dataframe_name = de_identified_dataframe_name
        self._deid_fields = deid_fields
        self._passthrough_dataframes = (
            frozenset(passthrough_dataframes)
            if passthrough_dataframes is not None
            else frozenset()
        )
        self._deid_required = deid_required
        self._resume_deid_submission = resume_deid_submission
        self._complete_after_retokenisation = complete_after_retokenisation
        super().__init__(name=DeIdentifyStage.name,
                         required_input_dataframes={self._deidentified_dataframe_name} | self._passthrough_dataframes,
                         provided_output_dataframes={self._deidentified_dataframe_name} | self._passthrough_dataframes)
    
    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, broadcast_state: Broadcast = None,
             **kwargs: Any) -> PipelineContext:
        if self._deid_required(context, spark):
            df_deid = spark.createDataFrame([], DEID_SCHEMA)
            df_clear = context.dataframes[self._deidentified_dataframe_name].df
            df_deid = df_deid.union(self._deid_fields.de_id_fields(spark, df_clear))
            df_deid = df_deid.drop_duplicates(['clear', 'pseudo_type'])
            submission_id = context.primitives['submission_id']
            location = broadcast_state.value['raw_bucket_path']
            context.primitives['deid_job_id'] = self.create_de_id_file(location, submission_id, df_deid)

        return context

    def create_de_id_file(self, location: str, submission_id: int, df_deid: DataFrame) -> int:
        job_id = DeIdJobs.create_deid_job(location, submission_id=submission_id,
                                          resume_deid_submission=self._resume_deid_submission,
                                          complete_after_retokenisation=self._complete_after_retokenisation)
        out_path = DeIdJobs.get_by_id(job_id).source
        df_deid.write.parquet(out_path)
        DeIdJobs.mark_ready(job_id)
        return job_id
