import os
from typing import Any, AbstractSet, Optional, Union, Callable, List

from pyspark.sql import SparkSession

from dsp.integration.mps.generate_mps_file import generate_mps_files
from dsp.pipeline.models import PipelineContext
from dsp.pipeline.pipeline import PipelineStage


class MPSCheckStage(PipelineStage):
    """
    Fires off a request to MPS and suspends the pipeline.
    """

    name = 'mps_check'

    CHUNK_SIZE_ROWS = 500000

    def __init__(
            self,
            trace_dataframe_name: str = "df",
            passthrough_dataframes: AbstractSet[str] = None,
            matched_output_dataframe_name: str = "df_matched",
            unmatched_output_dataframe_name: str = "df_unmatched",
            local_id_prefix: Optional[str] = None,
            mailbox_from: Optional[Union[str, Callable]] = None,
            skip_conditions: Optional[List[Callable]] = None
    ):
        """
        Args:
            trace_dataframe_name (str): Name to give to the enriched dataframe output by this stage; default value "df"
            passthrough_dataframes (AbstractSet[str]): pass through
            matched_output_dataframe_name (str): Name to give to the matched dataframe by this stage;
                                                 default value "df_matched"
            unmatched_output_dataframe_name (str): Name to give to the unmatched dataframe by this stage;
                                                   default value "df_unmatched"
            local_id_prefix (Optional[str]): An optional attribute to allow for specifying the local ID prefix to use
                                             for the MPS request
            mailbox_from (Optional[Union[str, Callable]]): An optional attribute that can provide the mailbox to send
                                                           MPS requests from, which can either be a string or a callable
                                                           to retrieve the mailbox from.
        """
        self._passthrough_dataframes = (
            frozenset(passthrough_dataframes) if passthrough_dataframes is not None else frozenset()
        )
        super().__init__(
            name=MPSCheckStage.name,
            required_input_dataframes={
                trace_dataframe_name, matched_output_dataframe_name,
                unmatched_output_dataframe_name, *self._passthrough_dataframes
            },
            provided_output_dataframes={
                trace_dataframe_name, matched_output_dataframe_name,
                unmatched_output_dataframe_name, *self._passthrough_dataframes
            }
        )
        self._trace_dataframe_name = trace_dataframe_name
        self._matched_dataframe_name = matched_output_dataframe_name
        self._unmatched_output_dataframe_name = unmatched_output_dataframe_name
        self._local_id_prefix = local_id_prefix
        self._mailbox_from = mailbox_from
        self._skip_conditions = skip_conditions or []

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        skip_condition_results = [
            skip_condition(context, spark)
            for skip_condition in self._skip_conditions
        ]
        if not any(skip_condition_results):
            # if there are no unmatched records from PDS check, don't reach out to MPS
            if context.dataframes[self._unmatched_output_dataframe_name].df.count():
                submission_id = str(context.primitives['submission_id'])
                mailbox_from = self._mailbox_from() if callable(self._mailbox_from) else self._mailbox_from
                generate_mps_files(
                    spark,
                    os.path.join(context.working_folder, 'mps'),
                    submission_id,
                    context.dataframes[self._unmatched_output_dataframe_name].df,
                    self.CHUNK_SIZE_ROWS,
                    self._local_id_prefix,
                    mailbox_from
                )

            context.dataframes = {df: context.dataframes[df] for df in self.provided_output_dataframes}

        return context
