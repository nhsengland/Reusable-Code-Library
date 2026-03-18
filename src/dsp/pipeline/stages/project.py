import os
from typing import Any, Callable, Sequence, Tuple, Iterable, AbstractSet

from pyspark.sql import SparkSession, DataFrame, Column

from dsp.dam import result_selections
from dsp.derive.some_derivation import selection
from dsp.pipeline import DEFAULT_SAVE_MODE
from dsp.pipeline.models import PipelineContext
from dsp.pipeline.pipeline import PipelineStage
from dsp.shared.constants import PATHS
from dsp.shared.logger import log_action


class ProjectStage(PipelineStage):
    """
    Legacy port of projecting stage from Pipeline 1.0
    """
    name = "project"

    class Parameters:
        """
        Parameters for a projection to be performed
        """

        def __init__(self, source_dataframe: str, target_path: str = PATHS.OUT,
                     projections: Sequence[Tuple[str, Callable[[], Column]]] = None):
            """
            Args:
                source_dataframe (str): The name of the dataframe to extract from the pipeline context to be projected
                target_path (str): Path to write the projected dataframe to; defaults to PATHS.OUT
                projections (Sequence[Tuple[str, Callable[[], Column]]]): Sequence of tuples describing the name of the
                    projected field and a lambda returning a Pyspark Column describing the value of the field; defaults
                    to None
            """
            self.source_dataframe = source_dataframe
            self.target_path = target_path
            self.projections = tuple(projections) if projections is not None else None

    def __init__(self, output_directory: str = PATHS.OUT,
                 parameters: Iterable[Parameters] = frozenset({Parameters('df')}),
                 passthrough_dataframes: AbstractSet[str] = frozenset()):
        """
        Args:
            output_directory (str): The directory under which the projections will be written, defaults to PATHS.OUT
            parameters (Sequence[Parameters]): Parameters for projections to be performed; by default, defines a single
            projection of the dataframe with name 'df' to PATHS.OUT using the dataset-specified fields
            passthrough_dataframes: dataframes that the stage expects to pass through without writing, none by default
        """
        self._output_directory = output_directory
        self._output_targets = frozenset(parameters)
        self._passthrough_dataframes = frozenset(passthrough_dataframes)
        super().__init__(name="project_{}".format(output_directory),
                         required_input_dataframes={
                             *(output_target.source_dataframe for output_target in self._output_targets),
                             *self._passthrough_dataframes},
                         provided_output_dataframes={
                             *(output_target.source_dataframe for output_target in self._output_targets),
                             *self._passthrough_dataframes})

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        dataset_id = context.primitives['dataset_id']
        output_directory = os.path.join(context.scoped_working_folder, self._output_directory)
        save_mode = context.primitives.get('save_mode', DEFAULT_SAVE_MODE)

        for projection_parameters in self._output_targets:
            data_frame = context.dataframes[projection_parameters.source_dataframe].df
            context.dataframes[projection_parameters.source_dataframe].df = \
                self.project(data_frame, output_directory, save_mode, target_path=projection_parameters.target_path,
                             projections=projection_parameters.projections
                             if projection_parameters.projections else result_selections(dataset_id))

        context.dataframes = {key: value for key, value in context.dataframes.items()
                              if key in self._passthrough_dataframes
                              or key in {output_target.source_dataframe for output_target in self._output_targets}}
        return context

    @staticmethod
    @log_action(log_args=['output_directory', 'save_mode'])
    def project(derived_df: DataFrame, output_directory: str, save_mode: str,
                projections: Sequence[Tuple[str, Callable[[], Column]]], target_path: str = PATHS.OUT):
        output_df = selection(derived_df, projections)
        out_path = os.path.join(output_directory, target_path)
        output_df.write.mode(save_mode).parquet(out_path)
        return output_df
