from typing import Any, Dict, Optional, Set, Callable

from pyspark.sql import SparkSession

from dsp.pipeline.models import PipelineContext
from dsp.pipeline.pipeline import PipelineStage, PipelineRejection


def _no_op(_: SparkSession, __: PipelineContext):
    pass


class SubmissionTypeSelectingStage(PipelineStage):
    """
    Pipeline stage which defers execution to a delegate depending on the file type of the current submission
    """
    name = "submission_type"

    def __init__(self, stages_by_type: Dict[str, PipelineStage],
                 notify_invalid_filetype_callback: Callable[[SparkSession, PipelineContext], None] = _no_op):
        """
        Args:
            stages_by_type: Dictionary of delegates keyed by file type of submission
            notify_invalid_filetype_callback: Function to be invoked on invocation with an unexpected file type
        """
        if not stages_by_type:
            raise ValueError("No stages specified")

        output_frames = None  # type: Optional[Set[str]]
        for stage in stages_by_type.values():
            if stage.required_input_dataframes:
                raise ValueError("Stage {} is not a loader, requires input dataframes".format(stage.name))

            if output_frames is None:
                output_frames = stage.provided_output_dataframes
            elif stage.provided_output_dataframes != output_frames:
                raise ValueError("Given stages do not have matching outputs")

        super().__init__(name=SubmissionTypeSelectingStage.name, provided_output_dataframes=output_frames)

        self._stages_by_type = dict(stages_by_type)
        self.name = "load_by_type: {}".format({key: value.name for key, value in self._stages_by_type.items()})
        self._notify_invalid_filetype_callback = notify_invalid_filetype_callback

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        file_type = context.primitives.get('file_type')
        if file_type not in self._stages_by_type:
            err_msg = "No loader for submission of type {}".format(file_type)
            self._notify_invalid_filetype_callback(spark, context)
            raise PipelineRejection(err_msg)

        return self._stages_by_type[file_type]._run(spark, context, *args, **kwargs)
