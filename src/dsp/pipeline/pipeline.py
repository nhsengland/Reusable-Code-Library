from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Any, Callable, Sequence, Tuple, AbstractSet, FrozenSet
from typing import Set

from pyspark.sql import SparkSession, DataFrame

from dsp.pipeline.models import PipelineStageSummary, PipelineState, PipelineContext
from dsp.shared.logger import log_action, add_fields
from dsp.shared.models import PipelineStatus
from dsp.shared.store.counters import Counters


class PipelineException(Exception):
    context = None

    def __init__(self, message: Optional[str] = None, context: Optional[PipelineContext] = None):
        """
        Args:
            message (str): Human-readable description of the rejection
            context (PipelineContext): Context of the pipeline run resulting in rejection
        """
        super(PipelineException, self).__init__(message)
        self.context = context


class PipelineRejection(PipelineException):
    """
    Exception describing the rejection of a submission during pipeline execution
    """
    pass


class PipelineStageException(Exception):

    def __init__(self, stage_result: PipelineStageSummary):
        self.stage_result = stage_result
        result = stage_result.to_primitive() if stage_result is not None else None
        super(PipelineStageException, self).__init__(result)


class PipelineRunException(Exception):

    def __init__(self, pipeline_result: PipelineState):
        self.pipeline_result = pipeline_result
        result = pipeline_result.to_primitive() if pipeline_result is not None else None
        super(PipelineRunException, self).__init__(result)


class PipelineStage(ABC):
    """
    Base class for stage in pipeline
    """

    def __init__(self, name: str, required_input_dataframes: AbstractSet[str] = None,
                 provided_output_dataframes: AbstractSet[str] = None):
        self.name = name
        self._required_input_dataframes = frozenset(required_input_dataframes) \
            if required_input_dataframes else frozenset()
        self._provided_output_dataframes = frozenset(provided_output_dataframes) \
            if provided_output_dataframes else frozenset()

    @property
    def required_input_dataframes(self) -> FrozenSet[str]:
        return self._required_input_dataframes

    @property
    def provided_output_dataframes(self):
        return self._provided_output_dataframes

    def validated_provided_dataframes(self, provided_dataframes: AbstractSet[str]):
        return len(self._required_input_dataframes - provided_dataframes) == 0

    @log_action(phase="pipeline_stage")
    def run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) \
            -> Tuple[PipelineStageSummary, PipelineContext]:
        """
        Execute the pipeline stage

        Args:
            spark (SparkSession): The active Spark session
            context (PipelineContext): The context of the pipeline execution up to this point
            *args (Any): Varargs for pipeline run
            **kwargs (Any): Keyword args for pipeline run

        Returns:
            Tuple[PipelineStageSummary, PipelineContext]: A description of the result of this stage and the
                PipelineContext to forward to the next stage
        """

        add_fields(stage=self.name, **context.primitives)

        stage_start_time = datetime.utcnow()

        try:

            stage_result, context = self._run_stage(spark, stage_start_time, context, *args, **kwargs)

            add_fields(**stage_result)
            return stage_result, context

        except Exception as e:
            stage_result = self._create_stage_result(PipelineStatus.Failed, stage_start_time)
            add_fields(**stage_result)
            raise PipelineStageException(stage_result) from e

    @log_action(phase="run_pipeline_stage")
    def _run_stage(
            self, spark: SparkSession, stage_start_time: datetime, context: PipelineContext, *args: Any, **kwargs: Any
    ) -> Tuple[PipelineStageSummary, PipelineContext]:

        add_fields(stage=self.name, **context.primitives)

        try:

            if not self._preconditions_met(spark, context):
                return self._create_stage_result(PipelineStatus.Suspended, stage_start_time), context

            context = self._run(spark, context, *args, **kwargs)
            return self._create_stage_result(PipelineStatus.Success, stage_start_time), context

        except PipelineRejection:
            return self._create_stage_result(PipelineStatus.Rejected, stage_start_time), context

    # pylint: disable=unused-argument,no-self-use
    def _preconditions_met(self, spark: SparkSession, context: PipelineContext) -> bool:
        """
        Test whether this pipeline stage is ready to execute, ie. all external preconditions are met

        Args:
            spark (SparkSession): The current Spark session
            context (PipelineContext): The context of this pipeline at the beginning of this stage execution

        Returns:
            bool: Whether all prerequisites are met for successful execution of this stage
        """
        return True

    @abstractmethod
    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        """
        Internal run method to be overridden by subtypes to implement the stage

        Args:
            spark (SparkSession): The current Spark session
            context (PipelineContext): The context of this pipeline at the beginning of this stage execution
            *args (Any): Varags for pipeline run
            **kwargs (Any): Keyword args for pipeline run

        Returns:
            PipelineContext: The context to forward on to the next stage

        Raises:
            PipelineRejection: If the submission is rejected by this stage
        """
        raise NotImplementedError

    def _create_stage_result(self, status: str, start_time: datetime, terminal: bool = None) -> PipelineStageSummary:
        """
        Helper method for initialising result summaries

        Args:
            status (str): PipelineStatus at the conclusion of this stage
            start_time (datetime): Timestamp that stage execution began

        Returns:
            PipelineStageSummary: Summary for this pipeline stage
        """
        terminal = terminal if terminal is not None else status != PipelineStatus.Success

        return PipelineStageSummary({
            'stage': self.name,
            'status': status,
            'terminal': terminal,
            'start_time': start_time,
            'end_time': datetime.utcnow()
        })


class PipelineRejectionHandler(ABC):
    """
    Base class for entities that are executed when a pipeline is rejected, in order to handle any cleanup tasks required
    """

    @abstractmethod
    def handle_pipeline_rejection(self, context: PipelineContext):
        """
        Hook for handling a rejected pipeline

        Args:
            context: The context of the pipeline that has been rejected
        """
        raise NotImplementedError


class NoOpPipelineRejectionHandler(PipelineRejectionHandler):
    """
    An instance of PipelineRejectionHandler that has no action
    """

    def handle_pipeline_rejection(self, context: PipelineContext):
        """
        Does nothing
        """
        pass


class Pipeline:
    """
    A pipeline whose stages can be configured at initialisation time and execution can be suspended and resumed when
    requirements are not yet met
    """

    _UNDEFINED_STAGE_NAME = 'UNDEFINED'

    def __init__(self, stages: Sequence[PipelineStage],
                 pipeline_rejection_handler: PipelineRejectionHandler = NoOpPipelineRejectionHandler(),
                 output_loader: Callable[[SparkSession, str], DataFrame] = None,
                 undefined_suspension_stage_handler: Callable[[], str] = None):
        """
        Args:
            stages: The stages for this pipeline instance
            output_loader: Custom function for loading output files, defaults to reading outputs as parquet format
            undefined_suspension_stage_handler: Handler for suspensions against undefined stages, raises exception by
                default
        """

        def _default_output_loader(spark: SparkSession, path: str) -> DataFrame:
            return spark.read.parquet(path)

        def _default_undefined_suspension_stage_handler() -> str:
            raise NotImplementedError

        self._validate_pipeline_configuration(stages)
        self._stages = tuple(stages)
        self._pipeline_rejection_handler = pipeline_rejection_handler
        self.output_loader = output_loader or _default_output_loader
        self._undefined_suspension_stage_handler = undefined_suspension_stage_handler \
                                                   or _default_undefined_suspension_stage_handler

    @log_action()
    def run(
            self,
            spark: SparkSession,
            context: PipelineContext,
            *args: Any,
            previous_run_result: PipelineState = None,
            **kwargs: Any
    ) -> PipelineState:
        """
        Execute the pipeline

        Args:
            spark (SparkSession): The current Spark session
            context (PipelineContext): The context under which the pipeline is executing
            previous_run_result (PipelineState): The state of the previous pipeline execution, if this run represents a
                resumption of a previous run
            *args (Any): Additional varags to forward to pipeline stages
            **kwargs (Any): Additional keyword args to forward to pipeline stages

        Returns:
            PipelineState: A description of the state of this pipeline on conclusion
        """
        pipeline_start_time = datetime.utcnow()
        if previous_run_result is None:
            stages = self._stages
        else:
            initial_stage_name = previous_run_result.stages[-1].stage

            if initial_stage_name == self._UNDEFINED_STAGE_NAME:
                initial_stage_name = self._undefined_suspension_stage_handler()

            def index_of(elements: Sequence[Any], predicate: Callable[[Any], bool]) -> int:
                for index, element in enumerate(elements):
                    if predicate(element):
                        return index

                return -1

            stages = self._stages[index_of(self._stages, lambda elem: elem.name == initial_stage_name):]

        stage_summaries = []  # type: List[PipelineStageSummary]
        stage_summary = None

        for stage in stages:

            try:

                stage_summary, context = stage.run(spark, context.clone(), *args, **kwargs)
                stage_summaries.append(stage_summary)

            except PipelineStageException as e:

                result = self._create_pipeline_result(
                    context,
                    PipelineStatus.Failed,
                    stage_summaries + [e.stage_result],
                    pipeline_start_time,
                )

                raise PipelineRunException(result) from e

            if stage_summary.terminal:
                break

        if stage_summary is None:
            raise RuntimeError("No stages executed")

        if stage_summary.status == PipelineStatus.Suspended:
            return self._create_pipeline_result(
                context,
                PipelineStatus.Suspended,
                stage_summaries,
                pipeline_start_time,
            )

        if stage_summary.status == PipelineStatus.Failed:
            return self._create_pipeline_result(
                context, PipelineStatus.Failed, stage_summaries, pipeline_start_time,
            )

        if stage_summary.status == PipelineStatus.Rejected:
            self._pipeline_rejection_handler.handle_pipeline_rejection(context)

        return self._create_pipeline_result(
            context, stage_summary.status, stage_summaries, pipeline_start_time
        )

    @staticmethod
    def _validate_pipeline_configuration(stages: Sequence[PipelineStage]):
        provided_inputs = set()  # type: Set[str]
        for stage in stages:
            if not stage.validated_provided_dataframes(provided_inputs):
                raise ValueError(
                    "Pipeline configuration error: Given pipeline stages produce an incompatible flow: {}\n"
                    "Stage {} required inputs {}, got {}\n"
                    "Missing inputs: {}".format([stage.name for stage in stages], stage.name,
                                                stage.required_input_dataframes, provided_inputs,
                                                stage.required_input_dataframes - provided_inputs))

            provided_inputs = stage.provided_output_dataframes

    @staticmethod
    def _create_pipeline_result(
            context: PipelineContext,
            status: str,
            stages: Sequence[PipelineStageSummary],
            start_time: datetime,
    ) -> PipelineState:
        """
        Helper method for constructing pipeline results

        Args:
            context (PipelineContext): The pipeline context at result time
            status (str): The PipelineStatus of this run
            stages (Sequence[PipelineStageSummary]): Details of the result of each of this stage's pipeline
            start_time (datetime): The timestamp of the beginning of pipeline execution

        Returns:
            PipelineState: A pipeline state instance for the given parameters
        """
        persist_dataframes = status == PipelineStatus.Suspended
        context_serialized = {} if status == PipelineStatus.Failed else context.serialize(persist_dataframes)
        context.cleanup()

        return PipelineState({
            'id': Counters.get_next_pipeline_state_id(),
            'context': context_serialized,
            'status': status,
            'stages': stages,
            'start_time': start_time,
            'end_time': datetime.utcnow(),
            'validation_summary_extract_id': context.primitives.get('validation_summary_extract_id'),
            'deid_extract_id': context.primitives.get('deid_extract_id'),
            'extract_ids': context.primitives.get('extract_ids')
        })
