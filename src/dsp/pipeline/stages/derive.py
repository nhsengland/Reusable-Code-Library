from functools import partial
from typing import Any, Dict, AbstractSet, Type

from pyspark import Broadcast, Row
from pyspark.sql import SparkSession

from dsp.dam import derivations, derivations_struct
from dsp.derive.some_derivation import derive
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage
from dsp.pipeline.worker_pipeline_stage import worker_pipeline_stage
from dsp.common.structured_model import DSPStructuredModel
from dsp.shared.logger import log_action


class DeriveStage(PipelineStage):
    """
    Legacy port of deriving stage from Pipeline 1.0
    """
    name = "derive"

    def __init__(self, input_dataframe_name: str = "df", output_dataframe_name: str = "df",
                 passthrough_dataframes: AbstractSet[str] = None):
        """
        Args:
            input_dataframe_name (str): Name of the dataframe input to this stage to perform derivations; default value
                "df"
            output_dataframe_name (str): Name to give to the dataframe output by this stage; default value "df"
            passthrough_dataframes (str): Dataframes to copy from the provided context to the produced context unchanged
        """
        self._input_dataframe_name = input_dataframe_name
        self._output_dataframe_name = output_dataframe_name
        self._passthrough_dataframes = frozenset(passthrough_dataframes) \
            if passthrough_dataframes is not None else frozenset()
        super().__init__(name=DeriveStage.name,
                         required_input_dataframes={input_dataframe_name, *self._passthrough_dataframes},
                         provided_output_dataframes={output_dataframe_name, *self._passthrough_dataframes})

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        dataset_id = context.primitives['dataset_id']
        data_frame = context.dataframes[self._input_dataframe_name].df
        derived_data_frame = derive(data_frame, derivations(dataset_id))
        new_context = context.clone()
        new_context.dataframes = dict(
            **{self._output_dataframe_name: DataFrameInfo(derived_data_frame)},
            **{dataframe_name: context.dataframes[dataframe_name] for dataframe_name in self._passthrough_dataframes}
        )
        return new_context


class StructuredDerivationsStage(PipelineStage):
    """
    Legacy port of deriving_struct stage from Pipeline 1.0
    """
    name = "derive_structured"

    class Parameters:
        """
        Parameters for structured derivations to be performed
        """

        def __init__(self, input_dataframe_name: str, output_dataframe_name: str,
                     root_model_type: Type[DSPStructuredModel] = None):
            """
            Args:
                input_dataframe_name (str): Dataframe to perform structured derivations against
                output_dataframe_name (str): Dataframe to write result to
                root_model_type (Type[DSPStructuredModel]): Root type of structured model for the input dataframe
            """
            self.input_dataframe_name = input_dataframe_name
            self.output_dataframe_name = output_dataframe_name
            self.root_model_type = root_model_type

    def __init__(self, parameters: AbstractSet[Parameters] = None,
                 passthrough_dataframes: AbstractSet[str] = None):
        """
        Args:
            parameters (AbstractSet[Parameters]): Parameters of derivations to perform; if unspecified, will perform
                derivations against input dataframe 'df' using the root model type associated with the dataset ID, and
                return the result as dataframe 'df'
            passthrough_dataframes (str): Dataframes to copy from the provided context to the produced context unchanged
        """
        self._parameters = frozenset(parameters) \
            if parameters is not None else frozenset({StructuredDerivationsStage.Parameters('df', 'df')})
        self._passthrough_dataframes = frozenset(passthrough_dataframes) \
            if passthrough_dataframes is not None else frozenset()
        super().__init__(
            name=StructuredDerivationsStage.name,
            required_input_dataframes={
                *{parameter.input_dataframe_name for parameter in self._parameters}, *self._passthrough_dataframes
            },
            provided_output_dataframes={
                *{parameter.output_dataframe_name for parameter in self._parameters}, *self._passthrough_dataframes
            }
        )

    # noinspection PyUnusedLocal
    @staticmethod
    @worker_pipeline_stage()  # Consumes broadcast_state
    def derive_struct(
            row: Row,
            root_model_type: Type[DSPStructuredModel],
            broadcast_state: Broadcast = None
    ) -> Row:  # pylint: disable=unused-argument
        """
        Perform structural derivations against a single row

        Args:
            row (Row): The dataframe row to have derivations applied
            root_model_type (Type[DSPStructuredModel]): Model to adapt the given dataframe into
            broadcast_state (Broadcast): broadcast state

        Returns:
            Row: A row with derivations performed
        """
        return root_model_type(row.asDict(recursive=True), accept_derived_values=False).as_row()

    @classmethod
    def derive(cls, spark, data_frame, dataset_id, root_model_type, broadcast_state: Broadcast):
        if root_model_type is None:
            root_model_type = derivations_struct(dataset_id)

        if root_model_type is None:
            return data_frame

        rdd = data_frame.rdd

        row_with_structured_derivations_rdd = rdd.map(
            partial(
                cls.derive_struct,
                root_model_type=root_model_type,
                broadcast_state=broadcast_state,
            )
        )

        # we're explicitly creating the dataframe struct, to deal with special cases where implicit df schema creation
        # would not work - such as when one column is always null
        model_schema = root_model_type.get_struct()
        return spark.createDataFrame(row_with_structured_derivations_rdd, model_schema)

    @log_action()
    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, broadcast_state: Broadcast = None,
             **kwargs: Any) -> PipelineContext:
        dataset_id = context.primitives['dataset_id']

        new_dataframes = {}  # type: Dict[str, DataFrameInfo]

        for parameter in self._parameters:
            data_frame = context.dataframes[parameter.input_dataframe_name].df
            derived_data_frame = self.derive(
                spark, data_frame, dataset_id,
                root_model_type=parameter.root_model_type,
                broadcast_state=broadcast_state
            )
            new_dataframes[parameter.output_dataframe_name] = DataFrameInfo(derived_data_frame)

        context.dataframes = {
            **new_dataframes,
            **{dataframe_name: context.dataframes[dataframe_name] for dataframe_name in self._passthrough_dataframes}
        }

        return context
