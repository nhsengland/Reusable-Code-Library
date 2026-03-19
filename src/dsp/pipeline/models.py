import os
from copy import deepcopy
from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional

from pyspark.rdd import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from schematics import Model
from schematics.types import (BooleanType, DateTimeType, DictType, IntType,
                              ListType, ModelType, StringType)

from dsp.common.context_memory_operations_helper import MemoryOperationsMixin
from dsp.common.schematics_types import AnyType
from dsp.dam import dataset_without_table_structure
from dsp.datasets.common import DQMessageType
from dsp.dq_files.output import Fields as DQFields
from dsp.pipeline import Metadata
from dsp.shared.common import concurrent_tasks, enumlike_values
from dsp.shared.constants import PATHS
from dsp.shared.models import PipelineResult, PipelineStatus, Submission
from dsp.shared.store.submissions import Submissions


class DataFrameInfo:
    """
    DataFrame decorator to store additional data for the purpose of serialization
    """
    df = None  # type: DataFrame
    temporary_view_name = None  # type: Optional[str]

    def __init__(self, df: DataFrame, temporary_view_name: str = None):
        """
        Args:
            df (DataFrame): The DataFrame to decorate
            temporary_view_name (str): The name of the temporary view at which this DataFrame is registered, if
                applicable
        """
        self.df = df
        self.temporary_view_name = temporary_view_name

    def __iter__(self):
        return iter((self.df, self.temporary_view_name))


ContextDictPrimitives = Dict[str, Any]
ContextDictDataFrames = Dict[str, DataFrameInfo]
ContextDictSerializedDataFrames = Dict[str, List[Optional[str]]]


class PipelineContext(MemoryOperationsMixin):
    """
    Stores the context of a Pipeline.

    Attributes:
        dataframes:  A bag of named dataframes, optionally annotated with a temporary view name.
        primitives:  A bag of primitives primitive objects.
                     Anything that serializes to JSON nicely should do (int, str, etc.).
        working_folder:  A directory to which dataframes would be written to / read from when required.

    Examples:
        Create a Pipeline Context:
        >>> df = ...   # type: DataFrame
        >>> context = PipelineContext('/tmp', {
        ...     'a_number': 1,
        ...     'a_string': 'abc'
        ... }, {
        ...     'df': DataFrameInfo(df),
        ...     'df_with_temporary_view': DataFrameInfo(df, 'temp_view'),
        ... }
        ... )

        Serialize a context without dataframes:
        >>> context.serialize(False)
        {'working_folder': '/tmp', 'primitives': {'a_number': 1, 'a_string': 'abc'}}

        Serialize a context with dataframes:
        >>> context.serialize(True)
        {'working_folder': '/tmp', 'primitives': {'a_number': 1, 'a_string': 'abc'},
        'dataframes': {'df': ['/tmp/partial/df', None],
        'df_with_temporary_table': ['/tmp/partial/df_with_temporary_view', 'temp_view'}}

        Deserialize a context:
        >>> spark = ...   # type: SparkSession
        >>> context_serialized = ...  # type: Dict
        >>> PipelineContext.deserialize(spark, context_serialized)
        <PipelineContext object at 0x7f886a540828>
    """

    dataframes = None  # type: ContextDictDataFrames
    primitives = None  # type: ContextDictPrimitives
    working_folder = None  # type: str

    def __init__(
            self,
            working_folder: str,
            primitives: Optional[ContextDictPrimitives] = None,
            dataframes: Optional[ContextDictDataFrames] = None,
            persistent_rdds: Optional[List[RDD]] = None,
    ):
        super().__init__(persistent_rdds)
        self.working_folder = working_folder
        self.primitives = primitives or {}
        self.dataframes = dataframes or {}

    @property
    def scoped_working_folder(self) -> str:
        """
        Helper method to retrieve the working directory when a result scope is specified

        Returns:
            str: The working folder with result scope taken into consideration
        """
        return os.path.join(self.working_folder, self.primitives['result_scope']) \
            if 'result_scope' in self.primitives else self.working_folder

    def to_metadata(self) -> Metadata:
        """
        Convert context to an equivalent Metadata instance for compatibility with Pipeline 1.0

        Returns:
            Metadata: Equivalent metadata instance for this context
        """
        metadata_raw = self.primitives.copy()
        metadata_raw['working_folder'] = self.working_folder
        return Metadata(metadata_raw)

    def serialize(self, with_dataframes: bool = True) -> Dict[str, Any]:
        """
        Convert context to serializable form.

        Args:
            with_dataframes (bool): Whether to serialize dataframes as part of this conversion

        Returns:
            Dict[str, Any]: Serializable form of the context
        """
        retval = {
            'working_folder': self.working_folder,
            'primitives': self.primitives,
        }
        if with_dataframes:
            retval['dataframes'] = self._write_dataframes()

        return retval

    def clone(self):
        """
        Create a clone of this context

        Returns:
            PipelineContext: A new context instance with the same state as this
        """
        # this is to avoid inadvertent updates of the PipelineContext contents
        # as PipelineContext uses dictionaries which are mutable
        return PipelineContext(self.working_folder,
                               deepcopy(self.primitives),
                               self.dataframes,
                               self.persistent_rdds)

    def _write_dataframe(
            self,
            serialized_dataframes: ContextDictSerializedDataFrames,
            name: str,
            df: DataFrame,
            temporary_view_name: Optional[str]
    ):
        """
        Write a single dataframe to the filesystem and store its path to a given dictionary

        Args:
            serialized_dataframes (ContextDictSerializedDataFrames): Dictionary storing serialized DataFrame info
            name (str): The name of this DataFrame
            df (DataFrame): The DataFrame to be serialized
            temporary_view_name (Optional[str]): The name of the temporary view this DataFrame is registered to, if
                applicable
        """
        target_path = os.path.join(self.working_folder, 'suspended', name)
        df.write.mode('overwrite').parquet(target_path)
        serialized_dataframes[name] = [target_path, temporary_view_name]

        submission_id = self.primitives.get('submission_id', '')
        Submissions.add_dataset_id_tag(target_path, submission_id, True)

    def _write_dataframes(self) -> ContextDictSerializedDataFrames:
        """
        Write the DataFrames registered to this instance to the filesystem

        Returns:
            ContextDictSerializedDataFrames: Dictionary storing the location of the written DataFrames
        """
        serialized_dataframes = {}  # type: ContextDictSerializedDataFrames

        concurrent_tasks(((name, self._write_dataframe, (serialized_dataframes, name, df, temporary_view_name))
                          for name, (df, temporary_view_name) in self.dataframes.items()))

        return serialized_dataframes

    @classmethod
    def from_submission(cls, submission: Submission) -> 'PipelineContext':
        """
        Produce a context from a Submission

        Args:
            submission (Submission): The submission from which to produce a context

        Returns:
            PipelineContext: A corresponding context
        """
        submission_metadata = submission.metadata
        return PipelineContext(submission_metadata.working_folder,
                               primitives={**{key: value for key, value in submission_metadata.to_primitive().items()
                                              if key != 'working_folder'},
                                           'submission_test_scope': submission.test_scope,
                                           'workspace': submission.workspace})

    @classmethod
    def from_metadata(cls, metadata: Metadata) -> 'PipelineContext':
        """
        Produce a context from a Pipeline 1.0 metadata instance

        Args:
            metadata (Metadata): Pipeline 1.0 metadata to adapt

        Returns:
            PipelineContext: An equivalent context instance
        """
        working_folder = metadata['working_folder']
        return PipelineContext(
            working_folder, primitives={key: value for key, value in metadata.items() if key != 'working_folder'}
        )

    @classmethod
    def deserialize(cls, spark: SparkSession, serialized: Dict[str, Any]) -> 'PipelineContext':
        """
        Restore a context from a serialized form

        Args:
            spark (SparkSession): The active Spark session
            serialized (Dict[str, Any]): The serialized form of a context

        Returns:
            PipelineContext: The deserialized context
        """
        dataframes = serialized.get('dataframes', {})
        if dataframes:
            dataframes = cls._read_dataframes(spark, dataframes)
        return cls(
            serialized['working_folder'],
            serialized['primitives'],
            dataframes
        )

    @staticmethod
    def _read_dataframe(
            spark: SparkSession,
            deserialized_dataframes: ContextDictDataFrames,
            name: str,
            target_path: str,
            temporary_view_name: str = None
    ):
        """
        Restore a previously serialized DataFrame from the filesystem

        Args:
            spark (SparkSession): The active Spark session
            deserialized_dataframes (ContextDictDataFrames): Dictionary of deserialized DataFrames to write to
            name (str): The name of the DataFrame being deserialized
            target_path (str): The path of the parquet file of the DataFrame
            temporary_view_name (Optional[str]): The name of the temporary view this DataFrame was registered as, if
                applicable
        """
        df = spark.read.parquet(target_path)
        if temporary_view_name is not None:
            df.createOrReplaceTempView(temporary_view_name)
        deserialized_dataframes[name] = DataFrameInfo(df, temporary_view_name)

    @staticmethod
    def _read_dataframes(
            spark: SparkSession,
            dataframes_serialized: ContextDictSerializedDataFrames
    ) -> ContextDictDataFrames:
        """
        Restore a collection of previously serialized DataFrames from the filesystem

        Args:
            spark (SparkSession): The active Spark session
            dataframes_serialized (ContextDictSerializedDataFrames): Descriptor of serialized DataFrames

        Returns:
            ContextDictDataFrames: Dictionary of deserialized DataFrames
        """
        deserialized_dataframes = {}  # type: ContextDictDataFrames

        concurrent_tasks(((name, PipelineContext._read_dataframe,
                           (spark, deserialized_dataframes, name, target_path, temporary_view_name))
                          for name, (target_path, temporary_view_name) in dataframes_serialized.items()))

        return deserialized_dataframes


class PipelineStageSummary(Model):
    """
    A model summarizing the outcome of a pipeline stage, without retaining the value
    """
    stage = StringType()
    status = StringType(choices=enumlike_values(PipelineStatus))
    terminal = BooleanType(default=False)
    start_time = DateTimeType()
    end_time = DateTimeType()


class PipelineState(Model):
    """
    A model describing the overall result of a pipeline execution
    """

    id = IntType()
    context = DictType(AnyType)  # type: Dict[str, Any]
    status = StringType(choices=enumlike_values(PipelineStatus))
    stages = ListType(ModelType(PipelineStageSummary))  # type: List[PipelineStageSummary]
    start_time = DateTimeType()
    end_time = DateTimeType()
    validation_summary_extract_id = IntType(required=False)
    deid_extract_id = IntType(required=False)
    extract_ids = DictType(IntType(), required=False)

    @property
    def duration(self) -> timedelta:
        """
        Returns:
            timedelta: The duration of this pipeline stage
        """
        return self.end_time - self.start_time

    def successful(self) -> bool:
        """
        Returns:
            bool: Whether the status of this stage is PipelineStatus.Success
        """
        return self.status == PipelineStatus.Success

    #  pylint: disable=E1136
    def to_legacy_result(self, spark: SparkSession,
                         output_loader: Callable[[SparkSession, str], DataFrame]) -> PipelineResult:
        """
        Adapt this instance to the equivalent Pipeline 1.0 result

        Returns:
            PipelineResult: The Pipeline 1.0 equivalent to this result
        """

        if self.status == PipelineStatus.Failed:
            return PipelineResult({
                'status': self.status,
                'total_records': 0,
                'dq_pass_count': 0,
                'dq_error_count': 0,
                'dq_warning_count': 0,
                'validation_summary_extract_id': self.validation_summary_extract_id
            })

        if self.status not in {PipelineStatus.Rejected, PipelineStatus.Success, PipelineStatus.Suspended}:
            raise NotImplementedError(
                "Cannot adapt Pipeline 2.0 state of status {} to Pipeline 1.0 result".format(self.status))

        primitives = self.context['primitives']
        working_folder = self.context['working_folder']
        if 'result_scope' in primitives:
            working_folder = os.path.join(working_folder, primitives['result_scope'])

        total_records = primitives.get('total_submitted_records', 0)

        def count_records(file: str, error_type: str) -> int:
            try:
                data_frame = output_loader(spark, file)
                dataset_id = primitives.get('dataset_id')
                if dataset_without_table_structure(dataset_id):
                    transformed_data_frame = \
                        data_frame.where(col(DQFields.ROW_INDEX).isNotNull()).filter(
                            data_frame[DQFields.TYPE] == error_type).drop_duplicates([DQFields.ROW_INDEX])
                else:
                    transformed_data_frame = data_frame.where(col(DQFields.TABLE).isNotNull()).where(
                        col(DQFields.ROW_INDEX).isNotNull()).filter(
                        data_frame[DQFields.TYPE] == error_type).drop_duplicates(
                        [DQFields.TABLE, DQFields.ROW_INDEX])

                return transformed_data_frame.count()
            except AnalysisException:
                return 0

        error_record_count = count_records(os.path.join(working_folder, PATHS.DQ), DQMessageType.Error)
        warning_record_count = count_records(os.path.join(working_folder, PATHS.DQ), DQMessageType.Warning)

        def submission_rejected() -> bool:
            try:
                dq_df = output_loader(spark, os.path.join(working_folder, PATHS.DQ))
                return dq_df.where(col(DQFields.ACTION) == 'reject_file').count() > 0
            except AnalysisException:
                return False

        dq_pass_count = 0 if submission_rejected() else max(total_records - error_record_count, 0)

        return PipelineResult({
            'status': self.status,
            'total_records': total_records,
            'dq_pass_count': dq_pass_count,
            'dq_error_count': error_record_count,
            'dq_warning_count': warning_record_count,
            'validation_summary_extract_id': self.validation_summary_extract_id,
            'deid_extract_id': self.deid_extract_id,
            'extract_ids': self.extract_ids,
            'queued_submission_id': primitives.get("queued_submission_id", 0),
        })
