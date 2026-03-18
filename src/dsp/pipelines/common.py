import os
from datetime import datetime
from typing import Any, Dict, List, Set, Union, AbstractSet, Mapping, Callable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, explode, trim, col, struct, concat, when
from pyspark.sql.types import StructType, StructField, TimestampType

from dsp.common.relational import Table
from dsp.dam import current_dataset_version, current_record_version
from dsp.datasets.common import DQMessageType, Fields as Common, Fields
from dsp.dq_files.dq.schema import DQ_MESSAGE_SCHEMA
from dsp.datasets.definitions.msds.submission_constants import Metadata
from dsp.dq_files.output import Fields as DQFields
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage, PipelineRejection
from dsp.validations.validation_rules import ValidationDetail
from dsp.shared.common import parse_timestamp
from dsp.shared.constants import PATHS, ClusterPools


def write_dq_df(dq_df: DataFrame, context: PipelineContext) -> None:
    """
    Write the dq Dataframe to the work folder

    Args:
        dq_df (DataFrame): The dq dataframe
        context (PipelineContext): Context for the current pipeline execution
    """
    output_folder = context.scoped_working_folder
    dq_results_path = os.path.join(output_folder, PATHS.DQ)
    dq_df.write.mode('append').parquet(dq_results_path)


def get_validation_codes(df: DataFrame) -> Set[str]:
    """
    Retrieve the validation codes reported from a given dataframe

    Args:
        df (DataFrame): The validated dataframe with reported validation code

    Returns:
        Set[str]: The set of unique validation codes reported in the dataframe
    """
    return {row[Common.DQ] for row in df.select(explode(Common.DQ).alias(Common.DQ)).distinct().collect()}


def map_java_type(java_val: object) -> Callable[[Any], Any]:
    java_type = type(java_val).__name__

    if java_type == 'java.lang.Integer':
        return java_val.intValue()

    if java_type == 'java.lang.Long':
        return java_val.longValue()

    if java_type == 'java.lang.Float':
        return java_val.floatValue()

    if java_type == 'java.lang.Double':
        return java_val.doubleValue()

    return java_val


class ValidateMandatoryTablesNotEmpty(PipelineStage):
    """
    Pipeline stage to ensure that a subset of dataframes contain records, rejecting the submission if any required
    dataframes are found to be empty
    """

    name = "validate_mandatory_tables_not_empty"

    def __init__(self, codes_by_table: Dict[Table, str], dataset_tables: List[Table],
                 get_validation_detail: Callable[[str], ValidationDetail],
                 passthrough_dataframes: AbstractSet[str] = frozenset()):
        """
        Args:
            codes_by_table: Codes of validation errors to be raised keyed by table to check for records
            dataset_tables: Complete set of tables for the dataset being processed
            get_validation_detail: Function to retrieve validation detail object given a validation code
            passthrough_dataframes: Additional dataframes that will be flowed unchanged by this stage
        """
        super().__init__(name=ValidateMandatoryTablesNotEmpty.name,
                         required_input_dataframes={*(table.name for table in dataset_tables), *passthrough_dataframes},
                         provided_output_dataframes={*(table.name for table in dataset_tables),
                                                     *passthrough_dataframes})
        self._codes_by_table = codes_by_table
        self._get_validation_detail = get_validation_detail

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        """
        Simple checks before the bulk of validations is executed.

        Args:
            spark (SparkSession): The current Spark session, with applicable dataframes registered as temp views
            context (PipelineContext): Context for the current pipeline execution

        Raises:
            PiplineRejection if check fails
        """
        dq_df = spark.createDataFrame([], StructType([
            *DQ_MESSAGE_SCHEMA,
            StructField(DQFields.DQ_TS, TimestampType())
        ]))

        dq_df = self._check_for_empty_tables(spark, dq_df)
        if dq_df.select(lit(1)).head(1):
            write_dq_df(dq_df, context)
            raise PipelineRejection

        return context

    def _check_for_empty_tables(self, spark: SparkSession, dq_df: DataFrame) -> DataFrame:
        """
        Check whether some tables are empty.

        Args:
            spark (SparkSession): The current Spark session
            dq_df (DataFrame): The dq dataframe to be enriched

        Returns:
            DataFrame: An updated copy of dq_df.
        """
        for table, error_code in self._codes_by_table.items():
            if not spark.table(table.name).dropna('all').head(1):
                rejection_detail = self._get_validation_detail(error_code)
                rejection_df = spark.createDataFrame([
                    (table.name, None, None, [], error_code, rejection_detail.message, {}, DQMessageType.Error,
                     'reject_file',
                     datetime.utcnow())
                ], StructType([
                    *DQ_MESSAGE_SCHEMA,
                    StructField(DQFields.DQ_TS, TimestampType())
                ]))
                dq_df = dq_df.union(rejection_df)
        return dq_df


class CleanseStage(PipelineStage):
    """
    Stage to clean the submitted field values
    """
    name = "cleanse_stage"

    def __init__(self, tables: List[Table], passthrough_dataframes: AbstractSet[str] = frozenset()):
        super().__init__(
            name=CleanseStage.name,
            required_input_dataframes={*(table.name for table in tables), *passthrough_dataframes},
            provided_output_dataframes={*(table.name for table in tables), *passthrough_dataframes}
        )
        self._cleansed_dataframes = {table.name for table in tables}
        self._passthrough_dataframes = passthrough_dataframes

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        def cleanse_dataframe(dataframe_info: DataFrameInfo) -> DataFrameInfo:
            table_df = dataframe_info.df
            table_df = CleanseStage.strip_whitespace(table_df)
            if dataframe_info.temporary_view_name:
                table_df.createOrReplaceTempView(dataframe_info.temporary_view_name)
            return DataFrameInfo(table_df, dataframe_info.temporary_view_name)

        context.dataframes = {
            **{passthrough_dataframe_name: context.dataframes[passthrough_dataframe_name]
               for passthrough_dataframe_name in self._passthrough_dataframes},
            **{cleansed_dataframe_name: cleanse_dataframe(context.dataframes[cleansed_dataframe_name])
               for cleansed_dataframe_name in self._cleansed_dataframes}
        }

        return context

    @staticmethod
    def strip_whitespace(df: DataFrame) -> DataFrame:
        """

        Args:
            df: dataframe

        Returns: dataframe after removing leading and trailing spaces of string column

        """
        col_names_and_types = df.dtypes
        for name, dtype in col_names_and_types:
            if dtype == 'string':
                df = df.withColumn(name, when(trim(col(name)) == '', None).otherwise(trim(col(name))))
        return df


class AddMetaColumnStage(PipelineStage):
    """
    Stage to add meta column to dataframe
    """
    name = "add_meta_column"

    def __init__(
            self, trace_dataframes: Union[AbstractSet[str], Mapping[str, str]] = None,
            passthrough_dataframes: AbstractSet[str] = None
    ):
        """
        Args:
            trace_dataframes (AbstractSet[str]): Dataframes to apply traceability columns to, default {'df'}
            passthrough_dataframes (AbstractSet[str]): Dataframes to passthrough unchanged
        """
        if trace_dataframes is None:
            self._trace_dataframes = {'df': 'df'}
        elif isinstance(trace_dataframes, AbstractSet):
            self._trace_dataframes = {dataframe_name: dataframe_name for dataframe_name in trace_dataframes}
        else:
            self._trace_dataframes = dict(trace_dataframes)
        self._passthrough_dataframes = frozenset(passthrough_dataframes) \
            if passthrough_dataframes is not None else frozenset()
        super().__init__(name=AddMetaColumnStage.name,
                         required_input_dataframes={*self._trace_dataframes.keys(), *self._passthrough_dataframes},
                         provided_output_dataframes={*self._trace_dataframes.values(), *self._passthrough_dataframes})

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:

        new_context = context.clone()
        new_context.dataframes = {
            **{
                output_df: DataFrameInfo(
                    self.add_meta_column(context.dataframes[input_df].df, context.to_metadata()))
                for input_df, output_df in self._trace_dataframes.items()},
            **{df: context.dataframes[df] for df in self._passthrough_dataframes}
        }

        return new_context

    @staticmethod
    def add_meta_column(raw_df: DataFrame, metadata: Metadata) -> DataFrame:
        """ ensure record traceability columns are present in the dataset
            e.g. add in our submission_id, and record_number
        Args:
            raw_df (DataFrame): the freshly loaded dataset, 'unmolested'
            metadata (dict): submission metadata

        Returns:
            DataFrame: dataframe with the extra required columns for traceability
        """
        submission_id = metadata['submission_id']
        submitted_timestamp = parse_timestamp(metadata['submitted_timestamp'])
        dataset_id = metadata['dataset_id']

        df = raw_df.withColumn(Common.META,
                               struct(lit(current_dataset_version(dataset_id)).alias(Fields.DATASET_VERSION),
                                      concat(lit('{}:'.format(submission_id)), raw_df.RowNumber).alias(Fields.EVENT_ID),
                                      lit(submitted_timestamp).alias(Fields.EVENT_RECEIVED_TS),
                                      raw_df.RowNumber.alias(Fields.RECORD_INDEX),
                                      lit(current_record_version(dataset_id)).alias(Fields.RECORD_VERSION)))

        return df


NP_KEY = "notebook_path"
BP_KEY = "base_parameters"
TN_KEY = "table_name"


class NotebookPayload:
    def __init__(
            self, notebook_path: str, run_name: str, cluster_pool: str = ClusterPools.SYSTEM, **kwargs
    ):
        self.run_name = run_name
        self.cluster_pool = cluster_pool
        self.notebook_task = {NP_KEY: notebook_path, BP_KEY: kwargs}

    def get_table_name(self):
        return self.notebook_task[BP_KEY][TN_KEY]
