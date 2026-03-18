from datetime import datetime
from typing import AbstractSet, Any, Callable

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType

from dsp.datasets.common import DQMessageType
from dsp.dq_files.dq.schema import DQ_MESSAGE_WITH_TIMESTAMP_SCHEMA
from dsp.dq_files.output import Fields as DQFields
from dsp.pipelines.common import write_dq_df

from dsp.pipeline.models import PipelineContext
from dsp.pipeline.pipeline import PipelineRejection, PipelineStage
from dsp.shared.logger import log_action, add_fields


class TotalRecordsStage(PipelineStage):
    """
    Total all records in the given dataframes and assign the value to the context primitives
    """
    name = "total_records"

    def __init__(
        self,
        key_name: str,
        dataframe_names: AbstractSet[str],
        passthrough_dataframes: AbstractSet[str] = frozenset(),
    ):
        """
        Args:
            key_name: Name of the primitive key to assign the total
            dataframe_names: Name of the dataframes in the context to total
            passthrough_dataframes: Name of dataframes expected to be present in the context but not to sum
        """
        super().__init__(name="total_records", required_input_dataframes={*dataframe_names, *passthrough_dataframes},
                         provided_output_dataframes={*dataframe_names, *passthrough_dataframes})
        self._key_name = key_name
        self._dataframe_names = dataframe_names

    @log_action()
    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        add_fields(context_primitives=context.primitives)

        total_records = 0

        rejections_df = None
        for dataframe_name in self._dataframe_names:
            try:
                dataframe_info = context.dataframes[dataframe_name]
                total_records += dataframe_info.df.count()
            except Py4JJavaError as e:
                rejections_df = self._handle_py4j_java_error(spark, e, dataframe_name, rejections_df)

        if rejections_df:
            write_dq_df(rejections_df, context)
            raise PipelineRejection(message=f'Failed to load dataframe from submitted database.')

        context.primitives[self._key_name] = total_records

        add_fields(total_records=total_records)

        return context

    @log_action()
    def _handle_py4j_java_error(
        self,
        spark: SparkSession,
        error: Py4JJavaError,
        df_name: str,
        rejections_df: DataFrame=None
    ) -> DataFrame:

        schema = StructType([*DQ_MESSAGE_WITH_TIMESTAMP_SCHEMA])
        error_code = 'MHSREJ201'
        message = 'Failed Data Load. ' \
            f'The submitted file could not be loaded into a processing context. Invalid data in table {df_name}.'

        if not rejections_df:
            rejections_df = spark.createDataFrame([], schema)

        rejection_df = spark.createDataFrame(
            [(
                df_name,
                None,
                None,
                [],
                error_code,
                message.format(table_name=df_name),
                {},
                DQMessageType.Error,
                'reject_file',
                datetime.utcnow()
            )],
            schema
        )

        return rejections_df.union(rejection_df)
