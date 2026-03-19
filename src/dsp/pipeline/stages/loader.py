import os
from abc import abstractmethod
from builtins import zip
from typing import Any, Sequence, Tuple

from pyspark import Row
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType

from dsp.pipeline.models import DataFrameInfo, PipelineContext
from dsp.pipeline.pipeline import PipelineRejection, PipelineStage
from dsp.shared.constants import PATHS, EXTENSION
from dsp.shared.logger import log_action, add_fields
from dsp.shared.common.test_helpers import smart_open
from dsp.shared.aws import s3_gunzip

WRITE_BUFFER_SIZE = 1024 * 1024 * 15


class DataFrameLoaderPipelineStage(PipelineStage):
    """
    Base definition for pipeline stages which load a single dataframe
    """
    name = "loader"

    def __init__(self, output_dataframe_name: str = "df"):
        """
        Args:
            output_dataframe_name (str): Name to give to the dataframe output by this stage; default value "df"
        """
        super().__init__(name=DataFrameLoaderPipelineStage.name, provided_output_dataframes={output_dataframe_name})
        self._output_dataframe_name = output_dataframe_name

    @log_action()
    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        working_folder = context.working_folder

        input_file = os.path.join(working_folder, PATHS.RAW_DATA)
        data_frame = self._load_from_path(spark, input_file, context)

        context.dataframes[self._output_dataframe_name] = DataFrameInfo(data_frame)
        add_fields(rows_submitted=data_frame.count())
        return context

    @abstractmethod
    def _load_from_path(self, spark: SparkSession, input_file: str, context: PipelineContext) -> DataFrame:
        """
        Load a dataframe from a given input path

        Args:
            spark (SparkSession): The current Spark session
            input_file (str): Path to the raw submission
            context (PipelineContext): The context of the pipeline execution

        Returns:
            DataFrame: The loaded DataFrame
        """
        raise NotImplementedError


class GenericDataFrameLoaderPipelineStage(DataFrameLoaderPipelineStage):
    """
    Base definition for Generic pipeline stages which load a single dataframe
    """
    name = "generic_loader"

    def is_file_gzipped(self, path: str) -> bool:
        """
        Checks if a file is a gzipped file.
        First checks the file extension then if not a .gz extension then opens the file
        and checks for a unicode error and if the invalid input of the codec failed on starts with `b'\x8b'` which is the
        the start byte of a gzip encoded file.

        Args:
            path (str): The path to the file to check.
        Return:
            bool: Whether the file is gzipped.
        """
        if path.endswith(EXTENSION.Gzip):
            return True
        try:
            with smart_open(path, 'r') as _file:
                for _ in _file:
                    pass
            return False
        except UnicodeDecodeError as unicode_exception:
            if unicode_exception.object[unicode_exception.start:unicode_exception.end].startswith(b'\x8b'):
                return True
            else:
                raise

    def __init__(self, output_dataframe_name: str = "df"):
        """
        Args:
            output_dataframe_name (str): Name to give to the dataframe output by this stage; default value "df"
        """
        super().__init__(output_dataframe_name=output_dataframe_name)

    @log_action()
    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        working_folder = context.working_folder

        input_file = os.path.join(working_folder, PATHS.RAW_DATA)

        if self.is_file_gzipped(input_file):
            new_path = f"{input_file}.raw"
            s3_gunzip(input_file, new_path, buffer_size=WRITE_BUFFER_SIZE)
            input_file = new_path

        data_frame = self._load_from_path(spark, input_file, context)

        context.dataframes[self._output_dataframe_name] = DataFrameInfo(data_frame)
        add_fields(rows_submitted=data_frame.count())
        return context

    @abstractmethod
    def _load_from_path(self, spark: SparkSession, input_file: str, context: PipelineContext) -> DataFrame:
        """
        Load a dataframe from a given input path

        Args:
            spark (SparkSession): The current Spark session
            input_file (str): Path to the raw submission
            context (PipelineContext): The context of the pipeline execution

        Returns:
            DataFrame: The loaded DataFrame
        """
        raise NotImplementedError


class CSVLoaderPipelineStage(GenericDataFrameLoaderPipelineStage):
    """
    Loads rows from the CSV file and annotate with the RowNumber column.
    expected_cols: optional
    """
    name = "csv_loader"

    _expected_cols = None  # type: Sequence[str]
    _has_header = True  # type: bool
    _row_number = True  # type: bool
    _one_indexed = False  # type: bool

    def __init__(self, output_dataframe_name: str, expected_cols: Sequence[str] = None, has_header: bool = True,
                 add_row_number: bool = True, one_indexed: bool = False):
        self._expected_cols = expected_cols
        self._has_header = has_header
        self._row_number = add_row_number
        self._one_indexed = one_indexed
        super().__init__(output_dataframe_name=output_dataframe_name)

    def _add_row_number(self, df: DataFrame):
        def mapper(row_and_index: Tuple[Row, int]):
            row, index = row_and_index
            index = index + 1 if self._one_indexed else index
            return Row(index, *row)

        df_with_row_number_schema = StructType([StructField("RowNumber", IntegerType()), *df.schema])
        df_with_row_number = df.rdd.zipWithIndex().map(mapper).toDF(df_with_row_number_schema)

        return df_with_row_number

    @log_action()
    def _load_df_from_path(self, spark: SparkSession, input_file: str) -> DataFrame:
        return spark.read.csv(input_file, header=self._has_header)

    @log_action()
    def _load_from_path(self, spark: SparkSession, input_file: str, context: PipelineContext) -> DataFrame:
        df = self._load_df_from_path(spark, input_file)
        if not self._has_header and self._expected_cols is not None:
            for old_name, new_name in zip(df.columns, self._expected_cols):
                df = df.withColumnRenamed(old_name, new_name)
        submitted_cols = df.schema.names

        add_fields(
            fields_submitted=submitted_cols,
            fields_expected=self._expected_cols
        )

        if self._expected_cols:
            if len(submitted_cols) != len(self._expected_cols):
                raise PipelineRejection(
                    message="There was a problem loading the csv file. "
                    "Number of columns in the csv don't match the expected number"
                )

        if self._has_header and self._expected_cols and (sorted(submitted_cols) != sorted(self._expected_cols)):
            not_submitted = set(self._expected_cols).difference(set(submitted_cols))
            superflux = set(submitted_cols).difference(set(self._expected_cols))
            add_fields(
                fields_not_submitted=not_submitted,
                fields_superflux=superflux
            )
            raise PipelineRejection(
                message="There was a problem loading the csv file. "
                "The column names are not a match. "
            )

        if not self._row_number:
            return df

        df_with_row_number = self._add_row_number(df)
        return df_with_row_number
