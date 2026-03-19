from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from dsp.datasets.sgss.ingestion.sgss.config import TEST_TYPE_TO_MAPPING_EXPLODED, Fields
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage


class SGSSMapResultToDescriptionStage(PipelineStage):
    """
    Maps a given SGSS record to its corresponding Snomed CT code, test kit and specimen type based on the provided
    Organism_Species_Name and Test_Type
    """
    name = "sgss_map_result_to_description"

    def __init__(self, input_dataframe_name: str, output_dataframe_name: str):
        super().__init__(
            name=self.name,
            required_input_dataframes={input_dataframe_name},
            provided_output_dataframes={output_dataframe_name},
        )
        self._input_dataframe_name = input_dataframe_name
        self._output_dataframe_name = output_dataframe_name

    @staticmethod
    def _map_result_to_description(spark: SparkSession, df: DataFrame) -> DataFrame:
        test_type_to_desc_schema = StructType([
            StructField('_test_type', StringType()),
            StructField('_organism_key', StringType()),
            StructField('test_result_sct', StringType()),
            StructField('test_result', StringType()),
            StructField('method_of_detection', StringType()),
            StructField('test_kit', StringType()),
            StructField('specimen_type', StringType()),
        ])

        test_descriptions = [
            (test_desc['test_type'], test_desc['organism_key'], test_desc['test_result_sct'],
             test_desc['test_result'], test_desc['method_of_detection'], test_desc['test_kit'],
             test_desc['specimen_type'])
            for test_desc in TEST_TYPE_TO_MAPPING_EXPLODED]

        test_type_map_df = spark.createDataFrame(test_descriptions, test_type_to_desc_schema)
        df_derived = df.join(test_type_map_df, on=((df[Fields.test_type] == test_type_map_df['_test_type']) &
                                                   (df[Fields.Organism_Species_Name] == test_type_map_df[
                                                       '_organism_key'])), how='left').drop('_organism_key',
                                                                                            '_test_type')
        return df_derived

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        df = context.dataframes[self._input_dataframe_name].df

        df = self._map_result_to_description(spark, df)

        context.dataframes[self._output_dataframe_name] = DataFrameInfo(df)

        return context
