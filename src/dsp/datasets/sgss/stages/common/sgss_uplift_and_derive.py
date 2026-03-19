from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, expr

from dsp.datasets.sgss.ingestion.sgss.config import Fields
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage
from dsp.udfs import LatLongFromPostcode, PCDSFromPostcode


class SGSSUpliftAndDeriveStage(PipelineStage):
    """
    Applies a series of transformations to the input SGSS DataFrame
    """
    name = "sgss_uplift_and_derive"

    def __init__(self, input_dataframe_name: str, output_dataframe_name: str):
        super().__init__(
            name=self.name,
            required_input_dataframes={input_dataframe_name},
            provided_output_dataframes={output_dataframe_name},
        )
        self._input_dataframe_name = input_dataframe_name
        self._output_dataframe_name = output_dataframe_name

    @staticmethod
    def _uplift_and_derive(df: DataFrame) -> DataFrame:
        df_derived = (
            df
                .withColumn(Fields.Specimen_Date, to_date(Fields.Specimen_Date, 'dd/MM/yyyy'))
                .withColumn(Fields.Lab_Report_Date, to_date(Fields.Lab_Report_Date, 'dd/MM/yyyy'))
                .withColumn(Fields.Patient_Date_Of_Birth, to_date(Fields.Patient_Date_Of_Birth, 'dd/MM/yyyy'))
                .withColumn(Fields.Age_in_Years, col(Fields.Age_in_Years).cast('smallint'))
                .withColumn(Fields.pcds, PCDSFromPostcode(col(Fields.Patient_PostCode), col(Fields.Lab_Report_Date)))
                .withColumn(Fields.pcds_sector,
                            expr(
                                f"concat(split({Fields.pcds}, ' ')[0], "
                                f"' ', substring(split({Fields.pcds}, ' ')[1], 1, 1))"))
                .withColumn(Fields.latlong, LatLongFromPostcode(col(Fields.Patient_PostCode),
                                                                col(Fields.Lab_Report_Date)))
                .withColumn(Fields.latitude, expr(f'{Fields.latlong}.Latitude'))
                .withColumn(Fields.longitude, expr(f'{Fields.latlong}.Longitude'))
                .drop(Fields.latlong)
        )

        return df_derived

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        df = context.dataframes[self._input_dataframe_name].df

        df = self._uplift_and_derive(df)

        context.dataframes[self._output_dataframe_name] = DataFrameInfo(df)

        return context
