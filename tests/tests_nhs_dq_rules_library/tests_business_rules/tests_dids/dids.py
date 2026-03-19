from typing import Any
import os
from dsp.datasets.enrichments.dids import enrich_with_pds_mps
from dsp.datasets.pipelines.mps_check import MPSCheckStage
from dsp.pipeline.stages.collect_mps_response import CollectMPSResponseStage
from dsp.pipeline.stages.mps_enrich import MPSEnrichStage
from dsp.pipeline.stages.pds_cross_check import PDSCrossCheckStage
from dsp.shared.constants import PATHS, DS
from dsp.dam.dq_errors import DQErrs
from pyspark.sql import SparkSession, DataFrame
from dsp.datasets.dids import POST_VALIDATION_TYPE_CORRECTIONS
from dsp.datasets.loaders import dids_csv, LoaderRejectionError, dids_xml
from dsp.datasets.loaders.helpers import write_invalid_schema_dq
from dsp.datasets.validations.common import xml_schema_is_valid
from dsp.dq.coerce_types import coerce_data_types
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineRejection, PipelineStage
from dsp.pipeline.stages import *
from dsp.pipeline.stages.total_records import TotalRecordsStage
from dsp.shared.constants import FT


OUTPUT_DATAFRAME_NAME = "output_df"


class LoadDIDSCSVStage(DataFrameLoaderPipelineStage):
    """
    Pipeline stage which loads CSV-formatted DIDS submissions to a single DataFrame
    """
    name = 'load_dids_csv'
    def __init__(self, output_dataframe_name: str = "df"):
        super().__init__(output_dataframe_name=output_dataframe_name)

    def _load_from_path(self, spark: SparkSession, input_file: str, context: PipelineContext) -> DataFrame:

        try:
            data_frame, _ = dids_csv.csv_loader(spark, input_file, context.to_metadata())
        except LoaderRejectionError:
            code = 'DIDS075'
            rejection_detail = DQErrs.DIDS075.label
            write_invalid_schema_dq(spark, context, {"error": code}, code, rejection_detail)
            raise PipelineRejection
        else:
            return data_frame


class LoadDIDSXMLStage(DataFrameLoaderPipelineStage):
    """
        Pipeline stage which loads XML-formatted DIDS submissions to a single DataFrame
        """
    name = 'load_dids_xml'
    def __init__(self, output_dataframe_name: str = "df"):
        super().__init__(output_dataframe_name=output_dataframe_name)

    def _load_from_path(self, spark: SparkSession, input_file: str, context: PipelineContext) -> DataFrame:
        code = 'DIDS076'
        rejection_detail = DQErrs.DIDS076.label
        submission_path = os.path.join(context.working_folder, PATHS.RAW_DATA)
        is_valid, errors = xml_schema_is_valid(submission_path, DS.DIDS)

        if not is_valid:
            write_invalid_schema_dq(spark, context, {"error": errors}, code, rejection_detail)
            raise PipelineRejection
        else:
            data_frame, _ = dids_xml.loader(spark, input_file, context.to_metadata())
            return data_frame

class DIDSPostValidationCorrectionStage(PipelineStage):
    name = "dids_post_validation_correction_stage"

    def __init__(self, input_dataframe_name: str = "df", output_dataframe_name: str = "df"):
        """
        Args:
            input_dataframe_name (str): Name of the dataframe input to this stage to validate; default value "df"
            output_dataframe_name (str): Name to give to the dataframe output by this stage; default value "df"
        """
        super().__init__(name=DIDSPostValidationCorrectionStage.name, required_input_dataframes={input_dataframe_name},
                         provided_output_dataframes={output_dataframe_name})
        self._input_dataframe_name = input_dataframe_name
        self._output_dataframe_name = output_dataframe_name

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        data_frame = context.dataframes[self._input_dataframe_name].df
        typed_df = coerce_data_types(data_frame, POST_VALIDATION_TYPE_CORRECTIONS)
        new_context = context.clone()
        new_context.dataframes = {self._output_dataframe_name: DataFrameInfo(typed_df)}
        return new_context


STAGES = [
    SubmissionTypeSelectingStage({
        FT.CSV: LoadDIDSCSVStage(),
        FT.XML: LoadDIDSXMLStage(),
    }),
    TotalRecordsStage('total_submitted_records', {'df'}),
    AddTraceabilityColumnsStage(),
    CanonicaliseFieldNamesStage(),
    CleanseDataFrameStage(),
    LegacyValidationsStage(),
    MetadataEnrichStage(),
    DIDSPostValidationCorrectionStage(),
    DeriveStage(),
    PDSCrossCheckStage(),
    MPSCheckStage(),
    CollectMPSResponseStage(passthrough_dataframe_names={'df'}),
    MPSEnrichStage(enrichments=[enrich_with_pds_mps]),
    ProjectStage(),
]  # type: List[PipelineStage]
