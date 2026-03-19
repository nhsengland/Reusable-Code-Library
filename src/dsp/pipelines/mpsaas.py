import csv
import datetime
import json
import os
from collections import namedtuple
from glob import glob
from typing import Any, AbstractSet, List, Dict

from pyspark.sql import SparkSession, DataFrame

from dsp.enrichments.mpsaas import enrich_mpsaas_with_person_id
from dsp.datasets.mps.request_header import Fields as MpsRequestHeaderFields
from dsp.datasets.mps.response_header import Fields as MPSResponseHeaderFields
from dsp.datasets.mps.response_header import Fields as MpsResponseHeaderFields
from dsp.datasets.mps.response_header import Fields as ResponseHeaderFields
#from dsp.loaders import mpsaas_csv, LoaderRejectionError
from dsp.pipelines.mps_check import MPSCheckStage
from dsp.integration.mps.constants import SpineResponseCodes
from dsp.integration.mps.mps_schema import generate_mps_response_header_fields
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage, PipelineRejection, PipelineRejectionHandler
from dsp.pipeline.stages.cleanse import CleanseDataFrameStage
from dsp.pipeline.stages.collect_mps_response import CollectMPSResponseStage, HeaderErrorHandler
from dsp.pipeline.stages.loader import DataFrameLoaderPipelineStage
from dsp.pipeline.stages.pds_cross_check import PDSCrossCheckStage
from dsp.pipeline.stages.total_records import TotalRecordsStage
from dsp.shared.aws import s3_split_path, s3_bucket
from dsp.shared.common.test_helpers import smart_open
from dsp.shared.constants import FT, MESH_WORKFLOW_ID, PATHS
from dsp.shared.logger import log_action, add_fields
from dsp.shared.models import MeshMetadata, MeshTransferStatus
from dsp.shared.store.mesh_upload_jobs import MeshUploadJobs

OutputFileConfig = namedtuple('OutputFileConfig', ['fields', 'file_path'])
OutputFileInfo = namedtuple('OutputFileInfo', ['header_file_path', 'body_file_path'])


@log_action()
def generate_output_files(
        spark: SparkSession,
        df: DataFrame,
        storage_path: str,
        config: OutputFileConfig
) -> OutputFileInfo:
    fields = config.fields
    fields[ResponseHeaderFields.NO_OF_DATA_RECORDS] = df.count()

    body_file_path = os.path.join(storage_path, config.file_path)
    df = df.repartition(1)  # so that only one CSV file is created
    df.write.csv(body_file_path)

    header_file_path = os.path.join(storage_path, '{}.header'.format(config.file_path))
    df = spark.createDataFrame([[str(v) for v in fields.values()]])
    df = df.repartition(1)  # so that only one CSV file is created
    df.write.csv(header_file_path)

    return OutputFileInfo(
        header_file_path=header_file_path,
        body_file_path=body_file_path
    )


def common_log_fields(context: PipelineContext) -> dict:
    return dict(
        submission_id=str(context.primitives['submission_id']),
        mps_request_ref=context.primitives['mps_request_reference']
    )


class LoadMPSaaSCsvStage(DataFrameLoaderPipelineStage):
    """
    Stage that loads a CSV-formatted MPS as a Service submission files to a dataframe
    """
    name = "load_mpsaas"

    def __init__(self, output_dataframe_name: str = "df"):
        super().__init__(output_dataframe_name=output_dataframe_name)

    def _load_from_path(self, spark: SparkSession, input_file: str, context: PipelineContext) -> DataFrame:
        try:
            data_frame, header = mpsaas_csv.csv_loader(spark, input_file, context)
            context.primitives['mps_request_reference'] = header[MpsRequestHeaderFields.REQUEST_REFERENCE]
        except LoaderRejectionError:
            raise PipelineRejection from LoaderRejectionError
        else:
            return data_frame


class MPSaaSEnrichStage(PipelineStage):
    """
    Pipeline Stage that enriches the MPS Response dataframe with a Person_ID column   
    """

    name = "mpsaas_enrich"

    def __init__(
            self, trace_dataframe_name: str = "df",
            passthrough_dataframes: AbstractSet[str] = None
    ):
        self._passthrough_dataframes = (
            frozenset(passthrough_dataframes) if passthrough_dataframes is not None else frozenset()
        )
        super().__init__(
            name=MPSaaSEnrichStage.name,
            required_input_dataframes={
                trace_dataframe_name, *self._passthrough_dataframes
            },
            provided_output_dataframes={
                trace_dataframe_name, *self._passthrough_dataframes
            }
        )
        self._trace_dataframe_name = trace_dataframe_name

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        df_records = context.dataframes[self._trace_dataframe_name].df
        submission_id = int(context.primitives['submission_id'])

        df_traced = enrich_mpsaas_with_person_id(submission_id, df_records)

        new_context = context.clone()
        new_context.dataframes = dict(
            **{self._trace_dataframe_name: DataFrameInfo(df_traced)},
            **{df: context.dataframes[df] for df in self._passthrough_dataframes}
        )
        return new_context


class OutputMPSaaSCsvStage(PipelineStage):
    """
    Terminal Stage that writes the MPSaaS output to CSV files
    """

    name = "output_mpsaas"

    def __init__(self, trace_dataframe_name: str = "df", passthrough_dataframes: AbstractSet[str] = None):
        self._passthrough_dataframes = (
            frozenset(passthrough_dataframes) if passthrough_dataframes is not None else frozenset()
        )
        super().__init__(
            name=OutputMPSaaSCsvStage.name,
            required_input_dataframes={
                trace_dataframe_name, *self._passthrough_dataframes
            },
            provided_output_dataframes={
                trace_dataframe_name, *self._passthrough_dataframes
            }
        )
        self._trace_dataframe_name = trace_dataframe_name

    @log_action()
    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        df_records = context.dataframes[self._trace_dataframe_name].df

        add_fields(**common_log_fields(context))

        out_config = OutputFileConfig(
            fields=generate_mps_response_header_fields(context.primitives['mps_request_reference']),
            file_path='mpsaas'
        )

        out_fileinfo = generate_output_files(spark, df_records, os.path.join(
            context.working_folder, PATHS.OUT), out_config)

        add_fields(
            outfiles=out_fileinfo
        )

        context.primitives['mpsaas_out_files'] = json.dumps(out_fileinfo)

        return context


class MPSaaSReplyOverMESHStage(PipelineStage):

    def __init__(self):
        super().__init__(name="mpsaas_mesh_reply")

    @log_action()
    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:

        add_fields(**common_log_fields(context))

        def outfile_location(path: str, suffix: str = FT.CSV) -> str:
            if path.startswith('s3://'):
                _, bucket, key_prefix = s3_split_path(path)
                keys = s3_bucket(bucket).objects.filter(Prefix='{}/'.format(key_prefix))
                if suffix:
                    keys = filter(lambda k: k.key.endswith(suffix), keys)
                return ['s3://{}/{}'.format(bucket, k.key) for k in keys]
            else:
                return glob('{}/*{}'.format(path, suffix))

        out_fileinfo = json.loads(context.primitives['mpsaas_out_files'])

        s3_locations = [
            outfile_location(out_fileinfo[0])[0],
            *outfile_location(out_fileinfo[1])
        ]

        filename = 'RESP_{}_{}.dat'.format(
            context.primitives['mps_request_reference'],
            datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        )

        mesh_transfer_id = MeshUploadJobs.create_mesh_upload_job(
            workflow_id=MESH_WORKFLOW_ID.MPSAAS,
            s3_locations=s3_locations,
            mesh_metadata=MeshMetadata(
                {
                    'filename': filename,
                    'local_id': context.primitives['submission_id'],
                    'message_type': 'DATA',
                }
            ),
            mailbox_to=context.primitives['sender_id'],
            status=MeshTransferStatus.Ready
        )

        add_fields(mesh_transfer_id=mesh_transfer_id)

        return context


def mpsaas_error_header_handler_provider(context: PipelineContext) -> HeaderErrorHandler:
    """
    Error handler provider for use by the CollectMPSResponse stage, which will reject the pipeline and return the error
    code to the user in the case that an error response is received

    Args:
        context: The context for the pipeline being executed

    Returns:
        An error handler for the CollectMPSResponse stage that copies the header error to the context and raises a
            PipelineRejection
    """
    def mpsaas_error_header_handler(header: Dict[str, Any]) -> None:
        header_error = header[MpsResponseHeaderFields.FILE_RESPONSE_CODE]
        context.primitives['mps_rejection_code'] = header_error
        raise PipelineRejection('Received response from MPS with code {}'.format(header_error))

    return mpsaas_error_header_handler


class MPSaaSPipelineRejectionHandler(PipelineRejectionHandler):

    def handle_pipeline_rejection(self, context: PipelineContext):
        mps_response_header_fields = generate_mps_response_header_fields(
            response_reference=context.primitives.get('mps_request_reference'),
            file_response_code=context.primitives.get('mps_rejection_code', SpineResponseCodes.UNEXPECTED_ERROR)
        )
        header_file_location = os.path.join(context.working_folder, PATHS.OUT, 'mpsaas.header/reject.csv')
        with smart_open(header_file_location, mode='w') as header_file:
            writer = csv.DictWriter(header_file, [*mps_response_header_fields.keys()])
            writer.writerow(mps_response_header_fields)

        filename = 'RESP_{}_{}.dat'.format(
            mps_response_header_fields[MPSResponseHeaderFields.RESPONSE_REFERENCE],
            datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        )

        mesh_transfer_id = MeshUploadJobs.create_mesh_upload_job(
            workflow_id=MESH_WORKFLOW_ID.MPSAAS,
            s3_locations=[header_file_location],
            mesh_metadata=MeshMetadata(
                {
                    'filename': filename,
                    'local_id': context.primitives['submission_id'],
                    'message_type': 'DATA',
                }
            ),
            mailbox_to=context.primitives['sender_id'],
            status=MeshTransferStatus.Ready
        )

        add_fields(mesh_transfer_id=mesh_transfer_id)


STAGES = [
    LoadMPSaaSCsvStage(),
    CleanseDataFrameStage(),
    PDSCrossCheckStage(),
    MPSCheckStage(),
    CollectMPSResponseStage(mps_result_dataframe_name='df',
                            header_error_handler_provider=mpsaas_error_header_handler_provider),
    MPSaaSEnrichStage(),
    TotalRecordsStage('total_submitted_records', {'df'}),
    OutputMPSaaSCsvStage(),
    MPSaaSReplyOverMESHStage()
]  # type: List[PipelineStage]
