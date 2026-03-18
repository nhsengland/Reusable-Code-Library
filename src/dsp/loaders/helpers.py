import os
from datetime import datetime
from typing import Dict
from pyspark.sql.session import SparkSession

from dsp.datasets.common import DQMessageType
from dsp.dq_files.dq.schema import DQ_MESSAGE_WITH_TIMESTAMP_SCHEMA

from dsp.pipeline.models import PipelineContext
from dsp.shared.constants import PATHS

def write_invalid_schema_dq(spark: SparkSession, context: PipelineContext, reasons: Dict[str, str], code: str, rejection_detail: str):
    fields = list(reasons.keys())
    dq_df = spark.createDataFrame([
        (
            None, None, None, fields, code, rejection_detail, reasons, DQMessageType.Error,
            'reject_file', datetime.utcnow()
        )
    ], DQ_MESSAGE_WITH_TIMESTAMP_SCHEMA)

    dq_results_path = os.path.join(context.scoped_working_folder, PATHS.DQ)
    dq_df.write.mode('append').parquet(dq_results_path)
