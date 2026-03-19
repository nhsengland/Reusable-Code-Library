from typing import Callable, Any
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import struct, concat_ws, lit, when, col, create_map
from pyspark.sql.types import DataType, StructType, StructField

from dsp.datasets.common import Fields as Common
from dsp.dq_files.dq.schema import DQ_MESSAGE_SCHEMA
from dsp.datasets.epma_national.constants import ExcludedColumns
from dsp.dq_files.output import Fields as DQFields
import dsp.datasets.models.csds as csds
import dsp.datasets.models.csds_v1_6 as csds_v1_6
import dsp.datasets.models.iapt as iapt
import dsp.datasets.models.iapt_v2_1 as iapt_v2_1
import dsp.datasets.models.mhsds_v5 as mhsds_v5
import dsp.datasets.models.mhsds_v5 as mhsds_v6
import dsp.datasets.models.msds as msds
import dsp.datasets.models.pcaremeds as pcaremeds
import dsp.datasets.models.epmawspc as epmawspc
import dsp.datasets.models.epmawsad as epmawsad
import dsp.datasets.models.epmawspc2 as epmawspc2
import dsp.datasets.models.epmawsad2 as epmawsad2
import dsp.datasets.models.epmanationalpres as epmanationalpres
import dsp.datasets.models.epmanationaladm as epmanationaladm
from dsp.datasets.epma.epmawspc2.epmawspc2 import EXCLUDED_FIELDS as epmawspc2_removed_columns
from dsp.datasets.epma.epmawsad2.epmawsad2 import EXCLUDED_FIELDS as epmawsad2_removed_columns
from dsp.datasets.models import gp_data
from dsp.datasets.models.uplift.model_uplift import uplift
from dsp.pipeline.loading import add_traceability_columns
from dsp.shared.common import parse_timestamp
from dsp.shared.constants import FT, DS

_DATASET_STRUCT = {
    DS.CSDS_GENERIC: csds.MPI.get_struct(meta_available=False),
    DS.CSDS_V1_6: csds_v1_6.MPI.get_struct(meta_available=False),
    DS.IAPT_GENERIC: iapt.Referral.get_struct(meta_available=False),
    DS.IAPT_V2_1: iapt_v2_1.Referral.get_struct(meta_available=False),
    DS.MHSDS_V1_TO_V5_AS_V6: mhsds_v5.Referral.get_struct(meta_available=False),
    DS.MHSDS_V6: mhsds_v6.Referral.get_struct(meta_available=False),
    DS.MSDS: msds.PregnancyAndBookingDetails.get_struct(meta_available=False),
    DS.PCAREMEDS: pcaremeds.PrimaryCareMedicineModel.get_struct(meta_available=False),
    DS.GP_DATA_PATIENT: gp_data.GPDataPatientModel.get_struct(meta_available=False, use_spec_spark_type=True),
    DS.GP_DATA_APPOINTMENT: gp_data.GPDataAppointmentModel.get_struct(meta_available=False, use_spec_spark_type=True),
    DS.EPMAWSPC: epmawspc.EPMAWellSkyPrescriptionModel.get_struct(meta_available=False),
    DS.EPMAWSAD: epmawsad.EPMAWellSkyAdministrationModel.get_struct(meta_available=False),
    DS.EPMAWSPC2: StructType([itm for itm in epmawspc2.EPMAWellSkyPrescriptionModel2.get_struct(meta_available=False) if
                              itm.name not in epmawspc2_removed_columns]),
    DS.EPMAWSAD2: StructType(
        [itm for itm in epmawsad2.EPMAWellSkyAdministrationModel2.get_struct(meta_available=False) if
         itm.name not in epmawsad2_removed_columns]),
    DS.EPMANATIONALPRES: StructType([
        itm for itm in epmanationalpres.EPMAPrescriptionModel.get_struct(meta_available=False)
        if itm.name not in ExcludedColumns.PRESC_COLS
    ]),
    DS.EPMANATIONALADM: StructType([
        itm for itm in epmanationaladm.EPMAAdministrationModel.get_struct(meta_available=False)
        if itm.name not in ExcludedColumns.ADMIN_COLS
    ])
}  # type: Dict[str, DataType]


def _dq_loader(spark: SparkSession, path: str, _: dict) -> DataFrame:
    df = spark.read.format("com.databricks.spark.xml") \
        .schema(StructType([StructField(field.name, field.dataType, nullable=True) for field in DQ_MESSAGE_SCHEMA])) \
        .load(path) \
        .withColumn(DQFields.FIELD_VALUES, when(col(DQFields.FIELD_VALUES).isNull(), create_map())
                    .otherwise(col(DQFields.FIELD_VALUES)))

    return df


def dq_expected_enrichment(df: DataFrame, metadata: dict) -> DataFrame:
    submission_id = metadata['submission_id']
    submitted_timestamp = parse_timestamp(metadata['submitted_timestamp'])

    df_wc = df.withColumn(Common.META, struct(
        concat_ws(':', lit(submission_id), df.ROW_INDEX).alias(Common.EVENT_ID),
        df.ROW_INDEX, lit(submitted_timestamp).alias(Common.EVENT_RECEIVED_TS)
    )).drop(DQFields.ROW_INDEX)

    return df_wc


DATATYPE_LOADERS = {
    FT.CSV: lambda spark, path, metadata: spark.read.option("header", "true").option("inferSchema", "false").csv(path),
    FT.PARQUET: lambda spark, path, metadata: uplift(spark,
                                                     spark.read.parquet(path),
                                                     dataset_id=metadata['dataset_id'],
                                                     table_prefix=path.rsplit('/', 1)[-1]),
    FT.JSON: lambda spark, path, metadata: spark.read.json(path=path,
                                                           schema=_DATASET_STRUCT[metadata['dataset_id']],
                                                           multiLine=True,
                                                           mode='FAILFAST'),
    'dq_xml': _dq_loader
}  # type: Dict[str, Callable[[SparkSession, str, Dict[str, Any]], DataFrame]]


def dataset_loader(file_type: str) -> Callable[[SparkSession, str, Dict[str, Any]], DataFrame]:
    return DATATYPE_LOADERS[file_type]


def load_files(spark: SparkSession, metadata: dict, add_metadata: bool) -> DataFrame:
    file_format = metadata['file_format']
    file_location = metadata['file_location']

    df = dataset_loader(file_format)(spark, file_location, metadata)

    if add_metadata:

        if Common.META in df.schema.names:
            return df

        return add_traceability_columns(spark, df, metadata)

    return df
