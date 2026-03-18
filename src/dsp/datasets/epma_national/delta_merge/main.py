from typing import Dict
import os

from dsp.common.delta.delta_helpers import delta_merge
from dsp.datasets.definitions.epma_national.epmanationaladm.submission_constants import \
    EPMANationalAdministrationMedicationColumns
from dsp.datasets.definitions.epma_national.epmanationalpres.submission_constants import \
    EPMANationalPrescriptionPrescriptionColumns
from dsp.datasets.epma_national.epmanationalpres.epmanationalpres import \
    DELTA_MERGES as epmanationalpres_delta_merge_params
from dsp.datasets.epma_national.epmanationaladm.epmanationaladm import \
    DELTA_MERGES as epmanationaladm_delta_merge_params
from dsp.pipeline.delta_merge_models import DeltaMergeParameters
from dsp.shared.constants import DS, PATHS
from dsp.shared.logger import log_action
from dsp.shared.models import Submission

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit


def get_dataset_specific_delta_merge_parameters() -> Dict[str, DeltaMergeParameters]:
    return {
        DS.EPMANATIONALPRES: epmanationalpres_delta_merge_params,
        DS.EPMANATIONALADM: epmanationaladm_delta_merge_params,
    }


def get_dataset_specific_timestamp_check_fields() -> Dict[str, str]:
    return {
        DS.EPMANATIONALPRES: EPMANationalPrescriptionPrescriptionColumns.PRESCRIBED_MEDICATION_RECORD_LAST_UPDATED,
        DS.EPMANATIONALADM: EPMANationalAdministrationMedicationColumns.MEDICATION_ADMINISTRATION_RECORD_LAST_UPDATED_TIMESTAMP,
    }


@log_action()
def delta_merge_epma_national(spark: SparkSession, submission: Submission):

    # Get submission info
    submitted = submission.submitted
    submission_id = submission.id
    dataset_id = submission.dataset_id

    # Get delta merge params
   
    dataset_delta_merges = get_dataset_specific_delta_merge_parameters().get(dataset_id)
    assert dataset_delta_merges, f'{dataset_id} provided is not a valid epma national dataset.'
    assert len(dataset_delta_merges) == 1, 'epma national merge does not support merging into multiple target tables.'
  
    delta_merge_params = dataset_delta_merges[0]

    working_folder = submission.metadata.working_folder
    source_directory = delta_merge_params.source_directory
    # Load pipeline result into df
    submission_df = spark.read.parquet(os.path.join(working_folder, PATHS.OUT, source_directory))
    join_cols = delta_merge_params.join_columns
    # Where multiple join cols, create list of col expressions
    join_criteria = [col(f'src.{flds}') == col(f'trgt.{flds}') for flds in join_cols]
   
    # Get timestamp check col from above defined map
    timestamp_check_col = get_dataset_specific_timestamp_check_fields().get(dataset_id)
    assert timestamp_check_col, f'the dataset {dataset_id} does not have an appropriate timestamp check column defined to run the epma national delta merge'

    output_cols = list(delta_merge_params.output_columns) + ['EFFECTIVE_FROM', 'EFFECTIVE_TO']
    target_table = delta_merge_params.target_table

    # check if submission previously merged - if so, ignore
    exists = spark.table(delta_merge_params.target_table).where(
        col('meta.EVENT_ID').like(f'{submission_id}:%')).limit(1).count()

    # Already merged?
    if exists:
        # Yup, bye...
        print('submission: {} already merged to: {}'.format(submission_id, delta_merge_params.target_table))
        return target_table

    # label pipeline result df with flag where rec previously received, determine effective_to timestamp for each record
    merge_cols_of_interest = ['META', timestamp_check_col, *list(join_cols)]
    target_df = spark.table(target_table).filter(col('EFFECTIVE_TO').isNull()).select(merge_cols_of_interest).alias(
        'trgt')
    delta_check_df = submission_df \
        .withColumn('EFFECTIVE_FROM', lit(submitted)).alias('src') \
        .join(target_df, on=join_criteria, how='left') \
        .withColumn('PREVIOUSLY_RECEIVED', when(col('trgt.META').isNotNull(), lit(True)).otherwise(lit(False))) \
        .withColumn('UPDATED_RECORD_FLAG', when(col(f'src.{timestamp_check_col}') >= col(f'trgt.{timestamp_check_col}'),
                                                lit(True)).otherwise(lit(False))) \
        .withColumn('EFFECTIVE_TO',
                    when((~col('PREVIOUSLY_RECEIVED')) | (col('PREVIOUSLY_RECEIVED') & col('UPDATED_RECORD_FLAG')),
                         lit(None)).otherwise(lit(submitted))) \
        .select('src.*', 'PREVIOUSLY_RECEIVED', 'UPDATED_RECORD_FLAG', 'EFFECTIVE_TO')

    # log if any records have been superceded - if so run merge statement
    delta_check_df.cache()
    rec_count = delta_check_df.filter((col('PREVIOUSLY_RECEIVED') == lit(True)) & col('EFFECTIVE_TO').isNull()).count()
    print(f'{rec_count} record(s) requiring update in {target_table}')

    # Where the new submission record has been determined to have EFFECTIVE_TO as null, end date the previous record in asset

    if rec_count:
        delta_merge(
            spark,
            target_delta_table_name=target_table,
            target_delta_table_alias='trgt',
            source_df=delta_check_df.filter(col('EFFECTIVE_TO').isNull()),
            source_table_alias='src',
            on=' and '.join([f'src.{flds}=trgt.{flds}' for flds in join_cols]),
            when_matched_update={'trgt.EFFECTIVE_TO': 'src.EFFECTIVE_FROM'}
                            )

    # append pipeline result to asset
    delta_check_df = delta_check_df.drop(*['PREVIOUSLY_RECEIVED', 'UPDATED_RECORD_FLAG'])
    delta_check_df.select(*output_cols).write.mode('append').format('delta').saveAsTable(target_table)

    delta_check_df.unpersist()

    # return target table for optimize and vacuum - unavailable locally, but handled by driving delta_merge notebook in databricks

    return [target_table]
