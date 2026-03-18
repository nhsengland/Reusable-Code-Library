import os
from typing import Set, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from dsp.common.delta.delta_helpers import delta_merge
from dsp.common.spark_helpers import table_exists, database_exists, create_asset
from dsp.datasets.sgss.ingestion.sgss.config import Fields
from shared.constants import PATHS, DS
from shared.logger import log_action, add_fields
from shared.models import Submission

sgss_excluded_columns = [
    "date_of_death",
    "death_status",
    "address",
    "email_address",
    "mobile_phone",
    "telephone",
    "gp_code",
    'MPSGIVEN_NAME',
    'MPSFAMILY_NAME',
    'MPSGENDER',
    'MPSDATE_OF_BIRTH',
    'MPSPOSTCODE',
    'MPSEMAIL_ADDRESS',
    'MPSMOBILE_NUMBER'
]

def get_dataset_specific_target_tables() -> Dict[str, Set]:
    return {DS.SGSS: {'sgss.sgss'},
            DS.SGSS_DELTA: {'sgss.full_non_de_dupe'}}


def get_dataset_output_tables() -> Dict[str, Set]:
    return {DS.SGSS: {''},
            DS.SGSS_DELTA: {''}}


def perform_delta_merge(spark: SparkSession, df: DataFrame, table_name: str, merge_key: str) -> Set[str]:
    tables = set()
    df_to_merge = df

    db, _ = table_name.split('.')

    if not database_exists(spark, db):
        create_asset(spark, 'curated', db)
        add_fields(database_created=True)

    incoming_cols = set(df_to_merge.schema.names)
    excluded_cols = set(sgss_excluded_columns)

    if db != 'sgss_ops':
        incoming_cols = incoming_cols - excluded_cols
        df_to_merge = df_to_merge.select(list(incoming_cols))

    if not table_exists(spark, table_name):
        df_to_merge.write.format('delta').saveAsTable(table_name)
        tables.add(table_name)
        add_fields(table_created=True)
        return tables

    existing_cols = set(spark.table(table_name).schema.names)

    existing_cols_lower = [existing_col.lower() for existing_col in existing_cols]
    incoming_cols_lower = [incoming_col.lower() for incoming_col in incoming_cols]

    new_cols = [new_col for new_col in incoming_cols if new_col.lower() not in existing_cols_lower]
    older_cols = [older_col for older_col in existing_cols if older_col.lower() not in incoming_cols_lower]

    if older_cols:
        for c in older_cols:
            df_to_merge = df_to_merge.withColumn(c, lit(None))

    if new_cols:
        new_cols = [df_to_merge.schema[c].simpleString().replace(':', ' ') for c in new_cols]
        add_fields(new_cols=new_cols)
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({', '.join(new_cols)})")

    df_to_merge = df_to_merge.select(*spark.table(table_name).schema.names)

    delta_merge(spark, target_delta_table_name=table_name, target_delta_table_alias='lhs',
                source_df=df_to_merge, source_table_alias='rhs', on=f'lhs.{merge_key} = rhs.{merge_key}',
                when_matched_update_all=True, when_not_matched_insert_all=True)

    tables.add(table_name)

    return tables

@log_action(log_args=['submission'])
def delta_merge_sgss(spark: SparkSession, submission: Submission) -> Set[str]:
    tables = set()
    dataset_id = submission.dataset_id
    working_directory = submission.metadata.working_folder

    target_tables = get_dataset_specific_target_tables()[dataset_id]

    assert len(target_tables) == 1, 'sgss delta merge does not support multi table merges'

    table_name = next(iter(target_tables))
    _, table = table_name.split('.')
    ops_table_name = 'sgss_ops.' + table

    add_fields(target_table=table_name)

    df_to_merge = spark.read.parquet(os.path.join(working_directory, PATHS.OUT))

    merge_key = Fields.CDR_Specimen_Request_SK

    tables.update(perform_delta_merge(spark, df_to_merge, table_name, merge_key))
    tables.update(perform_delta_merge(spark, df_to_merge, ops_table_name, merge_key))

    return tables
