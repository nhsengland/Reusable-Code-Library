import importlib
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from dsp.dam import current_record_version
from dsp.datasets.common import Fields as CommonFields
from nhs_reusable_code_library.resuable_codes.destructure import Destructurer
from dsp.shared.logger import add_fields, log_action


@log_action(log_args=['dataset_id', 'table_prefix', 'df_record_version'])
def uplift(spark: SparkSession, df: DataFrame, dataset_id: str, table_prefix: str,
           df_record_version: Optional[int] = None) -> DataFrame:
    """
    Uplift a dataframe (structured model) for the given dataset to the latest version for that dataset.

    Returns:
        the uplifted dataframe
    """
    latest_version = current_record_version(dataset_id)
    add_fields(latest_version=latest_version)

    if not Destructurer.ROOT_MODELS.get(dataset_id):
        # Some datasets don't have any uplifts available
        add_fields(
            message='No root model for this dataset, so no uplifts will be available. Returning original DataFrame'
        )
        return df

    if df_record_version is None:  # explicit check for None as 0 is a valid version
        # Take the first row of the DF and get its version
        head = df.head()
        if not head:
            # There's no data so there's nothing to uplift
            # Just return a new dataframe
            module = importlib.import_module("dsp.datasets.models.uplift.{}.{}.version_{}".format(
                dataset_id, table_prefix.lower(), latest_version)
            )
            schema = getattr(module, 'schema')
            df = spark.createDataFrame([], schema, verifySchema=False)
            return df

        if CommonFields.META not in head:
            add_fields(
                message='No META in this model. Returning original DataFrame'
            )
            return df

        df_record_version = head[CommonFields.META][CommonFields.RECORD_VERSION]

    add_fields(record_version=df_record_version)

    if df_record_version == latest_version:
        # We're done
        return df

    if df_record_version > latest_version:
        raise ValueError('Record version "{}" is greater than the latest version "{}" of {}'.format(df_record_version,
                                                                                                    latest_version,
                                                                                                    dataset_id))

    module = importlib.import_module("dsp.datasets.models.uplift.{}.{}.uplifts".format(
        dataset_id.lower(), table_prefix.lower()
    ))

    uplifts = getattr(module, 'UPLIFTS')

    if df_record_version not in uplifts:
        raise ValueError('No migration available to uplift from record version {}'.format(df_record_version))

    new_version, df = uplifts[df_record_version](df)

    add_fields(new_version=new_version)

    if new_version == latest_version:
        # We're done
        return df

    # Need another iteration
    return uplift(spark, df, dataset_id, table_prefix, new_version)
