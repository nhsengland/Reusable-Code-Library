from pyspark.sql import DataFrame

from dsp.common import canonical_name
from dsp.dam import canonicalise_fields
from dsp.pipeline import Metadata


def canonicalise_field_names(data_frame: DataFrame, metadata: Metadata = None) -> DataFrame:
    if metadata:
        dataset_id = metadata['dataset_id']

        if not canonicalise_fields(dataset_id):
            return data_frame

    for name in data_frame.schema.names:
        c_name = canonical_name(name)
        if c_name == name:
            continue
        data_frame = data_frame.withColumnRenamed(name, c_name)

    return data_frame
