from collections import OrderedDict
from typing import Tuple

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from schematics.exceptions import DataError

from dsp.datasets.fields.mpsaas.header import Order as HeaderFieldsList
from dsp.datasets.fields.mpsaas.submitted import Order, Fields as MpsRequestFields
from dsp.datasets.loaders import LoaderRejectionError
from dsp.datasets.models.mpsaas import MPSRequestHeaderRecord
from dsp.integration.mps.constants import SpineResponseCodes
from dsp.pipeline.models import PipelineContext

_SUBMISSION_SCHEMA = StructType([
    StructField(field, StringType(), True) for field in Order
])


def csv_loader(spark: SparkSession, path: str, context: PipelineContext, max_file_size: int = 500000) -> Tuple[
    DataFrame, OrderedDict]:
    """
    Returns a MPSaaS DataFrames from the csv. Raises an IndexError if the no. of columns don't match the expected

    Args:
       spark: The current Spark session
       path: The path to the CSV file to be loaded
       context: The context for the pipeline being executed
       max_file_size: The maximum file size to accept

    Returns:
       A tuple whose first element is a dataframe of the MPS records to enrich, and the second is a dictionary of the
       headers
    """
    # Read in mps request with schema
    # TODO: Currently this extends or truncates input silently as necessary; should validate submission fields
    df_with_mpsaas_columns = spark.read.csv(path, schema=_SUBMISSION_SCHEMA)

    # Pull out the header row and check it is well formed
    header_row = df_with_mpsaas_columns.first()

    submitted_header_fields_count = len(list(filter(None, header_row)))
    if submitted_header_fields_count != len(HeaderFieldsList):
        context.primitives['mps_rejection_code'] = SpineResponseCodes.NOT_ENOUGH_FIELDS_PROVIDED \
            if submitted_header_fields_count < len(HeaderFieldsList) \
            else SpineResponseCodes.NUMBER_OF_FIELDS_GREATER_THAN_ALLOWED
        raise LoaderRejectionError("Header line has a wrong number of fields")

    # Reformat the header with the correct column names
    header = OrderedDict()
    header.update(zip(HeaderFieldsList, header_row))

    # Validate the header format
    try:
        header_record = MPSRequestHeaderRecord({k.lower(): v for k, v in header.items()})
    except DataError as ex:
        # TODO: Could be improved with more precise feedback, ie. INVALID_TIMESTAMP; see DSP-7737
        context.primitives['mps_rejection_code'] = SpineResponseCodes.PARSE_ERROR
        raise LoaderRejectionError from ex

    # Reject if the record count > 500,000 as per https://nhsd-jira.digital.nhs.uk/browse/DSP-7736
    if header_record.no_of_data_records > max_file_size:
        context.primitives['mps_rejection_code'] = SpineResponseCodes.MAXIMUM_RECORDS_EXCEEDED
        raise LoaderRejectionError from ValueError(
            'Row count ({}) greater than maximum {}'.format(header_record.no_of_data_records, max_file_size))

    # Filter out the first line
    df_with_mpsaas_columns = df_with_mpsaas_columns.filter(
        col(MpsRequestFields.UNIQUE_REFERENCE).isNull() |
        ~col(MpsRequestFields.UNIQUE_REFERENCE).like('MPTREQ_%')
    )

    # Check the record count is accurate
    if header_record.no_of_data_records != df_with_mpsaas_columns.count():
        context.primitives['mps_rejection_code'] = SpineResponseCodes.DATA_RECORD_COUNT_MISMATCH
        raise LoaderRejectionError from ValueError(
            'Header row count ({}) does not match actual row count ({})'.format(
                header_record.no_of_data_records, df_with_mpsaas_columns.count()))

    return df_with_mpsaas_columns, header
