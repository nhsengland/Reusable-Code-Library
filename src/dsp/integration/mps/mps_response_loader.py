import csv
from collections import OrderedDict
from io import StringIO
from typing import Union, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession

from dsp.datasets.mps.response import Fields as MpsResponseFields
from dsp.datasets.mps.response_header import Fields as MpsResponseHeaderFields
from dsp.datasets.mps.response_header import Order as MpsResponseHeaderFieldsOrder
from dsp.loaders import LoaderRejectionError
from dsp.integration.mps.mps_schema import mps_response_schema
from dsp.shared.common.test_helpers import smart_open
from dsp.shared.logger import log_action, add_fields


@log_action(log_args=['path', 'with_header', 'recovery_run'])
def csv_loader(spark: SparkSession, path: str, with_header: bool = False, first_pass: bool = True) -> Union[
    DataFrame,
    Tuple[DataFrame, OrderedDict]
]:
    """
    Loads the MPS response into a Dataframe.

    Attributes:
        spark (SparkSession)
        path (str) location of the MPS response chunk
        with_header (bool) if True, will return a tuple of dataframe, header - otherwise just the dataframe
                           defaults to False
    """
    mps_file_df = (spark.read
                    .option('escape','\"')
                    .csv(path, mps_response_schema))

    mps_header_df = mps_file_df.filter(
        col(MpsResponseFields.UNIQUE_REFERENCE).isNotNull() &
        col(MpsResponseFields.UNIQUE_REFERENCE).like('MPTREQ_%')
    )

    header_row = mps_header_df.first()
    if len(list(filter(None, header_row))) != len(MpsResponseHeaderFieldsOrder):
        raise LoaderRejectionError("Header line has a wrong number of fields")

    header = OrderedDict()
    header.update(zip(MpsResponseHeaderFieldsOrder, header_row))
    add_fields(**header)

    # Filter out the first line
    mps_data_df = mps_file_df.filter(
        col(MpsResponseFields.UNIQUE_REFERENCE).isNull() |
        ~col(MpsResponseFields.UNIQUE_REFERENCE).like('MPTREQ_%')
    )

    mps_data_df.cache()

    actual_row_count = mps_data_df.count()
    expected_row_count = int(header[MpsResponseHeaderFields.NO_OF_DATA_RECORDS])

    add_fields(actual_row_count=actual_row_count, expected_row_count=expected_row_count)

    if actual_row_count != expected_row_count:
        mps_data_df.unpersist()
        if first_pass:
            fixed_path = attempt_recovery(path)
            return csv_loader(spark, fixed_path, with_header, first_pass=False)
        else:
            raise LoaderRejectionError(
                f"The number of rows in the file ({actual_row_count}) doesn't match the header ({expected_row_count})."
            )

    if with_header:
        return mps_data_df, header

    return mps_data_df


def attempt_recovery(path: str) -> str:
    """We have seen examples of an individual MPS response line being split across multiple lines. This causes a problem
    as we expect every line to be a complete record. There is no valid reason for lines to be split, but it's a
    side-effect of DQ issues in PDS data, e.g. addresses having carriage returns in them

    Args:
        path: the path to the file to recover from

    Returns:
        the path the to fixed file
    """

    with smart_open(path, 'r') as input_file:
        input_data = input_file.read()
        reader = csv.reader(StringIO(input_data), delimiter=',', quotechar='"')

        fixed_path = path + '_fixed'

        with smart_open(fixed_path, mode='w') as output_file:
            output_data = StringIO()
            writer = csv.writer(output_data, delimiter=',', quotechar='"')
            for row in reader:
                writer.writerow([item.replace('\n', '').replace('\r', '').strip() for item in row])

            output_file.write(output_data.getvalue())

        return fixed_path
