from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import desc, rank, from_json, split, col, slice, array_join, expr, array_sort, reverse, lower, floor, lit
from pyspark.sql.types import *
from pyspark.sql.types import Row

from nhs_reusable_code_library.resuable_codes.spark_helpers import schema_fill_values
from dsp.datasets.common import PDSCohortFields as Fields
from dsp.loaders import LoaderRejectionError
from dsp.model.pds_record import PDSRecord, PDSDataImportException
from dsp.shared.logger import log_action, add_fields


PARSED_RECORD_SCHEMA = StructType([
    StructField(Fields.NHS_NUMBER, StringType()),
    StructField(Fields.REPLACED_BY, StringType()),
    StructField(Fields.EMAIL_ADDRESS, StringType()),
    StructField(Fields.MOBILE_PHONE, StringType()),
    StructField(Fields.TELEPHONE, StringType()),
    StructField(Fields.DOB, IntegerType()),
    StructField(Fields.DOD, IntegerType()),
    StructField(Fields.DATE, StringType()),
    StructField(Fields.SENSITIVE, BooleanType()),
    StructField(Fields.DEATH_STATUS, StringType()),
    StructField(Fields.GP_CODES, ArrayType(StructType([
        StructField(Fields.VAL, StringType()),
        StructField(Fields.FROM, IntegerType()),
        StructField(Fields.TO, IntegerType()),
        StructField(Fields.SCN, IntegerType())
    ]
    ))),
    StructField(Fields.ADDRESS_HISTORY, ArrayType(StructType([
        StructField(Fields.ADDR, ArrayType(StructType([
            StructField(Fields.LINE, StringType())
        ]))),
        StructField(Fields.POSTAL_CODE, StringType()),
        StructField(Fields.FROM, IntegerType()),
        StructField(Fields.TO, IntegerType()),
        StructField(Fields.SCN, IntegerType())
    ]
    ))),
    StructField(Fields.NAME_HISTORY, ArrayType(StructType([
        StructField(Fields.GIVEN_NAME, ArrayType(StructType([
            StructField(Fields.NAME, StringType())
        ]))),
        StructField(Fields.FAMILY_NAME, StringType()),
        StructField(Fields.FROM, IntegerType()),
        StructField(Fields.TO, IntegerType()),
        StructField(Fields.SCN, IntegerType())
    ]
    ))),
    StructField(Fields.GENDER_HISTORY, ArrayType(StructType([
        StructField(Fields.GENDER, StringType()),
        StructField(Fields.FROM, IntegerType()),
        StructField(Fields.TO, IntegerType()),
        StructField(Fields.SCN, IntegerType())
    ]
    ))),
    StructField(Fields.CONFIDENTIALITY, ArrayType(StructType([
        StructField(Fields.VAL, StringType()),
        StructField(Fields.FROM, IntegerType()),
        StructField(Fields.TO, IntegerType()),
        StructField(Fields.SCN, IntegerType())
    ]
    ))),
    StructField(Fields.MIGRANT_DATA, ArrayType(StructType([
        StructField(Fields.VISA_STATUS, StringType()),
        StructField(Fields.BRP_NO, StringType()),
        StructField(Fields.HOME_OFFICE_REF_NO, StringType()),
        StructField(Fields.NATIONALITY, StringType()),
        StructField(Fields.VISA_FROM, IntegerType()),
        StructField(Fields.VISA_TO, IntegerType()),
        StructField(Fields.FROM, IntegerType()),
        StructField(Fields.TO, IntegerType()),
        StructField(Fields.SCN, IntegerType())
    ]
    ))),
    StructField(Fields.MPSID_DETAILS, ArrayType(StructType([
        StructField(Fields.MPS_ID, StringType()),
        StructField(Fields.LPI, StringType()),
        StructField(Fields.SCN, IntegerType())
    ]
    ))),
])

PDS_SCHEMA = StructType([
    StructField(Fields.ACTIVITY_DATE, StringType(), True),
    StructField(Fields.SERIAL_CHANGE_NUMBER, IntegerType(), True),
    StructField(Fields.NHS_NUMBER, StringType(), True),
    StructField(Fields.DATE_OF_BIRTH, IntegerType(), True),
    StructField(Fields.DATE_OF_DEATH, IntegerType(), True),
    StructField(Fields.PARSED_RECORD, PARSED_RECORD_SCHEMA, True),
    StructField(Fields.RECORD, StringType(), True),
    StructField(Fields.TRANSFER_ID, StringType(), True)
])

_LINE = 'line'
_ITEMS = 'items'


@log_action(log_args=['path'])
def pds_loader(spark: SparkSession, path: str, transfer_id: str) -> DataFrame:
    """The file that we receive from PDS has a header line containing metadata, followed by N lines of actual data that
    we are interested in. The line is pipe delimited, but it's not a true pipe delimited file because the record part
    of each line can contain unescaped pipes. So we can't use spark.read.csv with a pipe delimiter - we have to read it
    as a text file and parse it ourselves.

    Args:
        spark: the spark engine
        path: the location of the file received from PDS
        transfer_id: the id of the transfer that contained this update

    Returns:
        the dataframe containing the PDS data

    """
    fields = [
        StructField(_LINE, StringType()),
    ]

    schema = StructType(fields)
    df_lines = spark.read.schema(schema=schema).text(path)

    header = df_lines.first()[0].split('|')

    file_ref, dsp, update, date_time, internal_id, file_number, total_num_of_files, total_number_of_nhs_nos = header

    add_fields(
        file_ref=file_ref,
        dsp=dsp,
        update=update,
        date_time=date_time,
        internal_id=internal_id,
        file_number=file_number,
        total_num_of_files=total_num_of_files,
        total_number_of_nhs_nos=total_number_of_nhs_nos
    )

    if dsp != 'DSP':
        raise LoaderRejectionError("Header line appears corrupt. Expected DSP, but got {}".format(dsp))

    df_loaded = _filter_out_header_line(df_lines)

    _check_row_count_matches_header(df_loaded, total_number_of_nhs_nos)

    df_remove_duplicates = _remove_duplicate_records(df_loaded)

    df_trimmed = _remove_older_records_from_dataset(df_remove_duplicates)

    latest_row_count = df_trimmed.count()

    df_validated = _validate_and_trim_dataset(
        spark,
        df_trimmed.withColumn(Fields.TRANSFER_ID, lit(transfer_id)),
        filter_invalid=True
    )

    df_parsed = _parse_and_derive_pds(df_validated, filter_invalid=True)

    success_count = df_parsed.count()

    failed_count = latest_row_count - success_count

    add_fields(success_count=success_count, failed_count=failed_count)

    if success_count == 0:
        raise LoaderRejectionError('pds_load returned no valid records .. this seems incorrect')

    return df_parsed


def _filter_out_header_line(df: DataFrame) -> DataFrame:
    """Filter out the first line (the second column has a fixed value) and alias the ones that we are interested in."""

    df_loaded = (
        df.withColumn(_ITEMS, split(col(_LINE), r'\|'))
        .filter(col(_ITEMS).getItem(1) != 'DSP')
        .withColumn(Fields.RECORD, array_join(slice(col(_ITEMS), 4, (1000*1000)), '|'))
        .select(
            col(_ITEMS).getItem(0).alias(Fields.ACTIVITY_DATE),
            col(_ITEMS).getItem(1).alias(Fields.SERIAL_CHANGE_NUMBER).cast('int'),
            col(_ITEMS).getItem(2).alias(Fields.NHS_NUMBER),
            col(Fields.RECORD)
        )
    )

    return df_loaded


def _check_row_count_matches_header(df: DataFrame, total_from_header: int) -> None:
    """Ensure the number of data rows match the number of records indicated by the header row"""
    actual_row_count = df.count()

    if actual_row_count != int(total_from_header):
        raise LoaderRejectionError(
            "The number of rows in the file ({}) doesn't match the header ({}).".format(actual_row_count,
                                                                                        total_from_header)
        )


def _remove_duplicate_records(df: DataFrame):
    df = df.dropDuplicates([
        Fields.NHS_NUMBER,
        Fields.SERIAL_CHANGE_NUMBER
    ])
    return df


def _remove_older_records_from_dataset(df: DataFrame) -> DataFrame:
    """Remove any historic PDS records based on the SCN for each NHS number"""
    w = Window.partitionBy(Fields.NHS_NUMBER).orderBy(desc(Fields.SERIAL_CHANGE_NUMBER))
    return df.withColumn("rank", rank().over(w)).filter(col('rank') == lit(1)).drop("rank")


def _validate_and_trim_dataset(spark: SparkSession, df_loaded: DataFrame,  filter_invalid: bool = True) -> DataFrame:
    """Validate all records, transform to our PDSRecord model and remove failed records, if there are any."""

    rdd_validated = df_loaded.rdd.map(_validate_and_transform)  # type: RDD[tuple]

    df_validated = spark.createDataFrame(rdd_validated, schema=PDS_SCHEMA)

    return df_validated.filter(col(Fields.SERIAL_CHANGE_NUMBER) != lit(0)) if filter_invalid else df_validated


def _validate_and_transform(row: Row) -> Row:
    try:

        # Build a PDSRecord from the data we have received from MESH
        record = PDSRecord.from_mesh_line(
            row[Fields.ACTIVITY_DATE],
            row[Fields.SERIAL_CHANGE_NUMBER],
            row[Fields.NHS_NUMBER],
            row[Fields.RECORD]
        )

        # Update the row with the data that has been populated within the PDSRecord
        new_row = schema_fill_values(
            PDS_SCHEMA,
            **{
                Fields.ACTIVITY_DATE: record.activity_date(),
                Fields.SERIAL_CHANGE_NUMBER: record.sequence_number(),
                Fields.NHS_NUMBER: record.nhs_number(),
                Fields.DATE_OF_BIRTH: record.date_of_birth(),
                Fields.DATE_OF_DEATH: record.date_of_death(),
                Fields.RECORD: record.to_json(),
                Fields.TRANSFER_ID: row[Fields.TRANSFER_ID]
            }
        )

    except PDSDataImportException as lre:
        print(lre)
        new_row = schema_fill_values(
            PDS_SCHEMA,
            **{
                Fields.ACTIVITY_DATE: '0',
                Fields.SERIAL_CHANGE_NUMBER: 0,
                Fields.NHS_NUMBER: '0',
                Fields.DATE_OF_BIRTH: 0,
                Fields.DATE_OF_DEATH: 0,
                Fields.RECORD: '',
                Fields.TRANSFER_ID: row[Fields.TRANSFER_ID]
            }
        )

    return new_row


def _parse_and_derive_pds(df_validated: DataFrame, filter_invalid: bool = True) -> DataFrame:
    """parse and derive pds records."""

    df_parsed = df_validated.withColumn(Fields.PARSED_RECORD, from_json(col(Fields.RECORD), PARSED_RECORD_SCHEMA))

    df_derived = (
        df_parsed
        .withColumn(Fields.MONTH_OF_BIRTH, floor(col(Fields.DATE_OF_BIRTH)/lit(100)))
        .withColumn(Fields.YEAR_OF_BIRTH, floor(col(Fields.MONTH_OF_BIRTH)/lit(100)))
        .withColumn(Fields.BIRTH_DECADE, floor(col(Fields.YEAR_OF_BIRTH)/lit(10)) * lit(10))
        .withColumn(Fields.REPLACED_BY, col(f'{Fields.PARSED_RECORD}.{Fields.REPLACED_BY}'))
        .withColumn(Fields.NAME, reverse(array_sort(expr("transform(filter(parsed_record.name_history, w -> w.to is null), x -> struct(x.scn as scn, x.from as from, x.to as to, filter(transform(x.givenName, y -> upper(y.name)), z -> length(z) > 0) as givenNames, upper(x.familyName) as familyName))"))).getItem(0))
        .withColumn(Fields.ADDRESS, reverse(array_sort(expr("transform(filter(parsed_record.address_history, w -> w.to is null), x -> struct(x.scn as scn, x.from as from, x.to as to, filter(transform(x.addr, y -> y.line), z -> length(z) > 0) as lines, upper(x.postalCode) as postCode))"))).getItem(0))
        .withColumn(Fields.GP, reverse(array_sort(expr("transform(filter(parsed_record.gp_codes, w -> w.to is null), x -> struct(x.scn as scn, x.from as from, x.to as to, x.val as code))"))).getItem(0))
        .withColumn(Fields.GENDER, reverse(array_sort(expr("transform(filter(parsed_record.gender_history, w -> w.to is null), x -> struct(x.scn as scn, x.from as from, x.to as to, x.gender as gender))"))).getItem(0))
        .withColumn(Fields.CONFIDENTIALITY, reverse(array_sort(expr("transform(filter(parsed_record.confidentiality, w -> w.to is null), x -> struct(x.scn as scn, x.from as from, x.to as to, x.val as code))"))).getItem(0))
        .withColumn(Fields.MIGRANT_DATA, reverse(array_sort(expr("transform(filter(parsed_record.migrant_data, w -> w.to is null), x -> struct(x.scn as scn, x.from as from, x.to as to, x.visa_status as visa_status, x.brp_no as brp_no, x.home_office_ref_no as home_office_ref_no, x.nationality as nationality, x.visa_from as visa_from, x.visa_to as visa_to))"))).getItem(0))
        .withColumn(Fields.EMAIL_ADDRESS, lower(col(f'{Fields.PARSED_RECORD}.{Fields.EMAIL_ADDRESS}')))
        .withColumn(Fields.MOBILE_PHONE, lower(col(f'{Fields.PARSED_RECORD}.{Fields.MOBILE_PHONE}')))
        .withColumn(Fields.TELEPHONE, lower(col(f'{Fields.PARSED_RECORD}.{Fields.TELEPHONE}')))
        .withColumn(Fields.SENSITIVE, col(f'{Fields.PARSED_RECORD}.{Fields.SENSITIVE}'))
        .withColumn(Fields.DEATH_STATUS, col(f'{Fields.PARSED_RECORD}.{Fields.DEATH_STATUS}'))
        .select(
            Fields.ACTIVITY_DATE,
            Fields.SERIAL_CHANGE_NUMBER,
            Fields.NHS_NUMBER,
            Fields.REPLACED_BY,
            Fields.BIRTH_DECADE,
            Fields.YEAR_OF_BIRTH,
            Fields.MONTH_OF_BIRTH,
            Fields.DATE_OF_BIRTH,
            Fields.DATE_OF_DEATH,
            Fields.SENSITIVE,
            Fields.DEATH_STATUS,
            Fields.NAME,
            Fields.GENDER,
            Fields.ADDRESS,
            Fields.GP,
            Fields.CONFIDENTIALITY,
            Fields.MIGRANT_DATA,
            Fields.EMAIL_ADDRESS,
            Fields.MOBILE_PHONE,
            Fields.TELEPHONE,
            Fields.PARSED_RECORD,
            Fields.RECORD,
            Fields.TRANSFER_ID
        )
    )

    return df_derived.filter(col(Fields.PARSED_RECORD).isNotNull()) if filter_invalid else df_derived
