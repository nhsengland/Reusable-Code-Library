import decimal
from datetime import date
from functools import partial
from typing import Tuple, List, Callable, Iterable

from pyspark.sql import DataFrame, Row, SparkSession, Column
from pyspark.sql.functions import col

from dsp.common import lhs, rhs
from nhs_reusable_code_library.resuable_codes.spark_helpers import schema_fill_values
from dsp.datasets.common import PDSCohortFields
from dsp.datasets.mps.request import Fields as Submitted
from dsp.datasets.mps.response import Fields as MPSResponse
from dsp.integration.mps.constants import SpineResponseCodes
from dsp.integration.mps.mps_schema import mps_request_schema_attributes, mps_response_schema_attributes, \
    mps_schema as MPS_SCHEMA
from dsp.shared.pds.pds_discovery import get_pds_data
from dsp.model.pds_record import PDSRecord
from dsp.shared.logger import log_action, add_fields


@log_action()
def cross_check(
        spark: SparkSession,
        df_mps_request: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """Performs cross check validation matching NHS number and DOB of submission data
       with pds data and returns matched and unmatched dataframe

       Args:
           spark: Spark session
           df_mps_request: A DataFrame at least partially conforming to the MPSRequest schema

       Returns:
          df_matched and df_unmatched dataframe
       """

    df_pds_data = get_pds_data(spark).select(
        col(PDSCohortFields.NHS_NUMBER),
        col(PDSCohortFields.RECORD)
    )

    df_joined = df_mps_request.alias('lhs').join(
        df_pds_data.alias('rhs'),
        col(lhs(Submitted.NHS_NO)) == col(rhs(PDSCohortFields.NHS_NUMBER)),
        how='left_outer'
    )

    _do_cross_check_trace = partial(do_cross_check_trace, mps_fields=df_mps_request.columns)

    rdd_matched = df_joined.rdd.map(_do_cross_check_trace)

    df_match_results = spark.createDataFrame(rdd_matched, schema=MPS_SCHEMA)

    df_match_results.cache()

    df_matched = df_match_results.filter("MATCH = True")
    df_matched = df_matched.select([col(source) for source in mps_response_schema_attributes])
    df_matched.cache()
    add_fields(matched_count=df_matched.count())

    df_unmatched = df_match_results.filter("MATCH = False")
    df_unmatched = df_unmatched.select([col(source) for source in mps_request_schema_attributes])
    df_unmatched.cache()
    add_fields(unmatched_count=df_unmatched.count())

    df_match_results.unpersist()

    return df_matched, df_unmatched


def make_matchable_df(df_records: DataFrame, mps_selections: Iterable[Tuple[str, Callable[[], Column]]]) -> DataFrame:
    for dest, source in mps_selections:
        df_records = df_records.withColumn(dest, source())

    df_records = df_records.select([col(dest) for dest, _ in mps_selections])

    return df_records


def do_cross_check_trace(row: Row, mps_fields: List[str]) -> Row:
    """Performs cross check validation for each row matching NHS number and DOB of submission data
          with pds data and returns matched and unmatched Row

          Args:
              row : Row
              mps_fields (List[str]): list of mps columns we're interested in

          Returns:
             no_match_record (has values populated for mps request schema )
             or
             match_record ( has values populated for mps response schema)
     """

    if not row[PDSCohortFields.NHS_NUMBER]:
        return _generate_unmatched_mps_request(row, mps_fields)

    # Load the PDS record so we can access its attributes
    pds_record = PDSRecord.from_json(row[PDSCohortFields.RECORD])

    dob = row[Submitted.DATE_OF_BIRTH]

    if pds_record.replaced_by_nhs_number():
        return _generate_unmatched_mps_request(row, mps_fields)

    if not dob or pds_record.date_of_birth() != dob:
        # Not matched on DOB - so we can't match
        return _generate_unmatched_mps_request(row, mps_fields)

    return _generate_matched_mps_response(
        row[Submitted.AS_AT_DATE],
        row[Submitted.UNIQUE_REFERENCE],
        row[Submitted.LOCAL_PATIENT_ID],
        pds_record
    )


def _generate_unmatched_mps_request(row: Row, mps_fields: List[str], mps_schema=MPS_SCHEMA):
    fields = {field: row[field] for field in mps_fields}

    unmatched_record = schema_fill_values(mps_schema, MATCH=False, **fields)

    return unmatched_record


def _generate_matched_mps_response(as_at_date: date, unique_reference: str, local_patient_id: str,
                                   pds_record: PDSRecord, mps_schema=MPS_SCHEMA, additional_fields={}):
    """Generate the equivalent of what MPS would respond with, based on the data that we have in our PDS cohort.
    """
    if not as_at_date:
        as_at_date = date.today()

    as_at = pds_record.get_point_in_time(as_at_date)

    dob = pds_record.date_of_birth() if pds_record.date_of_birth() else None
    dod = pds_record.date_of_death() if pds_record.date_of_death() else None

    gp_record_at = pds_record.gp_code_at(as_at)
    postcode_at = pds_record.postcode_at(as_at)
    family_name_at = pds_record.family_name_at(as_at)
    given_names_at = pds_record.given_names_at(as_at)
    given_name_at = given_names_at[:1][0] if given_names_at else None
    other_given_names_at = ' '.join(given_names_at[1:]) if given_names_at else None
    gender_at = pds_record.gender_at(as_at)
    address_at = pds_record.address_at(as_at)
    confidentiality_at = pds_record.confidentiality_at(as_at)
    sensitive_at = pds_record.sensitive_at(as_at)

    match_record = schema_fill_values(
        mps_schema,
        **{
            "MATCH": True,
            MPSResponse.UNIQUE_REFERENCE: unique_reference,
            MPSResponse.REQ_NHS_NUMBER: pds_record.nhs_number(),
            MPSResponse.FAMILY_NAME: family_name_at,
            MPSResponse.GIVEN_NAME: given_name_at,
            MPSResponse.OTHER_GIVEN_NAME: other_given_names_at,
            MPSResponse.GENDER: gender_at,
            MPSResponse.DATE_OF_BIRTH: dob,
            MPSResponse.DATE_OF_DEATH: None if sensitive_at else dod,
            MPSResponse.ADDRESS_LINE1: None if sensitive_at or not address_at else address_at[0:1][0],
            MPSResponse.ADDRESS_LINE2: None if sensitive_at or not address_at else address_at[1:2][0],
            MPSResponse.ADDRESS_LINE3: None if sensitive_at or not address_at else address_at[2:3][0],
            MPSResponse.ADDRESS_LINE4: None if sensitive_at or not address_at else address_at[3:4][0],
            MPSResponse.ADDRESS_LINE5: None if sensitive_at or not address_at else address_at[4:5][0],
            MPSResponse.POSTCODE: None if sensitive_at else postcode_at,
            MPSResponse.GP_PRACTICE_CODE: None if sensitive_at else gp_record_at,
            MPSResponse.NHAIS_POSTING_ID: None,
            MPSResponse.AS_AT_DATE: as_at,
            MPSResponse.LOCAL_PATIENT_ID: local_patient_id,
            MPSResponse.INTERNAL_ID: unique_reference,
            MPSResponse.TELEPHONE_NUMBER: None if sensitive_at else pds_record.telephone(),
            MPSResponse.MOBILE_NUMBER: None if sensitive_at else pds_record.mobile_phone(),
            MPSResponse.EMAIL_ADDRESS: None if sensitive_at else pds_record.email_address(),
            MPSResponse.SENSITIVTY_FLAG: confidentiality_at,
            MPSResponse.MPS_ID: None,
            MPSResponse.ERROR_SUCCESS_CODE: SpineResponseCodes.SENSITIVE if sensitive_at else SpineResponseCodes.SUCCESS,
            MPSResponse.MATCHED_NHS_NO: pds_record.nhs_number(),
            MPSResponse.MATCHED_ALGORITHM_INDICATOR: 1,  # Cross Check
            MPSResponse.MATCHED_CONFIDENCE_PERCENTAGE: decimal.Decimal(100),
        },
        **additional_fields
    )
    return match_record
