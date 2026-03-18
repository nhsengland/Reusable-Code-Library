import decimal
from datetime import date
from functools import partial, reduce
from operator import ior
from typing import Any, AbstractSet, Tuple, Iterable, Callable, List

from pyspark.sql import SparkSession, DataFrame, Column, Row, Window
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, substring, floor, lit, when, count

from dsp.common import lhs, rhs
from nhs_reusable_code_library.resuable_codes.spark_helpers import schema_fill_values
from dsp.dam import mps_request
from dsp.datasets.common import PDSCohortFields
#from dsp.datasets.covid19_testing.pipelines.covid19_testing_common.common import create_cases
#from dsp.datasets.covid19_testing.schema.shared.util import cleaned_first_name, cleaned_last_name, \
    #cleaned_mobile_number, cleaned_email
from dsp.datasets.sgss.ingestion.sgss.config import Fields
from dsp.integration.mps.constants import SpineResponseCodes
from dsp.shared.pds.pds_discovery import get_pds_data
from dsp.model.pds_record import PDSRecord
from dsp.pipeline.models import PipelineContext, DataFrameInfo
from dsp.pipeline.pipeline import PipelineStage
from dsp.datasets.mps.request import Fields as Submitted
from dsp.datasets.mps.response import Fields as MPSResponse
from dsp.integration.mps.mps_schema import mps_request_schema_attributes, MPSResponseOrder, \
    mps_schema as MPS_SCHEMA
from dsp.shared.logger import log_action, add_fields


sgss_mps_schema = StructType(
    MPS_SCHEMA.fields + [
        StructField(Fields.PdsCrossCheckCondition, StringType(), True)
    ])

sgss_mps_response_schema_attributes = MPSResponseOrder + [Fields.PdsCrossCheckCondition]


def _add_cleaned_pds_fields(spark: SparkSession) -> DataFrame:
    df = get_pds_data(spark) \
        .withColumn(PDSCohortFields.GIVEN_NAME, col('name.givenNames').getItem(0)) \
        .withColumn(PDSCohortFields.FAMILY_NAME, col('name.familyName')) \
        .select(
        col(PDSCohortFields.BIRTH_DECADE),
        col(PDSCohortFields.NHS_NUMBER),
        col(PDSCohortFields.DATE_OF_BIRTH),
        col(PDSCohortFields.GIVEN_NAME),
        col(PDSCohortFields.FAMILY_NAME),
        col(PDSCohortFields.MOBILE_PHONE),
        col(PDSCohortFields.EMAIL_ADDRESS),
        col(PDSCohortFields.GENDER),
        col(PDSCohortFields.DATE_OF_DEATH),
        col(PDSCohortFields.REPLACED_BY),
        col(PDSCohortFields.RECORD)
    )
    df = cleaned_first_name(df, PDSCohortFields.GIVEN_NAME, '_given_name')
    df = cleaned_last_name(df, PDSCohortFields.FAMILY_NAME, '_family_name')
    df = cleaned_mobile_number(df, PDSCohortFields.MOBILE_PHONE, '_mobile_phone')
    df = cleaned_email(df, PDSCohortFields.EMAIL_ADDRESS, '_email')

    return df


def _add_cleaned_sgss_fields(df: DataFrame) -> DataFrame:
    df = cleaned_first_name(df, Submitted.GIVEN_NAME, '_given_name')
    df = cleaned_last_name(df, Submitted.FAMILY_NAME, '_family_name')
    df = cleaned_mobile_number(df, Submitted.MOBILE_NUMBER, '_mobile_number')
    df = cleaned_email(df, Submitted.EMAIL_ADDRESS, '_email')

    return df


@log_action()
def sgss_cross_check(
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

    df_pds_data = _add_cleaned_pds_fields(spark)
    df_mps_request = _add_cleaned_sgss_fields(df_mps_request)

    birth_decade_cond = (
        (floor(col(lhs(Submitted.DATE_OF_BIRTH)) / lit(100000)) * lit(10) == col(rhs(PDSCohortFields.BIRTH_DECADE)))
    )

    active_record_cond = ((col(rhs(PDSCohortFields.DATE_OF_DEATH)).isNull()) &
                          (col(rhs(PDSCohortFields.REPLACED_BY)).isNull()))
    gname_match_cond = (col(lhs('_given_name')) == col(rhs('_given_name')))
    gname_partial_match_cond = (
            substring(col(lhs('_given_name')), 1, 3) == substring(col(rhs('_given_name')), 1,
                                                                         3))
    fname_match_cond = (col(lhs('_family_name')) == col(rhs('_family_name')))
    dob_match_cond = (col(lhs(Submitted.DATE_OF_BIRTH)) == col(rhs(PDSCohortFields.DATE_OF_BIRTH)))
    gender_match_cond = (col(lhs(Submitted.GENDER)) == col(rhs('gender.gender')))
    mobno_match_cond = (col(lhs('_mobile_number')) == col(rhs('_mobile_phone')))
    email_match_cond = (col(lhs('_email')) == col(rhs('_email')))
    mobno_or_email_match_cond = (mobno_match_cond | email_match_cond)

    join_conditions = {
        'default': (
            (col(lhs(Submitted.NHS_NO)) == col(PDSCohortFields.NHS_NUMBER)) &
            (col(lhs(Submitted.DATE_OF_BIRTH)) == col(rhs(PDSCohortFields.DATE_OF_BIRTH)))
        ),
        'j': (
            active_record_cond &
            gname_match_cond & fname_match_cond & dob_match_cond & gender_match_cond & mobno_or_email_match_cond
        ),
        'k': (
            active_record_cond &
            gname_partial_match_cond & fname_match_cond & dob_match_cond & gender_match_cond & mobno_or_email_match_cond
        )
    }

    # set nhs number to null (unmatched) if there is more than a single match for each record
    dedup_window = Window.partitionBy(Submitted.UNIQUE_REFERENCE)

    df_joined = df_mps_request.alias('lhs') \
        .join(df_pds_data.alias('rhs'), on=(birth_decade_cond &
                                            reduce(ior, join_conditions.values())), how='left_outer')

    df_joined = df_joined.withColumn(PDSCohortFields.NHS_NUMBER,
                                     when(count(Submitted.UNIQUE_REFERENCE).over(dedup_window) > lit(1), lit(None))
                                     .otherwise(col(PDSCohortFields.NHS_NUMBER))) \
        .dropDuplicates([Submitted.UNIQUE_REFERENCE])

    join_conditions_case = create_cases(join_conditions).otherwise(lit(None))

    df_joined = df_joined.withColumn(Fields.PdsCrossCheckCondition, join_conditions_case)

    _do_cross_check_trace = partial(_sgss_do_cross_check_trace, mps_fields=df_mps_request.columns)

    rdd_matched = df_joined.rdd.map(_do_cross_check_trace)

    df_match_results = spark.createDataFrame(rdd_matched, schema=sgss_mps_schema)

    df_match_results.cache()

    df_matched = df_match_results.filter("MATCH = True")
    df_matched = df_matched.select([col(source) for source in sgss_mps_response_schema_attributes])
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


def _sgss_do_cross_check_trace(row: Row, mps_fields: List[str]) -> Row:
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

    return _generate_matched_mps_response(
        row[Submitted.AS_AT_DATE],
        row[Submitted.UNIQUE_REFERENCE],
        row[Submitted.LOCAL_PATIENT_ID],
        pds_record,
        additional_fields={Fields.PdsCrossCheckCondition: row[Fields.PdsCrossCheckCondition]}
    )


def _generate_unmatched_mps_request(row: Row, mps_fields: List[str], mps_schema=sgss_mps_schema):
    fields = {field: row[field] for field in mps_fields}

    unmatched_record = schema_fill_values(mps_schema, MATCH=False, **fields)

    return unmatched_record


def _generate_matched_mps_response(as_at_date: date, unique_reference: str, local_patient_id: str,
                                   pds_record: PDSRecord, mps_schema=sgss_mps_schema, additional_fields={}):
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


class SGSSPDSCrossCheckStage(PipelineStage):
    """
    Fires off a request to PDS aka local trace check, attempts an enrichment from PDS
    and returns an enriched dataframe as well as a dataframe of unmatched Referrals,
    which can potentially be enriched through MPS.
    """

    name = 'sgss_pds_cross_check'

    def __init__(
            self,
            trace_dataframe_name: str = "df",
            passthrough_dataframes: AbstractSet[str] = None,
            matched_output_dataframe_name: str = "df_matched",
            unmatched_output_dataframe_name: str = "df_unmatched"
    ):
        """
        Args:
            trace_dataframe_name (str): Name to give to the enriched dataframe output by this stage; default value "df"
            passthrough_dataframes (AbstractSet[str]): pass through
            matched_output_dataframe_name (str): Name to give to the matched dataframe by this stage;
                                                 default value "df_matched"
            unmatched_output_dataframe_name (str): Name to give to the unmatched dataframe by this stage;
                                                   default value "df_unmatched"
        """
        self._passthrough_dataframes = (
            frozenset(passthrough_dataframes) if passthrough_dataframes is not None else frozenset()
        )
        super().__init__(
            name=SGSSPDSCrossCheckStage.name,
            required_input_dataframes={trace_dataframe_name, *self._passthrough_dataframes},
            provided_output_dataframes={
                trace_dataframe_name, matched_output_dataframe_name,
                unmatched_output_dataframe_name, *self._passthrough_dataframes
            }
        )
        self._trace_dataframe_name = trace_dataframe_name
        self._matched_dataframe_name = matched_output_dataframe_name
        self._unmatched_dataframe_name = unmatched_output_dataframe_name

    def _run(self, spark: SparkSession, context: PipelineContext, *args: Any, **kwargs: Any) -> PipelineContext:
        df_records = context.dataframes[self._trace_dataframe_name].df
        df_mps_request = mps_request(context.primitives['dataset_id'])(df_records)
        df_matched, df_unmatched = sgss_cross_check(spark, df_mps_request)
        context.dataframes[self._matched_dataframe_name] = DataFrameInfo(df_matched)
        context.dataframes[self._unmatched_dataframe_name] = DataFrameInfo(df_unmatched)

        new_context = context.clone()
        new_context.dataframes = {
            **{
                self._trace_dataframe_name: context.dataframes[self._trace_dataframe_name],
                self._matched_dataframe_name: DataFrameInfo(df_matched),
                self._unmatched_dataframe_name: DataFrameInfo(df_unmatched)
            },
            **{df: context.dataframes[df] for df in self._passthrough_dataframes}
        }

        return new_context
