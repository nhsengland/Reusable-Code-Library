from typing import Any, Dict, List
from functools import reduce
from operator import ior
from botocore.errorfactory import ClientError

# noinspection PyPackageRequirements
from pyspark.sql import DataFrame, Column
# noinspection PyPackageRequirements
from pyspark.sql.functions import broadcast, col, concat, lit, regexp_replace, split, substring as pysubstring, upper, \
    when as pswhen, to_timestamp, substring
from pyspark.sql.types import StringType

from dsp.datasets.postcode.extract_constants import DeltaExtractCategory
from dsp.datasets.postcode.npex_extract import Fields as NpexOutputFields
from dsp.datasets.postcode.shared.util import clean_email, clean_uppercase_transform, clean_mobile_number, \
    clean_gender
from dsp.shared.aws import get_feature_toggle


def get_matchdate_dataframe(df: DataFrame, match_date_column: str) -> DataFrame:
    match_date_df = df.withColumn(
        match_date_column,
        pswhen(col('AppointmentDate').isNotNull(),
               col('AppointmentDate')).otherwise(col('TestStartDate'))
    )

    return match_date_df


def get_df_with_postcode_countries(match_date_df, refdata, match_date_column, ref_data_columns, postcode_field):
    df_with_postcode_countries = match_date_df.alias('lhs').join(
        refdata.alias('rhs'),
        (col(f'lhs.{postcode_field}') == col('rhs.pcds')) &
        ((col('rhs.record_start_date') <= col(match_date_column)) &
         ((col('rhs.record_end_date') >= col(match_date_column)) | col('rhs.record_end_date').isNull())),
        how='left').select(*(['lhs.*'] + ref_data_columns)).drop(match_date_column)
    return df_with_postcode_countries


def get_df_with_countries_converted(spark, df_with_postcode_countries, country_code, country_field):
    country_code_mapping = generate_country_code_mapping(spark, country_field)

    df_with_postcode_countries_converted = df_with_postcode_countries.alias('lhs').join(
        broadcast(country_code_mapping.alias('rhs')), col(f'lhs.{country_code}') == col('rhs.Code'),
        how='left'
    ).select('lhs.*', f'rhs.{country_field}')

    return df_with_postcode_countries_converted


def generate_country_code_mapping(spark, country_field) -> DataFrame:
    values = [("E92000001", DeltaExtractCategory.ENGLAND.value),
              ("S92000003", DeltaExtractCategory.SCOTLAND.value),
              ("W92000004", DeltaExtractCategory.WALES.value),
              ("N92000002", DeltaExtractCategory.NORTHERN_IRELAND.value), ]
    columns = ['Code', country_field]
    return spark.createDataFrame(values, columns)


def get_delta_extract_countries(toggle_prefix: str,
                                country_categories_list: List[DeltaExtractCategory]) -> List[DeltaExtractCategory]:
    country_categories = []

    for category in country_categories_list:
        toggle = False
        split_toggle = False

        try:
            toggle = get_feature_toggle(
                f'{toggle_prefix}_send_delta_extract_{category.value.lower().replace(" ", "_")}')
        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                toggle = False

        if toggle:
            try:
                split_toggle = get_feature_toggle(
                    f'{toggle_prefix}_separate_lft_tests_delta_extract_{category.value.lower().replace(" ", "_")}'
                )
            except ClientError as e:
                if e.response['Error']['Code'] == 'ParameterNotFound':
                    split_toggle = False
            if split_toggle:
                country_categories += [
                    _country for _country in country_categories_list
                    if _country.value in [f"{category.value}_LFT", f"{category.value}_non_LFT"]
                ]
            else:
                country_categories.append(category)

    return country_categories


def normalise_postcode(x: str): return pswhen(col(x) == lit(''), None).otherwise(
    concat(
        split(
            upper(regexp_replace(col(x), " ", "")), ".{3}$").getItem(0),
        lit(' '),
        pysubstring(
            upper(regexp_replace(col(x), " ", "")), -3, 3),
    ))


def normalised_postcode(df, postcode, normalised_postcode_column):
    df_formatted_postcodes = df.withColumn(normalised_postcode_column, normalise_postcode(postcode))

    return df_formatted_postcodes


def any_field_contains_linebreak(df: DataFrame):
    conds = []
    for col_name in df.columns:
        if type(df.schema[col_name].dataType) is StringType:
            conds.append(col(col_name).isNotNull() & col(col_name).contains('\n'))
    return reduce(ior, conds)


def remove_rows_containing_linebreaks(df: DataFrame) -> DataFrame:
    return df.where(~any_field_contains_linebreak(df))


def create_cases(mapping: Dict[Any, Column]) -> Column:
    result = None
    for case_then, case_when in mapping.items():
        if result is None:  # first
            result = pswhen(case_when, lit(case_then))
        else:  # rest
            result = result.when(case_when, lit(case_then))
    return result


def keystone_pds_matched_flag_filter():
    return ((col(NpexOutputFields.MPSConfidence)['MatchedAlgorithmIndicator'] == '3') |
            (
                    (
                            (col(NpexOutputFields.MPSConfidence)[
                                 'MatchedAlgorithmIndicator'] == '1') |
                            (col(NpexOutputFields.MPSConfidence)[
                                 'MatchedAlgorithmIndicator'] == '4')
                    ) &
                    (
                            (col(NpexOutputFields.FirstName).isNotNull()) &
                            (col(NpexOutputFields.FirstName) != '') &
                            (substring(
                                clean_uppercase_transform(NpexOutputFields.FirstName), 1,
                                3) ==
                             substring(
                                 clean_uppercase_transform(NpexOutputFields.MPSFirstName),
                                 1, 3))
                    ) &
                    (
                            (col(NpexOutputFields.LastName).isNotNull()) &
                            (col(NpexOutputFields.LastName) != '') &
                            (clean_uppercase_transform(NpexOutputFields.LastName) ==
                             clean_uppercase_transform(NpexOutputFields.MPSLastName))
                    ) &
                    (
                            ((col(NpexOutputFields.Gender) == 'Male') | (
                                    col(NpexOutputFields.Gender) == 'Female')) &
                            (clean_gender(NpexOutputFields.Gender) == col(
                                NpexOutputFields.MPSGender))
                    ) &
                    (
                            (to_timestamp(NpexOutputFields.DateOfBirth,
                                          'yyyy-MM-dd').isNotNull()) &
                            (to_timestamp(NpexOutputFields.DateOfBirth,
                                          'yyyy-MM-dd') == col(
                                NpexOutputFields.MPSDateOfBirth))
                    ) &
                    (
                            (
                                    (col(NpexOutputFields.NormalisedPostcode).isNotNull()) &
                                    (col(NpexOutputFields.NormalisedPostcode) != '') &
                                    ((col(NpexOutputFields.NormalisedPostcode) ==
                                      normalise_postcode(NpexOutputFields.MPSPostcode)) |
                                     (col(NpexOutputFields.MPSConfidence)[
                                          'PostcodeScorePercentage'] == lit(100.00)))
                            ) |
                            (
                                    (col(NpexOutputFields.EmailAddress).isNotNull()) &
                                    (col(NpexOutputFields.EmailAddress) != '') &
                                    (clean_email(
                                        NpexOutputFields.EmailAddress) == clean_email(
                                        NpexOutputFields.MPSEmailAddress))
                            ) |
                            (
                                    (col(NpexOutputFields.MobileNumber).isNotNull()) &
                                    (col(NpexOutputFields.MobileNumber) != '') &
                                    (clean_mobile_number(
                                        NpexOutputFields.MobileNumber) == clean_mobile_number(
                                        NpexOutputFields.MPSMobileNumber))
                            )
                    )
            ))
