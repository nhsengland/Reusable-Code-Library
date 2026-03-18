from collections import namedtuple
from functools import reduce
from operator import getitem
from typing import Iterable, Tuple, Hashable, Union, Dict, MutableMapping, Callable, List, Any

from pyspark import RDD, Row
from pyspark.sql import SparkSession, DataFrame, Column

from dsp.common import canonical_name
from dsp.datasets.common import Fields as CommonFields
from dsp.enrichments.mps.response import Fields as MpsResponseFields
from dsp.enrichments.mps.constants import (
    RESULT_INDICATING_NHS_NUMBERS
)
from dsp.common.model_accessor import ModelPath
from dsp.shared.common import base36encode

MergeKey = Union[str, ModelPath]
MergeKeys = Iterable[MergeKey]

PATH_SEPARATOR = '.'

EnrichedField = namedtuple('EnrichedField', 'left_path, right_path, strategy')


def generate_unique_ids(submission_id: int, rdd: RDD):
    rdd_with_index = rdd.zipWithIndex()

    def mapper(row_and_index: Tuple[Row, int]):
        row, index = row_and_index
        row_values = row.asDict()
        submission_and_index = str(submission_id) + str(index).zfill(7)
        uniqid_base36 = 'U' + base36encode(int(submission_and_index)).zfill(9)
        row_values[CommonFields.UNIQ_ID] = uniqid_base36
        return Row(**row_values)

    return rdd_with_index.map(mapper)


def get_mps_confidence_dict(mps_response_row: Row) -> Dict:
    return {
        'MatchedAlgorithmIndicator': mps_response_row[MpsResponseFields.MATCHED_ALGORITHM_INDICATOR],
        'MatchedConfidencePercentage': mps_response_row[MpsResponseFields.MATCHED_CONFIDENCE_PERCENTAGE],
        'FamilyNameScorePercentage': mps_response_row[MpsResponseFields.ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC],
        'GivenNameScorePercentage': mps_response_row[MpsResponseFields.ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC],
        'DateOfBirthScorePercentage': mps_response_row[MpsResponseFields.ALGORITHMIC_TRACE_DOB_SCORE_PERC],
        'GenderScorePercentage': mps_response_row[MpsResponseFields.ALGORITHMIC_TRACE_GENDER_SCORE_PERC],
        'PostcodeScorePercentage': mps_response_row[MpsResponseFields.ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC],
    }


def _is_valid_nhs_no(nhs_no: str):
    return nhs_no and nhs_no not in RESULT_INDICATING_NHS_NUMBERS


def get_person_id(mps_response_row: Row) -> str:
    nhs_no = mps_response_row[MpsResponseFields.MATCHED_NHS_NO]
    mps_id = mps_response_row[MpsResponseFields.MPS_ID]
    uniq_id = mps_response_row[CommonFields.UNIQ_ID]

    if _is_valid_nhs_no(nhs_no):
        return nhs_no

    if mps_id:
        return mps_id

    return uniq_id


def prepare_for_join(rdd: RDD, merge_keys: MergeKeys) -> RDD:
    merge_key_split = [str(k).split(PATH_SEPARATOR) for k in merge_keys]

    def _map(row: Row) -> Tuple[Tuple[Hashable], Row]:
        if not isinstance(row, dict):
            row = row.asDict(True)
        merge_key_values = tuple([str(reduce(getitem, merge_key, row)) for merge_key in merge_key_split])  # type: Tuple
        return merge_key_values, row

    return rdd.map(_map)


def join(
        left_rdd: RDD,
        right_rdd: RDD,
        merge_keys_left: MergeKeys,
        merge_keys_right: MergeKeys,
) -> RDD:
    left_rdd = prepare_for_join(left_rdd, merge_keys_left)
    right_rdd = prepare_for_join(right_rdd, merge_keys_right)
    return left_rdd.leftOuterJoin(right_rdd)


def enrich_with_meta(
        _spark: SparkSession, records_df: DataFrame, metadata: MutableMapping[str, Any],
        enrichments: List[Tuple[str, Callable[[MutableMapping[str, Any]], Column]]]
) -> DataFrame:
    for col_name, add_from_metadata in enrichments:
        c_name = canonical_name(col_name)

        records_df = records_df.withColumn(c_name, add_from_metadata(metadata))

    return records_df


def get_mps_sensitivity(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.SENSITIVTY_FLAG]


def get_mps_match_algorithm_indicator(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.MATCHED_ALGORITHM_INDICATOR]


def get_mps_match_confidence_percentage(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.MATCHED_CONFIDENCE_PERCENTAGE]


def get_mps_address_line1(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.ADDRESS_LINE1]


def get_mps_address_line2(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.ADDRESS_LINE2]


def get_mps_address_line3(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.ADDRESS_LINE3]


def get_mps_address_line4(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.ADDRESS_LINE4]


def get_mps_postcode(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.POSTCODE]


def get_mps_date_of_death(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.DATE_OF_DEATH]


def get_mps_family_name(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.FAMILY_NAME]


def get_mps_given_name(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.GIVEN_NAME]


def get_mps_mobile_number(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.MOBILE_NUMBER]


def get_mps_email_address(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.EMAIL_ADDRESS]


def get_mps_matched_nhs_no(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.MATCHED_NHS_NO]


def get_valid_mps_matched_nhs_no(mps_response_row: Row) -> str:
    nhs_no = get_mps_matched_nhs_no(mps_response_row)

    if _is_valid_nhs_no(nhs_no):
        return nhs_no

    return None


def get_mps_date_of_birth(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.DATE_OF_BIRTH]


def get_mps_gp_practice_code(mps_response_row: Row) -> str:
    return mps_response_row[MpsResponseFields.GP_PRACTICE_CODE]
