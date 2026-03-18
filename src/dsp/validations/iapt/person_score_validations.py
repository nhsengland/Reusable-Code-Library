from typing import List, Mapping, Optional

from nhs_reusable_code_library.resuable_codes.spark_helpers import typed_udf
from dsp.validations.iapt.assessment_scales import ASSESSMENT_SCALES
from dsp.validations.iapt.person_score_validators import PersonScoreValidator
from dsp.validations.common import AssessmentToolName
from pyspark.sql import Column
from pyspark.sql.types import BooleanType


def is_valid_assessment_scale_code(code_col: Column, assessment_tool_names: List[AssessmentToolName] = None) -> Column:
    """
    Check whether a given SNOMED code is acceptable for tables IDS606 and IDS607

    Args:
        :param code_col:  The column containing SNOMED values
        :param assessment_tool_names:
    Returns:
        Column: A boolean column for whether the given column contains a valid value
    """
    return _is_valid_code(code_col, ASSESSMENT_SCALES, assessment_tool_names)


@typed_udf(BooleanType())
def is_valid_score_format_for_snomed(
        snomed_code: str, score: str) -> Optional[bool]:
    if score is None or snomed_code is None or snomed_code not in ASSESSMENT_SCALES:
        return True

    validator = ASSESSMENT_SCALES[snomed_code]
    return validator.is_valid_format(score)


@typed_udf(BooleanType())
def is_valid_assessment_scale_score(snomed_code: str, score: str) -> Optional[bool]:
    """
    A pyspark UDF for determining whether a submitted score is valid for a given code, for tables IDS606, IDS607

    Args:
        snomed_code (str): The submitted SNOMED code for the assessment being scored
        score (str): The score received for the assessment

    Returns:
        bool: Whether the given score is valid for the assessment
    """
    return _is_valid_score(snomed_code, score, ASSESSMENT_SCALES)


def _is_valid_code(
        snomed_code_column: Column, validators_by_code: Mapping[str, PersonScoreValidator],
        assessment_tool_names: List[AssessmentToolName] = None) -> Column:

    if assessment_tool_names:
        updated_iapt_assessment_tools = {}
        for k, v in ASSESSMENT_SCALES.items():
            if v.assessment_tool_name in assessment_tool_names:
                updated_iapt_assessment_tools[k] = v

        validators_by_code = updated_iapt_assessment_tools

    return snomed_code_column.isNull() | snomed_code_column.isin(list(validators_by_code.keys()))


def _is_valid_score(snomed_code: Optional[str], score: Optional[str],
                    validators_by_code: Mapping[str, PersonScoreValidator]) -> Optional[bool]:
    if score is None or snomed_code is None or snomed_code not in validators_by_code:
        return True

    validator = validators_by_code[snomed_code]
    return validator.validate_person_score(score)
