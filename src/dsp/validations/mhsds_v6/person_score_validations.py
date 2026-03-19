from typing import Optional

from nhs_reusable_code_library.resuable_codes.spark_helpers import typed_udf
from dsp.validations.mhsds_v6.assessment_scales import ASSESSMENT_SCALES
from dsp.validations.common import is_valid_code, is_valid_score
from pyspark.sql import Column
from pyspark.sql.types import BooleanType

_MH_ASSESSMENT_TOOLS = ASSESSMENT_SCALES

def is_valid_assessment_scale_code(code_col: Column) -> Column:
    """
    Check whether a given SNOMED code is acceptable for tables MHS606, MHS607 and MHS608

    Args:
        code_col (Column): The column containing SNOMED values

    Returns:
        Column: A boolean column for whether the given column contains a valid value
    """
    return is_valid_code(code_col, _MH_ASSESSMENT_TOOLS)


@typed_udf(BooleanType())
def is_valid_assessment_scale_score(snomed_code: str, score: str) -> Optional[bool]:
    """
    A pyspark UDF for determining whether a submitted score is valid for a given code, for tables MHS606, MHS607 and
    MHS608

    Args:
        snomed_code (str): The submitted SNOMED code for the assessment being scored
        score (str): The score received for the assessment

    Returns:
        bool: Whether the given score is valid for the assessment
    """
    return is_valid_score(snomed_code, score, _MH_ASSESSMENT_TOOLS)
