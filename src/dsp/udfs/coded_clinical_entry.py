from types import LambdaType
from typing import Optional, Mapping, Any

from dsp.udfs.misc import is_valid_snomed_ct


def icd_10_format() -> LambdaType:
    """
    ICD 10 function with minimum and maximum expected alphanumeric format length
    Return:
         function: ICD 10 lambda function
    """
    return lambda x: (4 <= len(x) <= 6)


def opcs_4_format() -> LambdaType:
    """
    OPCS-4 function with minimum and maximum expected alphanumeric format length
    Return:
         function: OPCS-4 lambda function
    """
    return lambda x: len(x) == 4


def read_code_v2_and_v3_format() -> LambdaType:
    """
    READ CODE v2 and v3 function with two expected format length
    Return:
         function: READ CODE lambda function
    """
    return lambda x: (len(x) == 5) | (len(x) == 7)


def snomed_ct_format(upper_bound: int = 18) -> LambdaType:
    """
    Args:
        upper_bound: eg some MSDS fields require snomed validation but can be up to 56 chars according to TOS
    SNOMED CT function with minimum and maximum expected numeric format length
    Return:
         function: SNOMED CT lambda function
    """
    return lambda x: (x.isnumeric()) and (6 <= len(x) <= upper_bound)


def alphanumeric_snomed_ct_format(upper_bound: int = 56) -> LambdaType:
    """
    Args:
        upper_bound: eg Procedure schema and status MSDS fields require snomed code to be min 6 and max 56 chars

    Return:
         function: SNOMED CT lambda function
    """
    return lambda x: (6 <= len(x) <= upper_bound)


def extended_snomed_ct_format() -> LambdaType:
    """
    SNOMED CT function with minimum and maximum expected numeric format length , partition and verhoeff check
    Return:
         function: SNOMED CT lambda function
    """
    return lambda x: (x.isnumeric()) and (6 <= len(x) <= 18 and is_valid_snomed_ct(x,6,18))


_DIAG_ICD_SNOMED_SCHEME_IN_USE = {"02": icd_10_format(),
                             "06": extended_snomed_ct_format()
                                  }

_DIAG_SCHEME_IN_USE = {"02": icd_10_format(),
                       "04": read_code_v2_and_v3_format(),
                       "05": read_code_v2_and_v3_format(),
                       "06": snomed_ct_format()
                       }

_FIND_ICD_SNOMED_SCHEME_IN_USE = {"01": icd_10_format(),
                             "04": extended_snomed_ct_format()
                             }

_FIND_SCHEME_IN_USE = {"01": icd_10_format(),
                       "02": read_code_v2_and_v3_format(),
                       "03": read_code_v2_and_v3_format(),
                       "04": snomed_ct_format()
                       }

_OBS_SCHEME_IN_USE = {"01": read_code_v2_and_v3_format(),
                      "02": read_code_v2_and_v3_format(),
                      "03": snomed_ct_format()
                      }

_SITUATION_SCHEME_IN_USE = {"01": icd_10_format(),
                            "02": read_code_v2_and_v3_format(),
                            "03": read_code_v2_and_v3_format(),
                            "04": snomed_ct_format()
                            }

_PROCEDURE_SCHEME_IN_USE_PLUS_STATUS = {"02": opcs_4_format(),
                                        "04": read_code_v2_and_v3_format(),
                                        "05": read_code_v2_and_v3_format(),
                                        "06": alphanumeric_snomed_ct_format()
                                        }

_PROCEDURE_SCHEME_IN_USE = {"04": read_code_v2_and_v3_format(),
                            "05": read_code_v2_and_v3_format(),
                            "06": snomed_ct_format()
                            }


def is_valid_find_code(scheme: Optional[str], code: Optional[str]) -> Optional[bool]:
    """
        A pyspark UDF for determining whether a submitted code is valid for FINDING SCHEME IN USE

        Args:
            scheme (str): The submitted scheme code for the assessment being scored
            code (str): The score received for the assessment

        Returns:
            bool: Whether the given score is valid for the assessment
        """
    return _is_a_valid_scheme_code(scheme, code, _FIND_SCHEME_IN_USE)


def is_valid_icd_snomed_find_code(scheme: Optional[str], code: Optional[str]) -> Optional[bool]:
    """
        A pyspark UDF for determining whether a submitted code is valid for FINDING SCHEME IN USE for MHSDS and IAPT

        Args:
            scheme (str): The submitted scheme code for the assessment being scored
            code (str): The score received for the assessment

        Returns:
            bool: Whether the given score is valid for the assessment
        """
    return _is_a_valid_scheme_code(scheme, code, _FIND_ICD_SNOMED_SCHEME_IN_USE)


def is_valid_diag_code(scheme: Optional[str], code: Optional[str]) -> Optional[bool]:
    """
        A pyspark UDF for determining whether a submitted code is valid for DIAGNOSIS SCHEME IN USE

        Args:
            scheme (str): The submitted scheme code for the assessment being scored
            code (str): The score received for the assessment
        """
    return _is_a_valid_scheme_code(scheme, code, _DIAG_SCHEME_IN_USE)


def is_valid_icd_snomed_diag_code(scheme: Optional[str], code: Optional[str]) -> Optional[bool]:
    """
        A pyspark UDF for determining whether a submitted code is valid for DIAGNOSIS SCHEME IN USE for MHSDS and IAPT

        Args:
            scheme (str): The submitted scheme code for the assessment being scored
            code (str): The score received for the assessment
        """
    return _is_a_valid_scheme_code(scheme, code, _DIAG_ICD_SNOMED_SCHEME_IN_USE)


def is_valid_observation_code(scheme: Optional[str], code: Optional[str]) -> Optional[bool]:
    """
        A pyspark UDF for determining whether a submitted code is valid for OBSERVATION SCHEME IN USE

        Args:
            scheme (str): The submitted scheme code for the assessment being scored
            code (str): The score received for the assessment
        """
    return _is_a_valid_scheme_code(scheme, code, _OBS_SCHEME_IN_USE)


def is_valid_situation_code(scheme: Optional[str], code: Optional[str]) -> Optional[bool]:
    """
        A pyspark UDF for determining whether a submitted code is valid for SITUATION SCHEME IN USE

        Args:
            scheme (str): The submitted scheme code for the assessment being scored
            code (str): The score received for the assessment
        """
    return _is_a_valid_scheme_code(scheme, code, _SITUATION_SCHEME_IN_USE)


def is_valid_procedure_and_procedure_status_code(scheme: Optional[str], code: Optional[str]) -> Optional[bool]:
    """
        A pyspark UDF for determining whether a submitted code is valid for PROCEDURE SCHEME IN USE PLUS STATUS
        (as used in, for example,  MSDS M202060 CODED PROCEDURE AND PROCEDURE STATUS (CODED CLINICAL ENTRY))

        Args:
            scheme (str): The submitted scheme code for the procedure
            code (str): The procedure code
        """
    return _is_a_valid_scheme_code(scheme, code, _PROCEDURE_SCHEME_IN_USE_PLUS_STATUS)


def is_valid_procedure_code(scheme: Optional[str], code: Optional[str]) -> Optional[bool]:
    """
        A pyspark UDF for determining whether a submitted code is valid for PROCEDURE SCHEME IN USE

        Args:
            scheme (str): The submitted scheme code for the procedure
            code (str): The procedure code
        """
    return _is_a_valid_scheme_code(scheme, code, _PROCEDURE_SCHEME_IN_USE)


def _is_a_valid_scheme_code(scheme: Optional[str], code: str, validators_by_code: Mapping[str, Any]) -> Optional[bool]:
    """
    Retrieve scheme rule by column name and code
    Args:
        scheme (str): The scheme name
        code (str): code to be validated
        validators_by_code (Union[str, Column]): The table column to be validated
    Return:
        bool: A scheme instance representing the given parameters
    """
    scheme_rule = validators_by_code.get(scheme)
    if (code is None) or (scheme_rule is None):
        return True
    return scheme_rule(code)
