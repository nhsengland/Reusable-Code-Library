from typing import Any, Optional, Dict

import pytest

from dsp.udfs.coded_clinical_entry import (
    _is_a_valid_scheme_code,
    icd_10_format,
    read_code_v2_and_v3_format,
    snomed_ct_format,
    _DIAG_SCHEME_IN_USE,
    _DIAG_ICD_SNOMED_SCHEME_IN_USE,
    _FIND_SCHEME_IN_USE,
    _FIND_ICD_SNOMED_SCHEME_IN_USE,
    _OBS_SCHEME_IN_USE,
    _SITUATION_SCHEME_IN_USE,
    _PROCEDURE_SCHEME_IN_USE,
    _PROCEDURE_SCHEME_IN_USE_PLUS_STATUS
)


@pytest.mark.parametrize(['code', 'expected'], [
    ("W1E", False),
    ("MERGER", False),
    ("AYE!", False),
    ("91945568100", True),
    ("125001", True),
    ("81107", False),
    ("81945", False),
    ("AYE3432", False),
    ("12345657895454681545645005", False),
    ("756419843567154002", True),
])
def test_is_valid_snomed_ct_format_function(code: Optional[str], expected: Optional[bool]):
    smt_ct_format = snomed_ct_format()
    assert smt_ct_format(code) == expected


@pytest.mark.parametrize(['code', 'expected'], [
    ("A2RT3L", False),
    ("12345657895454681545645165", False),
    ("AYE3432", True),
    ("A23L", False),
    ("54681545645165", False),
    ("AY3432", False),
    ("A2RTL", True),
])
def test_is_valid_read_code_v2_and_v3_format(code: Optional[str], expected: Optional[bool]):
    read_code_format = read_code_v2_and_v3_format()
    assert read_code_format(code) == expected


@pytest.mark.parametrize(['code', 'expected'], [
    ("W1E", False),
    ("MERGER", True),
    ("AYE", False),
    ("A2R.T3", True),
    ("AYE3432", False),
    ("AY3432", True),
])
def test_is_valid_icd_10_format_function(code: Optional[str], expected: Optional[bool]):
    icd10_format = icd_10_format()
    assert icd10_format(code) == expected


@pytest.mark.parametrize(['scheme', 'code', 'validators_by_code', 'expected'], [
    ("456", "A", _DIAG_SCHEME_IN_USE, True),
    ("123", None, _DIAG_SCHEME_IN_USE, True),
    (None, "A", _DIAG_SCHEME_IN_USE, True),
    ("456", "A", _DIAG_ICD_SNOMED_SCHEME_IN_USE, True),
    ("123", None, _DIAG_ICD_SNOMED_SCHEME_IN_USE, True),
    (None, "A", _DIAG_ICD_SNOMED_SCHEME_IN_USE, True),
    ("456", "A", _FIND_SCHEME_IN_USE, True),
    ("123", None, _FIND_SCHEME_IN_USE, True),
    (None, "A", _FIND_SCHEME_IN_USE, True),
    ("456", "A", _FIND_ICD_SNOMED_SCHEME_IN_USE, True),
    ("123", None, _FIND_ICD_SNOMED_SCHEME_IN_USE, True),
    (None, "A", _FIND_ICD_SNOMED_SCHEME_IN_USE, True),
    ("456", "A", _OBS_SCHEME_IN_USE, True),
    ("123", None, _OBS_SCHEME_IN_USE, True),
    (None, "A", _OBS_SCHEME_IN_USE, True),
    ("06", "dhhw", _SITUATION_SCHEME_IN_USE, True),
    ("04", None, _SITUATION_SCHEME_IN_USE, True),
    ("02", "MERGER", _DIAG_SCHEME_IN_USE, True),
    ("04", "46534", _DIAG_SCHEME_IN_USE, True),
    ("05", "D3tBA53", _DIAG_SCHEME_IN_USE, True),
    ("06", "1234565789545468155", _DIAG_SCHEME_IN_USE, False),
    ("02", "MERGER", _DIAG_ICD_SNOMED_SCHEME_IN_USE, True),
    ("06", "1234565789545468155", _DIAG_ICD_SNOMED_SCHEME_IN_USE, False),
    ("01", "ERGE", _FIND_SCHEME_IN_USE, True),
    ("02", "UUID", _FIND_SCHEME_IN_USE, False),
    ("03", "Kenneth1", _FIND_SCHEME_IN_USE, False),
    ("04", "12345", _FIND_SCHEME_IN_USE, False),
    ("01", "ABCD", _FIND_ICD_SNOMED_SCHEME_IN_USE, True),
    ("04", "12345", _FIND_ICD_SNOMED_SCHEME_IN_USE, False),
    ("01", "BRAKE", _OBS_SCHEME_IN_USE, True),
    ("02", "BrAkErS", _OBS_SCHEME_IN_USE, True),
    ("03", "123456578103", _OBS_SCHEME_IN_USE, True),
    ("01", "ERGE", _SITUATION_SCHEME_IN_USE, True),
    ("02", "UUID", _SITUATION_SCHEME_IN_USE, False),
    ("03", "Kenneth1", _SITUATION_SCHEME_IN_USE, False),
    ("04", "12345", _SITUATION_SCHEME_IN_USE, False),
    ("02", "ERGE", _PROCEDURE_SCHEME_IN_USE_PLUS_STATUS, True),
    ("02", "9696", _PROCEDURE_SCHEME_IN_USE_PLUS_STATUS, True),
    ("04", "Kenneth1", _PROCEDURE_SCHEME_IN_USE_PLUS_STATUS, False),
    ("06", "1104631000000101:363589002=26604007", _PROCEDURE_SCHEME_IN_USE_PLUS_STATUS, True),
    ("06", "1104631000000101", _PROCEDURE_SCHEME_IN_USE_PLUS_STATUS, True),
    ("06", "26604007", _PROCEDURE_SCHEME_IN_USE_PLUS_STATUS, True),
    ("06", "855866084077057683780171616509601376341981531879639840091", _PROCEDURE_SCHEME_IN_USE_PLUS_STATUS, False),
    ("06", "1104631000000101:VALID=26604007", _PROCEDURE_SCHEME_IN_USE_PLUS_STATUS, True),
    ("06", "VALID", _PROCEDURE_SCHEME_IN_USE_PLUS_STATUS, False),
    ("04", "Kenneth1", _PROCEDURE_SCHEME_IN_USE, False),
    ("06", "855866084077050101", _PROCEDURE_SCHEME_IN_USE, True),
    ("06", "8558660840770576837", _PROCEDURE_SCHEME_IN_USE, False),

])
def test_is_is_a_valid_scheme_code(scheme: Optional[str], code: Optional[str],
                                   validators_by_code: Dict[str, Any], expected: Optional[bool]):
    assert _is_a_valid_scheme_code(scheme, code, validators_by_code) == expected
