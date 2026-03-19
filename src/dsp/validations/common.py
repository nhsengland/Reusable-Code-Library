import datetime
import hashlib
import os
import re
import warnings
from collections import namedtuple
from difflib import unified_diff
from enum import Enum
from typing import Container, List, Union, Tuple, Mapping, Optional
from abc import ABC, abstractmethod
from pyspark.sql import Column

with warnings.catch_warnings():
    from defusedxml import __version__ as _dxml_version

    # Using pinned version, this can be safely ignored.
    if _dxml_version == "0.7.1":
        warnings.simplefilter("ignore", category=DeprecationWarning)

    import defusedxml.lxml as etree

import pkg_resources
import json
from jaydebeapi import Cursor
from lxml.etree import XMLSchema, DocumentInvalid, ParseError

from dsp.shared.common.static_resource_helpers import extract_package_resources_file
from dsp.shared.aws import local_mode
from dsp.shared.common.test_helpers import smart_open
from dsp.shared.constants import DS
from dsp.shared.logger import log_action, add_fields

SchemaRow = namedtuple('SchemaRow', ['table_name', 'column_name', 'data_type', 'character_maximum_length'])

XSD_PATHS = {
    DS.MSDS: ('testdata/msds_test_data/schema/MSDSMSDS_XMLSchema-v2-0.xsd', 'testdata/msds_test_data/schema/MSDS_XMLDataTypes-v2-0.xsd'),
    #DS.CSDS: ['csds/schema/CSDSv1_5_0_Final.xsd'],
    DS.CSDS_V1_6: ['testdata/csds_test_data/csds_v1_6/schema/CSDSv1_6_Final.XSD'],
    DS.DIDS: ('testdata/dids_test_data/schema/DIDS_XMLSchema-v1-0.xsd', 'testdata/dids_test_data/schema/DIDS_XMLDataTypes-v1-0.xsd')
}

ISO639_1_LANG_CODES = {
    'aa', 'ab', 'ae', 'af', 'ak', 'am', 'an', 'ar', 'as', 'av', 'ay', 'az', 'ba', 'be', 'bg', 'bh', 'bi', 'bm', 'bn',
    'bo', 'bo', 'br', 'bs', 'ca', 'ce', 'ch', 'co', 'cr', 'cs', 'cu', 'cv', 'cy', 'da', 'de', 'dv', 'dz', 'ee', 'el',
    'en', 'eo', 'es', 'et', 'eu', 'fa', 'ff', 'fi', 'fj', 'fo', 'fr', 'fy', 'ga', 'gd', 'gl', 'gn', 'gu', 'gv', 'ha',
    'he', 'hi', 'ho', 'hr', 'ht', 'hu', 'hy', 'hz', 'ia', 'id', 'ie', 'ig', 'ii', 'ik', 'io', 'is', 'it', 'iu', 'ja',
    'jv', 'ka', 'ka', 'kg', 'ki', 'kj', 'kk', 'kl', 'km', 'kn', 'ko', 'kr', 'ks', 'ku', 'kv', 'kw', 'ky', 'la', 'lb',
    'lg', 'li', 'ln', 'lo', 'lt', 'lu', 'lv', 'mg', 'mh', 'mi', 'mk', 'ml', 'mn', 'mr', 'ms', 'mt', 'my', 'na', 'nb',
    'nd', 'ne', 'ng', 'nl', 'nn', 'no', 'nr', 'nv', 'ny', 'oc', 'oj', 'om', 'or', 'os', 'pa', 'pi', 'pl', 'ps', 'pt',
    'qu', 'rm', 'rn', 'ro', 'ru', 'rw', 'sa', 'sc', 'sd', 'se', 'sg', 'si', 'sk', 'sk', 'sl', 'sm', 'sn', 'so', 'sq',
    'sr', 'ss', 'st', 'su', 'sv', 'sw', 'ta', 'te', 'tg', 'th', 'ti', 'tk', 'tl', 'tn', 'to', 'tr', 'ts', 'tt', 'tw',
    'ty', 'ug', 'uk', 'ur', 'uz', 've', 'vi', 'vo', 'wa', 'wo', 'xh', 'yi', 'yo', 'za', 'zh', 'zu',
}

XML_RESERVED_CHARS = {
    '"': "&#34; doublequote",
    "&": "&#38; ampersand",
    "'": "&#39; apostrophe",
    "<": "&#60; lessthan",
    ">": "&#62; greaterthan",
}


class AssessmentToolName(Enum):
    BIQ = 'Body Image Questionnaire'
    BPI = 'Brief Pain Inventory'
    CFQ11 = 'Chalder Fatigue Scale'
    CAT = 'COPD Assessment Test'
    DDS = 'Diabetes Distress Scale'
    GAD7 = 'Generalised Anxiety Disorder 7'
    HAI = 'Health Anxiety Inventory  - Week'
    T_PEQ = 'IAPT Treatment Patient Experience Questionnaire'
    A_PEQ = 'IAPT Assessment Patient Experience Questionnaire'
    IPCQ = 'iMTA Productivity Cost Questionnaire'
    IBS_SSS = 'Irritable Bowel Syndrome - Symptom Severity Scale score'
    MI = 'Mobility Inventory  for Agoraphobia'
    OCI = 'Obsessive Compulsive Inventory'
    PDSS = 'Panic Disorder Severity Scale'
    PHQ9 = 'Patient Health Questionnaire-9'
    PHQ15 = 'Patient Health Questionnaire-15'
    PSWQ = 'Penn State Worry Questionnaire'
    PCL5 = 'PTSD  checklist for DSM-5'
    SPIN = 'Social Phobia Inventory'
    WSAS = 'Work And Social Adjustment Scale'


@log_action(log_args=['path', 'dataset_id'])
def xml_schema_is_valid(
        path: str,
        dataset_id: str,
        zipped: bool = False,
        primitive_path: str = None,
        validate_filetype: bool = False
) -> Tuple[bool, str]:

    xsd_paths = XSD_PATHS[dataset_id]

    xsd_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, xsd_paths[0]))

    if not local_mode():
        extracted_paths = [extract_package_resources_file(os.path.join('datasets', p), '/tmp') for p in xsd_paths]

        xsd_path = extracted_paths[0]

    if validate_filetype:
        path_to_validate = primitive_path or path
        if not re.match(r"^.+\.xml$|^.+\.idb$", path_to_validate.lower()):
            return False, "Only .idb or .xml file extensions permitted."

    with open(xsd_path, 'rb') as schema_file:

        tree = etree.parse(schema_file)
        schema = XMLSchema(tree)

        with smart_open(path, 'rb', zipped=zipped) as content_file:
            try:
                xml_doc = etree.parse(content_file)
            except ParseError as e:
                add_fields(xml_parsing_failed=e.args[0])
                return False, e.args[0]

            for element in xml_doc.iter():
                if element.text in XML_RESERVED_CHARS:
                    char_name = XML_RESERVED_CHARS[element.text]
                    return False, f'Field named {element.tag} on line {element.sourceline} ' \
                                  f'contains the XML reserved char {char_name}.'

            try:
                schema.assertValid(xml_doc)
            except DocumentInvalid as e:
                add_fields(invalid_xml_desc=e.args[0])
                return False, e.args[0]

    return True, None


@log_action(log_args=['dataset_id'])
def accessdb_schema_is_valid(cursor: Cursor, dataset_id: str) -> Tuple[bool, str]:
    cursor.execute("SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH "
                   "FROM information_schema.columns "
                   "WHERE TABLE_SCHEMA = 'PUBLIC'"
                   "ORDER BY TABLE_NAME, COLUMN_NAME")

    actual_rows = cursor.fetchall()
    actual_rows = sorted([SchemaRow(*row) for row in actual_rows])
    actual_hash = _get_schema_hash(actual_rows)

    expected_rows = sorted(_load_idb_rows(f'{dataset_id}/schema/idb_schema.json'))
    expected_hash = _get_schema_hash(expected_rows)

    is_valid = actual_hash == expected_hash

    add_fields(is_valid=is_valid)

    diff = None
    if not is_valid:
        diff = get_schema_diff(expected_rows, actual_rows)
        add_fields(schema_diff=diff)

    return is_valid, diff


def get_schema_diff(expected_schema: List[SchemaRow], actual_schema: List[SchemaRow]) -> str:
    diff = []

    expected_tables = [x.table_name for x in expected_schema]
    actual_tables = [x.table_name for x in actual_schema]

    all_tables = set(expected_tables).union(actual_tables)

    for table in sorted(all_tables):
        expected_for_table = filter(lambda x: x.table_name == table, expected_schema)
        actual_for_table = filter(lambda x: x.table_name == table, actual_schema)

        expected_for_table = [str(x) for x in expected_for_table]
        actual_for_table = [str(x) for x in actual_for_table]

        udiff = unified_diff(expected_for_table, actual_for_table)
        udiff = '\n'.join(udiff)
        if udiff:
            diff.append(udiff)

    return '\n'.join(diff)


def _load_idb_rows(idb_path: str) -> List[SchemaRow]:
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir,
                                        idb_path))

    if local_mode():
        with open(path) as idb_schema:
            schema = json.load(idb_schema)["schema"]
            return [SchemaRow(row['TABLE_NAME'], row['COLUMN_NAME'], row['DATA_TYPE'], row['CHARACTER_MAXIMUM_LENGTH'])
                    for row in schema]

    idb_schema = pkg_resources.resource_string('dsp', os.path.join('datasets', idb_path)).decode('utf-8')
    schema = json.loads(idb_schema)["schema"]
    return [SchemaRow(row['TABLE_NAME'], row['COLUMN_NAME'], row['DATA_TYPE'], row['CHARACTER_MAXIMUM_LENGTH'])
            for row in schema]


def _get_schema_hash(schema_rows: List[SchemaRow]) -> str:
    schema_string = str(schema_rows).encode()
    return hashlib.md5(schema_string).hexdigest()


class DateDelta:
    """
    Similar to datetime.timedelta, but deals with "intuitive" deltas (month/years) on top of "exact" deltas
    (days/weeks).

    Adding a DateDelta with month/years set to a datetime.date does not alter the day part, unless
        a) the target month is to short; day is set to the last day of the target month
        b) the source day is the last day of it's month; target day is set to the last day of target month

    >>> datetime.date(2019, 1, 1) - DateDelta(days=5)
    datetime.date(2018, 12, 27)
    >>> datetime.date(2019, 1, 30) + DateDelta(months=2)
    datetime.date(2019, 3, 30)
    >>> datetime.date(2019, 1, 30) + DateDelta(months=1)
    datetime.date(2019, 2, 28)
    >>> datetime.date(2019, 2, 28) + DateDelta(months=1)
    datetime.date(2019, 3, 31)
    >>> datetime.date(2019, 2, 28) + DateDelta(years=1)
    datetime.date(2020, 2, 29)
    >>> datetime(2019, 1, 1, 23, 45) - DateDelta(weeks=5)
    datetime.datetime(2018, 11, 27, 23, 45)

    """

    def __init__(self, days: int = 0, weeks: int = 0, months: int = 0, years: int = 0):
        self._days = days + 7 * weeks
        self._months = months + 12 * years
        if self._days and self._months:
            raise ValueError("Using exact (days/weeks) and inexact (months/years) deltas at the same time")

    @property
    def days(self):
        return self._days

    @property
    def months(self):
        return self._months

    def __str__(self):
        return "DateDelta(days={}, months={})".format(self._days, self._months)

    def __repr__(self):
        return self.__str__()

    def __add__(self, other: Union[datetime.date, datetime.datetime]) -> Union[datetime.date, datetime.datetime]:
        one_day = datetime.timedelta(days=1)
        if not isinstance(other, (datetime.date, datetime.datetime)):
            return NotImplemented
        if self._days:
            return other + self._days * one_day
        else:
            if other.month != (other + one_day).month:
                # Special case: ensure last day of the month stays last day of month
                return self.__add__(other + one_day) - one_day
            try:
                new_year, new_month = self._as_year_month(other.month + self._months)
                new_year += other.year
                return other.replace(year=new_year, month=new_month)
            except ValueError:
                # Target month has less days than other.days; move to the last day of the target month
                new_year, new_month = self._as_year_month(other.month + self._months + 1)
                new_year += other.year
                return other.replace(year=new_year, month=new_month, day=1) - one_day

    @staticmethod
    def _as_year_month(months) -> Tuple[int, int]:
        years, months_ = divmod(months, 12)
        if months_ == 0:
            months_ = 12
            years -= 1
        return years, months_

    def __radd__(self, other: Union[datetime.date, datetime.datetime]) -> Union[datetime.date, datetime.datetime]:
        return self.__add__(other)

    def __rsub__(self, other: Union[datetime.date, datetime.datetime]) -> Union[datetime.date, datetime.datetime]:
        return (-self).__add__(other)

    def __neg__(self) -> "DateDelta":
        return DateDelta(days=-self._days, months=-self._months)

    def __eq__(self, other: "DateDelta") -> bool:
        if not isinstance(other, DateDelta):
            return NotImplemented
        return (self._days, self._months) == (other.days, other.months)


class PersonScoreValidator(ABC):
    """
    Base class for instances validating person scores
    """

    @abstractmethod
    def validate_person_score(self, person_score: str) -> bool:
        """
        Validate the submitted person score

        Args:
            person_score (str): The submitted value to be validated

        Returns:
            bool: Whether the submitted value is valid
        """


class PersonScoreInSetValidator(PersonScoreValidator):
    """
    A validator which requires that a submitted person score be one of a set of acceptable values
    """

    def __init__(self, acceptable_values: Tuple):
        """
        Args:
            acceptable_values (AbstractSet[str]): The set of values accepted by this validator
        """
        self._acceptable_values = acceptable_values

    def validate_person_score(self, person_score: str) -> bool:
        return person_score in self._acceptable_values


class PersonScoreFormatValidator(PersonScoreValidator):
    """
    A validator which requires that a submitted person score be in the correct format in line
    with the format column in the TOS.
    """

    def __init__(self, format_pattern: str = None):
        """

        Args:
            format_pattern: regex pattern for the accepted format
        """
        self._format_pattern = format_pattern

    def validate_person_score(self, person_score: str) -> bool:
        return self._format_pattern is None or \
            re.match(r"{}".format(self._format_pattern), person_score) is not None


class PersonScoreWithinRangeValidator(PersonScoreValidator):
    """
    A validator which requires that a submitted person score be a numeric value within a given range
    """

    def __init__(self, lower_bound: Union[float, int], upper_bound: Union[float, int],
                 decimal_digits_number: int = 1, allow_na: Optional[bool] = False):
        """
        Args:
            lower_bound (float): The lower bound of the acceptable range, inclusive
            upper_bound (float): The upper bound of the acceptable range, inclusive
            decimal_digits_number (int): If provided, indicates the maximal number of decimal digits,
                                         by default set to 1, for a sake of the backward compatibility
            allow_na (bool): Occasionally we allow 'NA' in the score field for certain assessment
                                codes. This flag tells the validator to return True for a score
                                of 'NA'
        """
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound
        self._decimal_digits_number = decimal_digits_number
        self._allow_na = allow_na

    @property
    def lower_bound(self) -> float:
        return self._lower_bound

    @property
    def upper_bound(self) -> float:
        return self._upper_bound

    def _validate_decimal_digits_number(self, person_score: str) -> None:
        pattern = r"^\d+(\.(\d{dec_num}))?$".replace("dec_num", str(self._decimal_digits_number))
        if not re.search(pattern, person_score):
            raise ValueError("the score has the incorrect number of decimal places")

    @staticmethod
    def _validate_if_person_score_has_no_leading_zeros(person_score: str) -> None:
        if not isinstance(person_score, str):
            raise TypeError("the person score is not a string")

        if re.match(r"^0(\d+)", person_score):
            raise ValueError("the person score has leading 0")

    def _validate_if_person_score_in_range(self, person_score: float) -> None:
        if not self._lower_bound <= person_score <= self._upper_bound:
            raise ValueError(
                f"the person score value should be between "
                f"{self._lower_bound} and {self._upper_bound}.")

    def validate_person_score(self, person_score: str) -> bool:
        if self._allow_na and person_score == 'NA':
            return True
        try:
            self._validate_if_person_score_has_no_leading_zeros(person_score)
            self._validate_decimal_digits_number(person_score)
            person_score_float = float(person_score)
            self._validate_if_person_score_in_range(person_score_float)
        except (ValueError, TypeError):
            return False
        else:
            return True


class PersonScoreIsStringOfScoreInSetsValidator(PersonScoreValidator):
    """
    A validator which requires that a submitted person score be a string of scores
    from several sets of acceptable values
    """

    def __init__(self, acceptable_value_list: List[Container]):
        """
        Args:
            acceptable_value_list (List[AbstractSet[str]]): The list of sets of values accepted by this validator
        """
        self._acceptable_value_list = acceptable_value_list

    def _all_scores_valid(self, person_score: str) -> bool:
        score_list = list(person_score)
        return all(
            [digit in allowed_values for (digit, allowed_values) in zip(person_score, self._acceptable_value_list)] 
        )


    def _correct_number_of_scores(self, person_score: str) -> bool:
        return len(person_score) == len(self._acceptable_value_list)

    def validate_person_score(self, person_score: str) -> bool:
        return self._correct_number_of_scores(person_score) and self._all_scores_valid(person_score)


def is_valid_code(snomed_code_column: Column, validators_by_code: Mapping[str, dict]) -> Column:
    return snomed_code_column.isNull() | snomed_code_column.isin(list(validators_by_code.keys()))


def is_valid_score(snomed_code: Optional[str], score: Optional[str],
                   validators_by_code: Mapping[str, dict],
                   format_only: Optional[bool] = False,
                   ) -> Optional[bool]:
    if score is None or snomed_code is None or snomed_code not in validators_by_code:
        return True
    if format_only:
        validator = validators_by_code[snomed_code]["format_validator"]
    else:
        validator = validators_by_code[snomed_code]["value_validator"]

    return validator.validate_person_score(score)
