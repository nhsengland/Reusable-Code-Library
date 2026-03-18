from decimal import Decimal
from abc import ABC, abstractmethod
from datetime import datetime, date, time
from functools import reduce, partial
from operator import add, is_not, contains, lt, not_, gt, ge, eq, le, ne
import re
from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Type,
    Union)

from dateutil.relativedelta import relativedelta
from dateutil import parser
from dateutil.rrule import rrule, DAILY

from dsp.model.imd_record import IMDRecordPaths
from dsp.model.ons_record import ONSRecordPaths
from dsp.pipeline import spark_state
from testdata.ref_data import providers
from testdata.ref_data.providers import ICD10CodeProvider
from dsp.common.model_accessor import ModelAccessor
from dsp.udfs.organisation import icb_code_from_sub_icb_code, postcode_from_org_code, ccg_code_from_gp_practice_code,\
    provider_code_from_site_code

from dsp.udfs.postcodes import icb_at, pcds_at, electoral_ward_at, county_at, ccg_at, unitary_authority_at, lsoa_at, \
    northings_at, eastings_at, distance_between_postcodes, is_pseudo_country_postcode, active_at
from dsp.shared import safe_issubclass
from dsp.shared.constants import FeatureToggles
from dsp.shared.submissions.calendars import submission_calendar
from dsp.common.constants import CCG_ICB_SWITCHOVER_PERIOD_START, CCG_ICB_SWITCHOVER_PERIOD_END, \
    ICB_DSS_MAPPING_PERIOD_START, ICB_DSS_MAPPING_PERIOD_END, DSS_ICB_RECORD_START_DATE

T = TypeVar('T')
U = TypeVar('U')

__all__ = [
    "AddToDate",
    "AgeAtDate",
    "AgeAtTime",
    "AgeBand",
    "AgeGroup",
    "ASQScoreBand",
    "BreastFeedingStatusMaster",
    "CCGFromGPPracticeCode",
    "CCGGPResExpression",
    "Case",
    "Cast",
    "CcgFromPostcode",
    "CleanEthnicCategory",
    "Concat",
    "ConditionalCommIds",
    "Count",
    "DateInBetween",
    "DateBeforeOrEqualTo",
    "DerivePostcode",
    "DerivePostcodeDistrict",
    "DeriveSnomedTerm",
    'DefaultNone',
    "Equals",
    "EvaluateIfValue",
    "ExtractFromDate",
    "ExtractFromTime",
    "FileTypeExpression",
    "FileTypeExpressionYTD",
    "FilterResolved",
    "Find",
    "FindFirst",
    "First",
    "FirstDateOfMonth",
    "GetAttr",
    "GetDefaultPostcode",
    "GetIMDDecile",
    "GetIMDQuartile",
    "HospitalProviderSpellSpecialCaseCommIds",
    "ICBFromPostcode",
    "ICBFromSubICB",
    "If",
    "Includes",
    "Literal",
    "LowerLayerSuperOutputAreaFromPostcode",
    "MatchCleanICD10Code",
    "Max",
    "Min",
    "MinFilterResolved",
    "MinResolved",
    "ModelExpression",
    "MultipleColumnsNotNull",
    "NaiveDatetimeFromTimestampString",
    "NoCommIds",
    "NotNull",
    "OrgDistance",
    "OverlapDays",
    "ProviderFromSiteCode",
    "RecordNumberOrUniqueID",
    "SchoolYear",
    "Select",
    "SimpleCommIds",
    "Sort",
    "StringToDateTime",
    "SubICBFromGPPracticeCode",
    "SubICBFromPostcode",
    "SubmissionId",
    "UnitaryAuthorityFromGPPracticeCode",
    "UniqMonth",
    "ValidPostcode",
    "DaysBetween",
    "MinutesBetween",
    "DerivePersonBMI"
]


def as_type(target_type: Type[T], input_value: Any, default: Optional[T] = None):
    """ Casts the input value to the specified type where possible, otherwise the default value is returned"""
    try:
        result = target_type(input_value)
    except (TypeError, ValueError):
        result = default

    return result


def is_non_negative_numeric(value):
    """ Tests if the specified value represents a non-negative number """
    return as_type(float, value, -1) >= 0


def get_field_name(expression):
    field_name = expression.accessor.__str__()
    if "." in field_name:
        return field_name.split(".")[-1]
    return field_name


class ModelExpression(ABC, Generic[T, U]):

    @abstractmethod
    def resolve_value(self, model: T) -> U:
        pass

    def _operator_dunder(self, operator: Callable[['ModelExpression', 'ModelExpression'], bool]) \
            -> Type['ModelExpression']:
        """
        Allows for easily defining 'dunders' (Python double underscore magic methods) on ModelExpression.

        See implementations below for examples.

        Args:
            operator:

        Returns:
            Type[ModelExpression]

        """

        class OperatorFactory(ModelExpression):

            def __init__(self, *args: ModelExpression):
                self.args = args

            def resolve_value(self, model: T) -> U:
                values = []
                for a in self.args:
                    if not isinstance(a, ModelExpression):
                        raise ValueError('You can only compare a ModelExpression to a ModelExpression.')
                    values.append(a.resolve_value(model))
                return operator(*values)

        return OperatorFactory

    def __contains__(self, other: 'ModelExpression'):
        return self._operator_dunder(contains)(self, other)

    def __lt__(self, other: 'ModelExpression') -> 'ModelExpression':
        return self._operator_dunder(lt)(self, other)

    def __le__(self, other: 'ModelExpression') -> 'ModelExpression':
        return self._operator_dunder(le)(self, other)

    def __eq__(self, other: 'ModelExpression') -> 'ModelExpression':
        return self._operator_dunder(eq)(self, other)

    def __ne__(self, other: 'ModelExpression') -> 'ModelExpression':
        return self._operator_dunder(ne)(self, other)

    def __ge__(self, other: 'ModelExpression') -> 'ModelExpression':
        return self._operator_dunder(ge)(self, other)

    def __gt__(self, other: 'ModelExpression') -> 'ModelExpression':
        return self._operator_dunder(gt)(self, other)

    def __invert__(self):
        return self._operator_dunder(not_)(self)


class Literal(ModelExpression):

    def __init__(self, value: Any):
        self.value = value

    def resolve_value(self, model):
        return self.value


class Select(ModelExpression):

    def __init__(self, accessor: Union[ModelAccessor, Any]):
        self.accessor = accessor  # type ModelAccessor
        assert safe_issubclass(type(accessor), ModelAccessor)

    def resolve_value(self, model):
        return self.accessor.__get__(model)


class Get(ModelExpression):

    def __init__(self, mapping: ModelExpression, key: ModelExpression):
        self.mapping = mapping
        self.key = key

    def resolve_value(self, model):
        return self.mapping.resolve_value(model).get(self.key.resolve_value(model))


class GetAttr(ModelExpression):

    def __init__(self, model_expr: ModelExpression, attr_expr: ModelExpression):
        self.model_expr = model_expr
        self.attr_expr = attr_expr

    def resolve_value(self, model):
        return getattr(self.model_expr.resolve_value(model), self.attr_expr.resolve_value(model), None)


class Count(ModelExpression):

    def __init__(self, iterable: ModelExpression):
        self.iterable = iterable

    def resolve_value(self, model: T):
        return len(self.iterable.resolve_value(model))


class Find(ModelExpression):

    def __init__(self, iterable: ModelExpression, predicate: Callable,
                 extra_args: Optional[Iterable[ModelExpression]] = None):
        self.iterable = iterable
        self.predicate = predicate
        self.extra_args = extra_args or []

    def resolve_value(self, model: T):
        extra_args = [x.resolve_value(model) for x in self.extra_args]
        values = self.iterable.resolve_value(model)
        if values is None:
            return []
        return [value for value in values if self.predicate(value, *extra_args)]


class First(ModelExpression):

    def __init__(self, iterable: ModelExpression):
        self.iterable = iterable

    def resolve_value(self, model: T):
        value = self.iterable.resolve_value(model)
        if value is None:
            return None
        return next(iter(value), None)


class FindFirst(ModelExpression):

    def __init__(self, iterable: ModelExpression, predicate: Callable,
                 extra_args: Optional[Iterable[ModelExpression]] = None):
        self.iterable = iterable
        self.predicate = predicate
        self.extra_args = extra_args

    def resolve_value(self, model: T):
        return First(Find(self.iterable, self.predicate, self.extra_args)).resolve_value(model)


class Sum(ModelExpression):

    def __init__(self, expr: ModelExpression[Any, List[int]]):
        self.expr = expr

    def resolve_value(self, model: T):
        return reduce(add, self.expr.resolve_value(model), 0)


class Multiply(ModelExpression):

    def __init__(self, left_expr: ModelExpression, right_expr: ModelExpression):
        self.left_expr = left_expr
        self.right_expr = right_expr

    def resolve_value(self, model: T):
        return self.left_expr.resolve_value(model) * self.right_expr.resolve_value(model)


class Concat(ModelExpression):

    def __init__(self, expressions: List):
        self.expressions_to_concat = expressions

    def resolve_value(self, model):
        result = None
        for expr in self.expressions_to_concat:
            if expr.resolve_value(model) is not None:
                if result:
                    result += str(expr.resolve_value(model))
                else:
                    result = str(expr.resolve_value(model))
        return result


class Cast(ModelExpression):
    """
    This is to cast the expression value to the specified value
    :return type casted model expression value
    """

    def __init__(self, expression: ModelExpression, type_val: type):
        self.expression = expression
        self.type_val = type_val

    def resolve_value(self, model):
        val = self.expression.resolve_value(model)
        if val is None:
            return None
        return self.type_val(val)


class DaysBetween(ModelExpression):

    def __init__(
            self, end_date_expr: ModelExpression,
            start_date_expr: ModelExpression,
            only_positive_results: bool = False,
            days_to_add: int = 0,
            account_for_time_element: bool = False
    ):
        self.end_date_expr = end_date_expr
        self.start_date_expr = start_date_expr
        self.only_positive_results = only_positive_results
        self.days_to_add = days_to_add
        self.account_for_time_element = account_for_time_element

    def resolve_value(self, model: T):
        """
            Calculates number of days between two dates.
        Args:
            end_date_expr:              ModelExpression
            start_date_expr:            ModelExpression
            only_positive_results:      for derivations which must return None if result is zero or less
            days_to_add:                for derivations which require result plus one day
            account_for_time_element:   for derivations to countenance time element in input dates

        Returns:
            Number of days between two dates, or None if either date is None or incorrect.
            If dates are the same date and time element is ignored, days between is 0.
        """

        end_date = self.end_date_expr.resolve_value(model)  # type: date
        start_date = self.start_date_expr.resolve_value(model)  # type: date
        days_to_add = self.days_to_add

        if type(end_date) is datetime:
            end_date = end_date.date() if not self.account_for_time_element else end_date
        elif type(end_date) is not date:
            return None

        if type(start_date) is datetime:
            start_date = start_date.date() if not self.account_for_time_element else start_date
        elif type(start_date) is not date:
            return None

        result = (end_date - start_date).days + days_to_add

        if result <= 0 and self.only_positive_results:
            return None

        return result


class MinutesBetween(ModelExpression):

    def __init__(self, start_date_expr: ModelExpression,
                 start_time_expr: ModelExpression,
                 end_date_expr: ModelExpression,
                 end_time_expr: ModelExpression):
        self.end_date_expr = end_date_expr
        self.end_time_expr = end_time_expr
        self.start_date_expr = start_date_expr
        self.start_time_expr = start_time_expr

    def resolve_value(self, model: T):
        """
            Calculates the number of minutes between 2 dates
        Args:
            start_date_expr: The start date for the calculation (passed in as a model expression)
            start_time_expr: The start time for the calculation (passed in as a model expression)
            end_date_expr: The end date for the calculation (passed in as a model expression)
            end_time_expr: The end time for the calculation (passed in as a model expression)

        Returns:
            Number of minutes between two dates,
            None if start date, end date is None or incorrect date / time provided
            See Also: DaysBetween for calculations of days rather than minutes
        """

        end_date = self.end_date_expr.resolve_value(model)  # type: date
        end_time = self.end_time_expr.resolve_value(model)  # type: time
        start_date = self.start_date_expr.resolve_value(model)  # type: date
        start_time = self.start_time_expr.resolve_value(model)  # type: time

        if type(end_date) is datetime:
            end_date = end_date.date
        elif type(end_date) is not date:
            return None

        if type(end_time) is datetime:
            end_time = end_time.time()
        elif type(end_time) is not time:
            return None

        if type(start_date) is datetime:
            start_date = start_date.date()
        elif type(start_date) is not date:
            return None

        if type(start_time) is datetime:
            start_time = start_time.time()
        elif type(start_time) is not time:
            return None

        start_datetime = datetime.combine(start_date, start_time)

        end_datetime = datetime.combine(end_date, end_time)

        return int((end_datetime - start_datetime).total_seconds() / 60.0)


class Filter(ModelExpression):

    def __init__(self, expr_iter: Iterator[ModelExpression], filter_func: Callable):
        self.expr_iter = expr_iter
        self.filter_func = filter_func

    def resolve_value(self, model: T) -> Iterator:
        """

        :param model:
        :return: filters the list of expression by the function passed
        """

        expr_val = list(map(lambda x: x.resolve_value(model), self.expr_iter))
        return filter(self.filter_func, expr_val)


class Reduce(ModelExpression):

    def __init__(self, expr_iter: Iterator[ModelExpression], reduce_func: Callable,
                 filter_none_vals: bool = True):
        self.expr_iter = expr_iter
        self.reduce_func = reduce_func
        self.filter_none_vals = filter_none_vals

    def resolve_value(self, model: T):
        """

        :param model:
        :return: return a list expression after applying the reduce operation on expression_list
        """

        if self.filter_none_vals:
            final_expr_list = list(Filter(self.expr_iter, partial(is_not, None)).resolve_value(model))
        else:
            final_expr_list = [expr.resolve_value(model) for expr in self.expr_iter]

        if not final_expr_list:
            return None

        return reduce(self.reduce_func, final_expr_list)


class Max(ModelExpression):
    """
    This will resolve to the maximum of the list of expressions
    """

    def __init__(self, expr_list: List[ModelExpression]):
        self.expr_list = expr_list

    def resolve_value(self, model: T):
        """

        :param model:
        :return:
        the maximum value from the list of expressions
        """
        return Reduce(expr_iter=iter(self.expr_list), reduce_func=max).resolve_value(model)


class Min(ModelExpression):
    """
    This will resolve to the minimum of the list of expressions
    """

    def __init__(self, expr_list: List[ModelExpression]):
        self.expr_list = expr_list

    def resolve_value(self, model: T):
        """

        :param model:
        :return:
        the minimum value from the list of expressions
        """
        return Reduce(expr_iter=iter(self.expr_list), reduce_func=min).resolve_value(model)


class OverlapDays(ModelExpression):
    """This will count the number of days common in two date ranges"""

    def __init__(self, date_range_1_start_expr: ModelExpression, date_range_1_end_expr: ModelExpression,
                 date_range_2_start_expr: ModelExpression, date_range_2_end_expr: ModelExpression,
                 days_to_add_expr: ModelExpression = Literal(0)):

        self.date_range_1_start_expr = date_range_1_start_expr
        self.date_range_1_end_expr = date_range_1_end_expr
        self.date_range_2_start_expr = date_range_2_start_expr
        self.date_range_2_end_expr = date_range_2_end_expr
        self.days_to_add_expr = days_to_add_expr

    def resolve_value(self, model: T):
        days_added_to_date_range_1_end_expr = AddToDate(self.date_range_1_end_expr, self.days_to_add_expr)
        return Max([
            Literal(0),
            DaysBetween(
                Min([days_added_to_date_range_1_end_expr, self.date_range_2_end_expr]),
                Max([self.date_range_1_start_expr, self.date_range_2_start_expr]),
                days_to_add=1
            )
        ]).resolve_value(model)


class SchoolYear(ModelExpression):

    def __init__(self, date_of_birth_expr: ModelExpression, date_of_activity_expr: ModelExpression,
                 age_at_activity_expr: ModelExpression):
        self.date_of_birth_expr = date_of_birth_expr
        self.date_of_activity_expr = date_of_activity_expr
        self.age_at_activity_expr = age_at_activity_expr
        self.dict_school_year = {
            -5: None,
            -4: "E1",
            -3: "E2",
            -2: "N1",
            -1: "N2",
            0: "R",
            1: "Y1",
            2: "Y2",
            3: "Y3",
            4: "Y4",
            5: "Y5",
            6: "Y6",
            7: "Y7",
            8: "Y8",
            9: "Y9",
            10: "Y10",
            11: "Y11",
            12: "Y12",
            13: "Y13",
            14: None,
        }

    def resolve_value(self, model):

        if (self.date_of_birth_expr.resolve_value(model) is None) or (
                self.date_of_activity_expr.resolve_value(model) is None
        ):
            return None

        birth_month_offset = (self.date_of_birth_expr.resolve_value(model).month + 3) % 12
        activity_month_offset = (self.date_of_activity_expr.resolve_value(model).month + 3) % 12
        ageyr = self.age_at_activity_expr.resolve_value(model)

        if ageyr > 18 or ageyr < 0:
            return None

        if birth_month_offset <= activity_month_offset:
            return self.dict_school_year[ageyr - 5]

        return self.dict_school_year[ageyr - 4]


class AgeGroup(ModelExpression):
    def __init__(self, age_yrs_expr: ModelExpression):
        self.age_yrs_expr = age_yrs_expr

    def resolve_value(self, model: T):
        return Case(
            variable=self.age_yrs_expr,
            branches=[
                (lambda age: age is None, Literal(None)),
                (lambda age: age <= 18, Literal("0-18")),
                (lambda age: age <= 64, Literal("19-64")),
                (lambda age: age <= 129, Literal("65_Plus")),
            ],
        ).resolve_value(model)


class AgeBand(ModelExpression):
    def __init__(self, age_yr_expr: ModelExpression):
        self.age_yr_expr = age_yr_expr

    def resolve_value(self, model: T):
        return Case(
            variable=self.age_yr_expr,
            branches=[
                (lambda age: age is None, Literal(None)),
                (lambda age: age <= 4, Literal("0-4")),
                (lambda age: age <= 9, Literal("5-9")),
                (lambda age: age <= 14, Literal("10-14")),
                (lambda age: age <= 19, Literal("15-19")),
                (lambda age: age <= 24, Literal("20-24")),
                (lambda age: age <= 29, Literal("25-29")),
                (lambda age: age <= 34, Literal("30-34")),
                (lambda age: age <= 39, Literal("35-39")),
                (lambda age: age <= 44, Literal("40-44")),
                (lambda age: age <= 49, Literal("45-49")),
                (lambda age: age <= 54, Literal("50-54")),
                (lambda age: age <= 59, Literal("55-59")),
                (lambda age: age <= 64, Literal("60-64")),
                (lambda age: age <= 69, Literal("65-69")),
                (lambda age: age <= 74, Literal("70-74")),
                (lambda age: age <= 79, Literal("75-79")),
                (lambda age: age <= 84, Literal("80-84")),
                (lambda age: age <= 89, Literal("85-89")),
                (lambda age: age <= 94, Literal("90-94")),
                (lambda age: age <= 99, Literal("95-99")),
                (lambda age: age <= 104, Literal("100-104")),
                (lambda age: age <= 109, Literal("105-109")),
                (lambda age: age <= 114, Literal("110-114")),
                (lambda age: age <= 119, Literal("115-119")),
                (lambda age: age <= 124, Literal("120-124")),
                (lambda age: age <= 129, Literal("125-129")),
            ],
        ).resolve_value(model)


class BreastFeedingStatusMaster(ModelExpression):
    def __init__(self, coded_finding: ModelExpression, breast_feeding_status: ModelExpression):
        self.coded_finding = coded_finding
        self.breast_feeding_status = breast_feeding_status

    def resolve_value(self, model: T) -> U:
        def helper_function(breastfeeding_statuses: List, check_value: str):
            return breastfeeding_statuses and breastfeeding_statuses[0].BreastFeedingStatus == check_value

        case_args_dict = {'coded_finding': self.coded_finding, 'breast_feeding_status': self.breast_feeding_status}

        return MultiCase(
            case_args_dict=case_args_dict,
            branches=[
                (lambda case_args: case_args['coded_finding'] in (
                    '169741004', '169745008', '169750002', '169751003', '169968005', '169973004', '230127002',
                    '412729005', '364991000000100', '62P1.', '62P5.', '62PA.', '62PB.', '6412.', '6422.', 'Ub1ve',
                    'XaJgn', '64e1.', 'XaPO0', '62PE.'
                ), Literal('B1')),
                (lambda case_args: case_args['coded_finding'] in (
                    '169743001', '169969002', '169974005', '733896006', '365091000000100', '491951000000103',
                    '62P3.', '6413.', '6423.', 'XaPO8', '62P30', 'XaQ7W'
                ), Literal('B2')),
                (lambda case_args: case_args['coded_finding'] in (
                    '169744007', '169746009', '169747000', '169967000', '169972009', '268472006', '412728002',
                    '365171000000100', '62P4.', '62P6.', '62P7.', '6411.', '6421.', '62P2.', '64e0.', '62PF.', 'XE1SF',
                    'XaJgm', 'XaPOG'
                ), Literal('B3')),
                (lambda case_args: helper_function(case_args['breast_feeding_status'], '01'), Literal('B1')),
                (lambda case_args: helper_function(case_args['breast_feeding_status'], '02'), Literal('B2')),
                (lambda case_args: helper_function(case_args['breast_feeding_status'], '03'), Literal('B3')),
            ]
        ).resolve_value(model)


class ASQScoreBand(ModelExpression):
    def __init__(self, snomed_id: ModelExpression, score: ModelExpression):
        self.snomed_id = snomed_id
        self.score = score

    def resolve_value(self, model: T):

        if self.snomed_id.resolve_value(model) is None:
            return None

        try:
            float(self.score.resolve_value(model))
        except (TypeError, ValueError):
            return None

        case_args_dict = {
            'snomed_id': self.snomed_id,
            'score': self.score
        }

        return MultiCase(
            case_args_dict=case_args_dict,
            branches=[
                (lambda case_args: case_args['snomed_id'] == '953211000000100' and (
                    0 <= float(case_args['score']) < 25.17), Literal('C1')),
                (lambda case_args: case_args['snomed_id'] == '953261000000103' and (
                    0 <= float(case_args['score']) < 24.02), Literal('C1')),
                (lambda case_args: case_args['snomed_id'] == '953311000000105' and (
                    0 <= float(case_args['score']) < 33.3), Literal('C1')),
                (lambda case_args: case_args['snomed_id'] == '953231000000108' and (
                    0 <= float(case_args['score']) < 38.07), Literal('G1')),
                (lambda case_args: case_args['snomed_id'] == '953281000000107' and (
                    0 <= float(case_args['score']) < 28.01), Literal('G1')),
                (lambda case_args: case_args['snomed_id'] == '953331000000102' and (
                    0 <= float(case_args['score']) < 36.14), Literal('G1')),
                (lambda case_args: case_args['snomed_id'] == '953221000000106' and (
                    0 <= float(case_args['score']) < 35.16), Literal('F1')),
                (lambda case_args: case_args['snomed_id'] == '953271000000105' and (
                    0 <= float(case_args['score']) < 18.42), Literal('F1')),
                (lambda case_args: case_args['snomed_id'] == '953321000000104' and (
                    0 <= float(case_args['score']) < 19.25), Literal('F1')),
                (lambda case_args: case_args['snomed_id'] == '953251000000101' and (
                    0 <= float(case_args['score']) < 31.54), Literal('S1')),
                (lambda case_args: case_args['snomed_id'] == '953301000000108' and (
                    0 <= float(case_args['score']) < 25.31), Literal('S1')),
                (lambda case_args: case_args['snomed_id'] == '953351000000109' and (
                    0 <= float(case_args['score']) < 32.01), Literal('S1')),
                (lambda case_args: case_args['snomed_id'] == '953241000000104' and (
                    0 <= float(case_args['score']) < 29.78), Literal('P1')),
                (lambda case_args: case_args['snomed_id'] == '953291000000109' and (
                    0 <= float(case_args['score']) < 27.62), Literal('P1')),
                (lambda case_args: case_args['snomed_id'] == '953341000000106' and (
                    0 <= float(case_args['score']) < 27.08), Literal('P1')),
                (lambda case_args: case_args['snomed_id'] == '953211000000100' and (
                    25.17 <= float(case_args['score']) <= 60), Literal('C2')),
                (lambda case_args: case_args['snomed_id'] == '953261000000103' and (
                    24.02 <= float(case_args['score']) <= 60), Literal('C2')),
                (lambda case_args: case_args['snomed_id'] == '953311000000105' and (
                    33.3 <= float(case_args['score']) <= 60), Literal('C2')),
                (lambda case_args: case_args['snomed_id'] == '953231000000108' and (
                    38.07 <= float(case_args['score']) <= 60), Literal('G2')),
                (lambda case_args: case_args['snomed_id'] == '953281000000107' and (
                    28.01 <= float(case_args['score']) <= 60), Literal('G2')),
                (lambda case_args: case_args['snomed_id'] == '953331000000102' and (
                    36.14 <= float(case_args['score']) <= 60), Literal('G2')),
                (lambda case_args: case_args['snomed_id'] == '953221000000106' and (
                    35.16 <= float(case_args['score']) <= 60), Literal('F2')),
                (lambda case_args: case_args['snomed_id'] == '953271000000105' and (
                    18.42 <= float(case_args['score']) <= 60), Literal('F2')),
                (lambda case_args: case_args['snomed_id'] == '953321000000104' and (
                    19.25 <= float(case_args['score']) <= 60), Literal('F2')),
                (lambda case_args: case_args['snomed_id'] == '953251000000101' and (
                    31.54 <= float(case_args['score']) <= 60), Literal('S2')),
                (lambda case_args: case_args['snomed_id'] == '953301000000108' and (
                    25.31 <= float(case_args['score']) <= 60), Literal('S2')),
                (lambda case_args: case_args['snomed_id'] == '953351000000109' and (
                    32.01 <= float(case_args['score']) <= 60), Literal('S2')),
                (lambda case_args: case_args['snomed_id'] == '953241000000104' and (
                    29.78 <= float(case_args['score']) <= 60), Literal('P2')),
                (lambda case_args: case_args['snomed_id'] == '953291000000109' and (
                    27.62 <= float(case_args['score']) <= 60), Literal('P2')),
                (lambda case_args: case_args['snomed_id'] == '953341000000106' and (
                    27.08 <= float(case_args['score']) <= 60), Literal('P2')),
                (lambda case_args: case_args['snomed_id'] == '953211000000100' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('C3')),
                (lambda case_args: case_args['snomed_id'] == '953261000000103' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('C3')),
                (lambda case_args: case_args['snomed_id'] == '953311000000105' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('C3')),
                (lambda case_args: case_args['snomed_id'] == '953231000000108' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('G3')),
                (lambda case_args: case_args['snomed_id'] == '953281000000107' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('G3')),
                (lambda case_args: case_args['snomed_id'] == '953331000000102' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('G3')),
                (lambda case_args: case_args['snomed_id'] == '953221000000106' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('F3')),
                (lambda case_args: case_args['snomed_id'] == '953271000000105' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('F3')),
                (lambda case_args: case_args['snomed_id'] == '953321000000104' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('F3')),
                (lambda case_args: case_args['snomed_id'] == '953251000000101' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('S3')),
                (lambda case_args: case_args['snomed_id'] == '953301000000108' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('S3')),
                (lambda case_args: case_args['snomed_id'] == '953351000000109' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('S3')),
                (lambda case_args: case_args['snomed_id'] == '953241000000104' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('P3')),
                (lambda case_args: case_args['snomed_id'] == '953291000000109' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('P3')),
                (lambda case_args: case_args['snomed_id'] == '953341000000106' and (
                    float(case_args['score']) < 0 or float(case_args['score']) > 60), Literal('P3')),
            ]).resolve_value(model)


class NaiveDatetimeFromTimestampString(ModelExpression):
    def __init__(self, timestamp_str_exp: ModelExpression):
        self.timestamp_str_exp = timestamp_str_exp

    def resolve_value(self, model):
        timestamp_str = self.timestamp_str_exp.resolve_value(model)
        if not timestamp_str:
            return None
        datetime_with_offset_pattern = r"(?P<date_and_time>.*)(?P<offset>(Z|(\+|-)\d\d:\d\d)$)"
        matches = re.match(datetime_with_offset_pattern, timestamp_str)
        if matches:
            return parser.parse(matches["date_and_time"])
        return parser.parse(timestamp_str)


class AgeAtDate(ModelExpression):
    YEARS = 1
    DAYS = 2

    def __init__(self, date_of_birth_expr: ModelExpression, date_for_age_expr: ModelExpression,
                 ignore_time: bool = False, time_unit: int = YEARS):
        self.date_of_birth_expr = date_of_birth_expr
        self.date_for_age_expr = date_for_age_expr
        self.ignore_time = ignore_time
        assert time_unit in (self.YEARS, self.DAYS)
        self.time_unit = time_unit

    def resolve_value(self, model):
        date_of_birth = self.date_of_birth_expr.resolve_value(model)
        date_for_age = self.date_for_age_expr.resolve_value(model)
        if not (date_for_age and date_of_birth):
            return None

        if self.ignore_time:
            date_for_age = datetime(date_for_age.year, date_for_age.month, date_for_age.day)
            date_of_birth = datetime(date_of_birth.year, date_of_birth.month, date_of_birth.day)

        if self.time_unit == self.YEARS:
            # handle leap year birth days
            if (
                    date_of_birth.month == 2
                    and date_of_birth.day == 29
                    and date_for_age.month == 2
                    and date_for_age.day == 28
            ):
                date_for_age = datetime(date_for_age.year, date_for_age.month, date_for_age.day - 1)

            return relativedelta(date_for_age, date_of_birth).years

        return (date_for_age - date_of_birth).days


class AgeAtTime(ModelExpression):
    HOURS = 1
    MINUTES = 2

    def __init__(self, date_of_birth_expr: ModelExpression, time_of_birth_expr: ModelExpression,
                 date_for_age_expr: ModelExpression, time_for_age_expr: ModelExpression,
                 time_unit: int = HOURS):
        self.date_of_birth_expr = date_of_birth_expr
        self.time_of_birth_expr = time_of_birth_expr
        self.date_for_age_expr = date_for_age_expr
        self.time_for_age_expr = time_for_age_expr
        assert time_unit in (self.HOURS, self.MINUTES)
        self.time_unit = time_unit

    def resolve_value(self, model):
        date_of_birth = self.date_of_birth_expr.resolve_value(model)
        date_for_age = self.date_for_age_expr.resolve_value(model)
        time_of_birth = self.time_of_birth_expr.resolve_value(model)
        time_for_age = self.time_for_age_expr.resolve_value(model)
        if not (date_for_age and date_of_birth and time_of_birth and time_for_age):
            return None
        datetime_of_birth = datetime(date_of_birth.year, date_of_birth.month, date_of_birth.day,
                                     time_of_birth.hour, time_of_birth.minute, time_of_birth.second)
        datetime_for_age = datetime(date_for_age.year, date_for_age.month, date_for_age.day,
                                    time_for_age.hour, time_for_age.minute, time_for_age.second)
        delta_in_seconds = int((datetime_for_age - datetime_of_birth).total_seconds())
        if self.time_unit == self.MINUTES:
            return delta_in_seconds // 60
        if self.time_unit == self.HOURS:
            return delta_in_seconds // (60 * 60)


class ExtractFromDate(ModelExpression):
    YEAR = 1
    MONTH = 2
    DAYOFWEEK = 3

    def __init__(self, date_expr: ModelExpression, field: int):
        assert field in (self.YEAR, self.MONTH, self.DAYOFWEEK)
        self.field = field
        self.date_expr = date_expr

    def resolve_value(self, model):
        value = self.date_expr.resolve_value(model)
        if value is None:
            return None
        if self.field == self.YEAR:
            return value.year
        elif self.field == self.MONTH:
            # expected value is an2: 01, 02, 03, 04,...
            return "{:02d}".format(value.month)
        elif self.field == self.DAYOFWEEK:
            # expected value is an2: 01, 02, 03, 04,...
            return "{:02d}".format(value.isoweekday())


class ExtractFromTime(ModelExpression):
    MERIDIAN = 1

    def __init__(self, time_expr: ModelExpression, field: int):
        assert field in (self.MERIDIAN,)
        self.field = field
        self.time_expr = time_expr

    def resolve_value(self, model):
        value = self.time_expr.resolve_value(model)
        if value is None:
            return None
        if self.field == self.MERIDIAN:
            # expected value is an2: 01, 02
            return "01" if value.hour < 12 else "02"


class EvaluateIfValue(ModelExpression):
    def __init__(self, dependent_fields_expr: list, model_expression: ModelExpression):
        self.dependent_fields_expr = dependent_fields_expr
        self.model_expression = model_expression

    def resolve_value(self, model):
        """
        Evaluate the model expression only if there the dependent fields have values
        :param model:
        :return: the result of the model expression in the model
        """
        for field_expr in self.dependent_fields_expr:
            if field_expr.resolve_value(model) is None:
                return None

        return self.model_expression.resolve_value(model)


class ValidPostcode(ModelExpression):
    def __init__(self, postcode_expr: ModelExpression, point_in_time_expr: ModelExpression):
        self.postcode_expr = postcode_expr
        self.point_in_time_expr = point_in_time_expr

    def resolve_value(self, model):
        postcode = self.postcode_expr.resolve_value(model)
        start_date = self.point_in_time_expr.resolve_value(model)
        if not postcode:
            return False

        return active_at(postcode, start_date)


class ValToDateTime(ModelExpression):

    def __init__(self, val_expr: ModelExpression, val_format: str):
        self.val_expr = val_expr
        self.val_format = val_format

    def resolve_value(self, model):
        return datetime.strptime(str(self.val_expr.resolve_value(model)), self.val_format)


class ToDate(ModelExpression):
    def __init__(self, dt_expr):
        self.dt_expr = dt_expr

    def resolve_value(self, model):
        if self.dt_expr.resolve_value(model) is None:
            return None
        else:
            return self.dt_expr.resolve_value(model).date()


class PostcodeFromODS(ModelExpression):
    def __init__(self, ods_expr: ModelExpression, point_in_time_expr: ModelExpression = None):
        self.ods_expr = ods_expr
        self.point_in_time_expr = point_in_time_expr

    def resolve_value(self, model):
        if self.ods_expr.resolve_value(model) is None:
            return None
        else:
            return postcode_from_org_code(self.ods_expr.resolve_value(model),
                                          self.point_in_time_expr.resolve_value(model))


class DerivePostcodeDistrict(ModelExpression):
    def __init__(self, postcode_expr: ModelExpression, point_in_time_expr: ModelExpression):
        self.postcode_expr = postcode_expr
        self.point_in_time_expr = point_in_time_expr

    def resolve_value(self, model):
        pcds = DerivePostcode(self.postcode_expr, self.point_in_time_expr, ONSRecordPaths.POSTCODE).resolve_value(model)

        return pcds.split(" ")[0] if pcds else None


class DerivePostcode(ModelExpression):
    field_name_dict = {
        ONSRecordPaths.POSTCODE: pcds_at,
        ONSRecordPaths.OS_WARD_2011: electoral_ward_at,
        ONSRecordPaths.LOWER_LAYER_SOA: lsoa_at,
        ONSRecordPaths.RESIDENCE_COUNTY: county_at,
        ONSRecordPaths.UNITARY_AUTHORITY: unitary_authority_at,
        ONSRecordPaths.OS_NORTH_1M: northings_at,
        ONSRecordPaths.OS_EAST_1M: eastings_at,
        ONSRecordPaths.CCG: ccg_at,
        ONSRecordPaths.ICB: icb_at
    }

    def __init__(self, postcode_expr: ModelExpression, point_in_time_expr: ModelExpression, field_name: str):
        self.postcode_expr = postcode_expr
        self.point_in_time_expr = point_in_time_expr
        self.field_name = field_name

    def resolve_value(self, model):
        postcode = self.postcode_expr.resolve_value(model)
        point_in_time = self.point_in_time_expr.resolve_value(model)

        return self.field_name_dict[self.field_name](postcode, point_in_time)


class GetIMDQuartile(ModelExpression):
    def __init__(self, postcode_expr: ModelExpression, point_in_time_expr: ModelExpression, imd_year=2015):
        self.postcode_expr = postcode_expr
        self.point_in_time_expr = point_in_time_expr
        self.imd_year = imd_year

    def resolve_value(self, model):
        lsoa = DerivePostcode(self.postcode_expr, self.point_in_time_expr,
                              ONSRecordPaths.LOWER_LAYER_SOA).resolve_value(model)
        if lsoa is None:
            return None

        provider = providers.IMDProvider()

        imd_quartile = provider.value_at(lsoa, IMDRecordPaths.Quartile, self.point_in_time_expr.resolve_value(model),
                                         self.imd_year)
        try:
            return int(imd_quartile.split()[0]) if imd_quartile else None
        except ValueError:
            return None


class GetIMDDecile(ModelExpression):
    def __init__(self, postcode_expr: ModelExpression, point_in_time_expr: ModelExpression, imd_year=2015,
                 should_sanitise=False):
        self.postcode_expr = postcode_expr
        self.point_in_time_expr = point_in_time_expr
        self.imd_year = imd_year
        self.should_sanitise = should_sanitise

    def resolve_value(self, model):
        lsoa = DerivePostcode(self.postcode_expr, self.point_in_time_expr,
                              ONSRecordPaths.LOWER_LAYER_SOA).resolve_value(model)
        if lsoa is None:
            return None

        provider = providers.IMDProvider()

        imd_decile = provider.value_at(lsoa, IMDRecordPaths.Decile,
                                       self.point_in_time_expr.resolve_value(model),
                                       self.imd_year)

        if not imd_decile:
            return None
        if self.should_sanitise:
            try:
                return int(imd_decile.split()[0])
            except ValueError:
                return None

        return imd_decile


class OrgDistance(ModelExpression):
    """
    The expression will return the distance between an org code and postcode
    :return distance in km rounded to the nearest integer value
    """

    def __init__(self, org_code_expr: ModelExpression, from_postcode_expr: ModelExpression,
                 point_in_time_expr: ModelExpression):
        self.org_code_expr = org_code_expr
        self.from_postcode_expr = from_postcode_expr
        self.point_in_time_expr = point_in_time_expr

    def resolve_value(self, model):
        point_in_time = self.point_in_time_expr.resolve_value(model)
        org_postcode = postcode_from_org_code(self.org_code_expr.resolve_value(model), point_in_time)

        distance = distance_between_postcodes(self.from_postcode_expr.resolve_value(model),
                                              org_postcode,
                                              point_in_time)
        if distance is None:
            return None

        return round(distance / 1000)


class GetDefaultPostcode(ModelExpression):
    def __init__(self, postcode_expr: ModelExpression):
        self.postcode_expr = postcode_expr

    def resolve_value(self, model):
        postcode = self.postcode_expr.resolve_value(model)
        if is_pseudo_country_postcode(postcode):
            return postcode

        return None


class If(ModelExpression):
    def __init__(self, condition: ModelExpression, then: ModelExpression, otherwise: ModelExpression):
        self.condition = condition
        self.then = then
        self.otherwise = otherwise

    def resolve_value(self, model: T):
        if self.condition.resolve_value(model):
            return self.then.resolve_value(model)

        return self.otherwise.resolve_value(model)


class Case(ModelExpression):
    def __init__(self, variable: ModelExpression,
                 branches: List[Tuple[Callable[[ModelExpression], bool], ModelExpression]],
                 default: Optional[ModelExpression] = None):
        self.variable = variable
        self.branches = branches
        if default is not None:
            self.branches.append((lambda x: True, default))

    def resolve_value(self, model: T):
        var_value = self.variable.resolve_value(model)
        for predicate, then in self.branches:
            if predicate(var_value):
                return then.resolve_value(model)
        return None


class MultiCase(ModelExpression):
    def __init__(self, case_args_dict: dict,
                 branches: List[Tuple[Callable[[ModelExpression], bool], ModelExpression]],
                 default: Optional[ModelExpression] = None):
        self.case_args_dict = case_args_dict
        self.branches = branches
        if default is not None:
            self.branches.append((lambda x: True, default))

    def resolve_value(self, model: T):
        var_values = {k: v.resolve_value(model) for (k, v) in self.case_args_dict.items()}
        for predicate, then in self.branches:
            if predicate(var_values):
                return then.resolve_value(model)
        return None


class NotNull(ModelExpression):
    def __init__(self, delegate: ModelExpression):
        self.delegate = delegate

    def resolve_value(self, model: T):
        """
            checks if value is not null
        Args:
            model: type of ModelExpression to evaluate for Not null

        Returns:
            boolean value. True if Not None and False if None
        """
        return self.delegate.resolve_value(model) is not None


class DefaultNone(ModelExpression):
    def __init__(self):
        pass

    def resolve_value(self, model: T):
        """
            returns None regardless of value
        Args:
            model: type of ModelExpression to evaluate to return None
        Returns:
            None
        """
        return None


class Equals(ModelExpression):
    """ Tests if the left hand expression is equal to the right hand expression """

    def __init__(self, lhs: ModelExpression, rhs: ModelExpression):
        self.lhs = lhs
        self.rhs = rhs

    def resolve_value(self, model: T) -> bool:
        """ Tests if the left hand expression is equal to the right hand expression

        Args:
            model: Model used when evaluating the ModelExpressions
        """
        return self.lhs.resolve_value(model) == self.rhs.resolve_value(model)


class Includes(ModelExpression):
    """ Tests if the input matches any one element in the right hand list """

    def __init__(self, input: ModelExpression, lookup: ModelExpression):
        self.input = input
        self.lookup = lookup

    def resolve_value(self, model: T) -> bool:
        """ Tests if the input contains any one element in the collection

        Args:
            model: Model used when evaluating the ModelExpressions
        """
        input = self.input.resolve_value(model)
        lookup = self.lookup.resolve_value(model)
        if input is None or lookup is None:
            return False

        return any(value for value in lookup if value == input)


class DateBeforeOrEqualTo(ModelExpression):
    def __init__(self, eval_expr: ModelExpression, check_date_expr: ModelExpression):
        self.eval_expr = eval_expr
        self.check_date_expr = check_date_expr

    def resolve_value(self, model: T):
        """
            checks if date is before or equal to the passed in date.
        Args:
            model: ModelExpression

        Returns:
            boolean value. True if date is before or equal to the passed in date. False if passed in date is None.
        """

        actual_date = self.eval_expr.resolve_value(model)  # type: datetime
        check_date = self.check_date_expr.resolve_value(model)  # type: datetime

        if actual_date is not None and check_date is not None:
            return actual_date <= check_date
        else:
            return False


class DateInBetween(ModelExpression):
    def __init__(self, eval_expr: ModelExpression, start_expr: ModelExpression, end_expr: ModelExpression):
        self.start_expr = start_expr
        self.end_expr = end_expr
        self.eval_expr = eval_expr

    def resolve_value(self, model: T):
        """
            checks if date is between start and end date.
        Args:
            model: ModelExpression

        Returns:
            boolean value. True if date is between start and end date . False if any date is None or not between
        """
        date_to_check = self.eval_expr.resolve_value(model)  # type: datetime
        start_date = self.start_expr.resolve_value(model)  # type: datetime
        end_date = self.end_expr.resolve_value(model)  # type: datetime

        return (start_date is not None and
                date_to_check is not None and
                end_date is not None and
                (start_date <= date_to_check <= end_date))


class AddToDate(ModelExpression):
    def __init__(self, input_date_expr: ModelExpression, days_to_add_expr: ModelExpression = None,
                 months_to_add_expr: ModelExpression = None,
                 years_to_add_expr: ModelExpression = None):
        self.days_to_add_expr = days_to_add_expr
        self.input_date_expr = input_date_expr
        self.months_to_add_expr = months_to_add_expr
        self.years_to_add_expr = years_to_add_expr

    def resolve_value(self, model: T):

        input_date = self.input_date_expr.resolve_value(model)
        if not input_date:
            return None

        days_to_add = 0
        months_to_add = 0
        years_to_add = 0

        if self.days_to_add_expr is not None:
            days_to_add = self.days_to_add_expr.resolve_value(model)

        if self.months_to_add_expr is not None:
            months_to_add = self.months_to_add_expr.resolve_value(model)

        if self.years_to_add_expr is not None:
            years_to_add = self.years_to_add_expr.resolve_value(model)

        return input_date + relativedelta(days=days_to_add, months=months_to_add, years=years_to_add)


class FirstDateOfMonth(ModelExpression):
    def __init__(self, input_date_expr: ModelExpression):
        self.input_date_expr = input_date_expr

    def resolve_value(self, model: T):
        input_date = self.input_date_expr.resolve_value(model)

        if not input_date:
            return None

        return input_date.replace(day=1)


class SubmissionId(ModelExpression):
    def __init__(self, meta_event_id_expr):
        self.meta_event_id_expr = meta_event_id_expr

    def resolve_value(self, model):
        event_id = self.meta_event_id_expr.resolve_value(model)
        return int(event_id.split(':')[0])


class CcgFromPostcode(ModelExpression):

    def __init__(self,
                 postcode_expr: ModelExpression,
                 event_date: ModelExpression,
                 enforce_icb_switchover_period: ModelExpression = Literal(False)):
        self.postcode_expr = postcode_expr
        self.event_date = event_date
        self.enforce_icb_switchover_period = enforce_icb_switchover_period

    def resolve_value(self, model):
        given_date = self.event_date.resolve_value(model)

        if self.enforce_icb_switchover_period.resolve_value(model) and DateInBetween(
                self.event_date, Literal(CCG_ICB_SWITCHOVER_PERIOD_START), Literal(CCG_ICB_SWITCHOVER_PERIOD_END)
        ).resolve_value(model):
            given_date = CCG_ICB_SWITCHOVER_PERIOD_START

        return ccg_at(self.postcode_expr.resolve_value(model), given_date)


class CCGFromGPPracticeCode(ModelExpression):
    """
    The CCG of the patient's registered GP Practice, as derived from data item GENERAL
    MEDICAL PRACTICE CODE (PATIENT REGISTRATION)
    Where a Null, Invalid or Default GPCD is submitted, Organisation Identifier (CCG of GP Practice)
    will appear as Null. Checks will be made point-in-time
    """

    def __init__(self,
                 gp_practice_code: ModelExpression,
                 event_date: ModelExpression,
                 enforce_icb_switchover_period: ModelExpression = Literal(False)):
        self.gp_practice_code = gp_practice_code
        self.event_date = event_date
        self.enforce_icb_switchover_period = enforce_icb_switchover_period

    def resolve_value(self, model):
        given_date = self.event_date.resolve_value(model)

        if self.enforce_icb_switchover_period.resolve_value(model) and DateInBetween(
                self.event_date, Literal(CCG_ICB_SWITCHOVER_PERIOD_START), Literal(CCG_ICB_SWITCHOVER_PERIOD_END)
        ).resolve_value(model):
            given_date = CCG_ICB_SWITCHOVER_PERIOD_START

        return ccg_code_from_gp_practice_code(self.gp_practice_code.resolve_value(model), given_date)


class CCGGPResExpression(ModelExpression):
    """
    CCG derived from CCG of GP Practice and CCG of Residence
    N.B Construction reflects DMS construction and may not be finalised.

    Organisation ID of CCG of GP practice or residence. Derived from MHS002GP and MHS001MPI table

    Existing conditions from TOS
        case when c.OrgCodeGPPrac IS not null then c.OrgCodeGPPrac
             when c.OrgCodeCCGGPPractice IS not null then c.OrgCodeCCGGPPractice
             else a.OrgCodeCCGRes end as IC_Rec_CCG

        WHERE IC_NAT_MRecent_IN_RP_FLAG = 'Y'
        And GMPCodeReg NOT IN ('V81999','V81998','V81997')
        and OrgCodeGPPrac <> '-1'
        and EndDateGMPRegistration is null

    """

    def __init__(self, gp_list_expr: ModelExpression, org_id_ccg_res: ModelExpression):
        self.gp_list_expr = gp_list_expr
        self.org_id_ccg_res = org_id_ccg_res

    def resolve_value(self, model):
        gp_list = self.gp_list_expr.resolve_value(model)

        for gp in gp_list:
            if (gp.EndDateGMPRegistration is not None
                    or gp.OrgIDGPPrac == '-1' or gp.GMPCodeReg in ['V81999', 'V81998', 'V81997']):
                continue

            if gp.OrgIDGPPrac:
                return gp.OrgIDGPPrac
            if gp.OrgIDCCGGPPractice:
                return gp.OrgIDCCGGPPractice

        return self.org_id_ccg_res.resolve_value(model)


class SubICBFromPostcode(ModelExpression):
    """
    The sub-ICB Location of residence, as derived from data item POSTCODE OF USUAL ADDRESS.
    """

    def __init__(self, postcode_expr: ModelExpression, event_date: ModelExpression):
        self.postcode_expr = postcode_expr
        self.event_date = event_date

    def resolve_value(self, model):
        given_date = self.event_date.resolve_value(model)

        if DateInBetween(
                self.event_date, Literal(ICB_DSS_MAPPING_PERIOD_START), Literal(ICB_DSS_MAPPING_PERIOD_END)
        ).resolve_value(model):
            given_date = DSS_ICB_RECORD_START_DATE

        return ccg_at(self.postcode_expr.resolve_value(model), given_date)


class ICBFromPostcode(ModelExpression):
    """
    The ICB of the patient's residence, as derived from data item POST CODE OF USUAL ADDRESS.
    """

    def __init__(self, postcode_expr: ModelExpression, event_date: ModelExpression):
        self.postcode_expr = postcode_expr
        self.event_date = event_date

    def resolve_value(self, model):
        given_date = self.event_date.resolve_value(model)

        if DateInBetween(
                self.event_date, Literal(ICB_DSS_MAPPING_PERIOD_START), Literal(ICB_DSS_MAPPING_PERIOD_END)
        ).resolve_value(model):
            given_date = DSS_ICB_RECORD_START_DATE

        return icb_at(self.postcode_expr.resolve_value(model), given_date)


class SubICBFromGPPracticeCode(ModelExpression):
    """
    "The Organisation Identifier (sub-ICB Location of GP Practice), as derived from the
    submitted GENERAL MEDICAL PRACTICE CODE (PATIENT REGISTRATION) (M002010).

    Where a Null, Invalid or Default GENERAL MEDICAL PRACTICE CODE (PATIENT REGISTRATION)
    is submitted, Organisation Identifier (sub-ICB Location of GP Practice) will appear as Null."
    """

    def __init__(self, gp_practice_code: ModelExpression, event_date: ModelExpression):
        self.gp_practice_code = gp_practice_code
        self.event_date = event_date

    def resolve_value(self, model):
        return ccg_code_from_gp_practice_code(
            self.gp_practice_code.resolve_value(model),
            self.event_date.resolve_value(model)
        )


class ICBFromSubICB(ModelExpression):
    """
    The ICB as derived from a sub-ICB via rel_type=ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF
    and role_ids=ODSRoleScope.STRATEGIC_PARTNERSHIP OR ODSRoleScope.REGION_GEOGRAPHY

    This is used to derive Organisation Identifier (ICB of GP Practice) from
    Organisation Identifier (sub-ICB Location of GP Practice)
    """

    def __init__(self, sub_icb: ModelExpression, event_date: ModelExpression):
        self.sub_icb = sub_icb
        self.event_date = event_date

    def resolve_value(self, model):
        return icb_code_from_sub_icb_code(
            self.sub_icb.resolve_value(model),
            self.event_date.resolve_value(model)
        )


# See note *** in the MHSDS record inclusion logic and note ** MSD extract specification document for details of this logic
class HospitalProviderSpellSpecialCaseCommIds(ModelExpression):

    def __init__(self,
                 start_date_expr: ModelExpression,
                 end_date_expr: ModelExpression,
                 comm_provider_expr: ModelExpression,
                 fallback_expr: ModelExpression,
                 reporting_period_start_expr: ModelExpression,
                 reporting_period_end_expr: ModelExpression,
                 ):
        """
        Args:
            start_date_expr: start date expression of this record
            end_date_expr: end date expression of this record
            comm_provider_expr: start date of the hospital spell provider commissioner
            fallback_expr: expression to get the commissioner in cases of gaps
        """
        self.start_date_expr = start_date_expr
        self.end_date_expr = end_date_expr
        self.comm_provider_expr = comm_provider_expr
        self.fallback_expr = fallback_expr
        self.reporting_period_start_expr = reporting_period_start_expr
        self.reporting_period_end_expr = reporting_period_end_expr

    def resolve_value(self, model):
        rp_start = self.reporting_period_start_expr.resolve_value(model)
        rp_end = self.reporting_period_end_expr.resolve_value(model)

        start_date = self.start_date_expr.resolve_value(model)
        end_date = self.end_date_expr.resolve_value(model)
        commissioners = self.comm_provider_expr.resolve_value(model)

        commissioner_ids = set()
        for current_date in rrule(
                # Filter nulls for until as end date could be null
                DAILY, dtstart=max(start_date, rp_start), until=min(filter(None, [end_date, rp_end]))
        ):
            # Find active commissioners
            comms_for_current = {
                comm.OrgIDComm
                for comm in commissioners
                if ((comm.StartDateOrgCodeComm <= current_date.date() and not comm.EndDateOrgCodeComm)
                    or comm.StartDateOrgCodeComm <= current_date.date() <= comm.EndDateOrgCodeComm)
            }
            # If no active comm then we're in a gap period, fall back
            if not comms_for_current:
                comms_for_current = {self.fallback_expr.resolve_value(model)}
            commissioner_ids |= comms_for_current

        return list(commissioner_ids)


class NoCommIds(ModelExpression):
    def resolve_value(self, model):
        return []


class ConditionalCommIds(ModelExpression):
    def __init__(self, primary_expr: ModelExpression, secondary_expr: ModelExpression):
        self.primary_expr = primary_expr
        self.secondary_expr = secondary_expr

    def resolve_value(self, model):
        if not self.primary_expr:
            raise ValueError("primary expression is missing")

        primary_comm = self.primary_expr.resolve_value(model)
        if primary_comm:
            if isinstance(primary_comm, list):
                commissioner_ids = set()
                for item in primary_comm:
                    commissioner_ids.add(item.OrgIDComm)

                return list(commissioner_ids - {None})
            else:
                return [primary_comm]
        else:
            secondary_comm = self.secondary_expr.resolve_value(model)
            if secondary_comm:
                if isinstance(secondary_comm, list):
                    commissioner_ids = set()
                    for item in secondary_comm:
                        commissioner_ids.add(item.OrgIDComm)

                    return list(commissioner_ids - {None})
                else:
                    return [secondary_comm]

        return []


class SimpleCommIds(ModelExpression):
    def __init__(self, primary_expr: ModelExpression):
        self.primary_expr = primary_expr

    def resolve_value(self, model):
        if not self.primary_expr:
            raise ValueError("primary expression is missing")

        primary_comm = self.primary_expr.resolve_value(model)
        if primary_comm:
            if isinstance(primary_comm, list):
                commissioner_ids = set()
                for item in primary_comm:
                    commissioner_ids.add(item.OrgIDComm)

                return list(commissioner_ids - {None})
            else:
                return [primary_comm]

        return []


class UniqMonth(ModelExpression):

    def __init__(self, date_expr: ModelExpression):
        self.date_expr = date_expr

    def resolve_value(self, model):
        date_for_month = self.date_expr.resolve_value(model)
        start_date = datetime(1900, 4, 1)
        time_diff = relativedelta(date_for_month, start_date)
        return time_diff.years * 12 + time_diff.months + 1


class FileTypeExpression(ModelExpression):
    """
    if the submitted date(META.EVENT_RECEIVED_TS) is more than 1 month after
    then returns true
    i.e.
    if the Submitted date is 2018-07-10 and reporting period end date is
    2018-05-31 then returns true
    """

    def __init__(self, submitted_date_time: ModelExpression, rp_start_date: ModelExpression, dataset: str):
        self.submitted_date_time_expr = submitted_date_time
        self.rp_start_date_expr = rp_start_date
        self.dataset = dataset

    def resolve_value(self, model):
        fake_historic_windows = spark_state.feature_toggle(FeatureToggles.HISTORICAL_SUBMISSION_WINDOWS)

        submission_datetime = self.submitted_date_time_expr.resolve_value(model)  # type: datetime
        reporting_period_start_date = self.rp_start_date_expr.resolve_value(model)  # type: date

        calendar = submission_calendar(self.dataset, feature_toggles=spark_state.feature_toggles())

        return calendar.get_submission_window_type(
            reporting_period_start_date, submission_datetime, fake_historic_windows=fake_historic_windows,
            allow_pre_sw=True
        )


class FileTypeExpressionYTD(ModelExpression):
    """
    An expression that returns the number of months that have passed since the start of the reporting period
    for the given submission.
    """

    def __init__(self, submitted_date_time: ModelExpression, rp_start_date: ModelExpression, dataset: str):
        self.submitted_date_time_expr = submitted_date_time
        self.rp_start_date_expr = rp_start_date
        self.dataset = dataset

    def resolve_value(self, model):
        fake_historic_windows = spark_state.feature_toggle(FeatureToggles.HISTORICAL_SUBMISSION_WINDOWS)

        submission_datetime = self.submitted_date_time_expr.resolve_value(model)  # type: datetime
        reporting_period_start_date = self.rp_start_date_expr.resolve_value(model)  # type: date

        calendar = submission_calendar(self.dataset, feature_toggles=spark_state.feature_toggles())

        return calendar.get_submission_num_sws_since_ytd_rp_start(
            reporting_period_start_date, submission_datetime, fake_historic_windows=fake_historic_windows
        )


class RecordNumberOrUniqueID(ModelExpression):

    def __init__(self, submission_id_expr: ModelExpression, row_number_expr: ModelExpression,
                 return_decimal: bool = False):
        self.submission_id_expr = submission_id_expr
        self.row_number_expr = row_number_expr
        self.return_decimal = return_decimal

    def resolve_value(self, model):
        submission_id = str(self.submission_id_expr.resolve_value(model))
        row_number = str(self.row_number_expr.resolve_value(model)).zfill(9)
        return_decimal = self.return_decimal
        if len(row_number) > 9:
            raise ValueError(
                'Cannot format row_number: {} to 9 digits, submission_id: {}'.format(row_number, submission_id)
            )
        concatenated = int("{}{}".format(submission_id, row_number))
        if return_decimal:
            return Decimal(concatenated)
        return concatenated


class CleanEthnicCategory(ModelExpression):

    def __init__(self, ethnic_category: ModelExpression):
        self.EthnicCategory = ethnic_category

    def resolve_value(self, model):
        ethnic_category = self.EthnicCategory.resolve_value(model)

        if ethnic_category in (None, '0'):
            return '-1'

        ethic_category_strip_upper = ethnic_category.strip().upper()

        if len(ethic_category_strip_upper) == 0:
            return '-1'

        if ethic_category_strip_upper in 'ABCDEFGHJKLMNPRSZ':
            return ethic_category_strip_upper

        if ethic_category_strip_upper in ('9', '99'):
            return '99'

        else:
            return '-3'


class MatchCleanICD10Code(ModelExpression):

    def __init__(self, icd10code: ModelExpression, findschemeinuse: ModelExpression):
        self.icd10code = icd10code
        self.findschemeinuse = findschemeinuse

    @staticmethod
    def _strip_last_char_and_validate(code: str, strip_character: str, icd10) -> str:
        icd10code_cleaned_without_ad = code.rstrip(strip_character)
        icd10code_cleaned_without_ad_with_x = f"{icd10code_cleaned_without_ad}X"
        icd10code_cleaned_with_x_with_stripchar = f"{icd10code_cleaned_without_ad}X{strip_character}"

        if icd10.alt_code_is_valid(icd10code_cleaned_without_ad_with_x):
            return icd10code_cleaned_with_x_with_stripchar

        icd10code_cleaned_without_ad_with_9 = f"{icd10code_cleaned_without_ad}9"
        icd10code_cleaned_with_9_with_stripchar = f"{icd10code_cleaned_without_ad}9{strip_character}"

        if icd10.alt_code_is_valid(icd10code_cleaned_without_ad_with_9):
            return icd10code_cleaned_with_9_with_stripchar

        if (len(icd10code_cleaned_without_ad) != 3) & icd10.alt_code_is_valid(icd10code_cleaned_without_ad):
            return code

        else:
            return '-3'

    def resolve_value(self, model):
        findschemeinuse = self.findschemeinuse.resolve_value(model)
        icd10code = self.icd10code.resolve_value(model)

        if findschemeinuse == '04':
            return icd10code
        elif findschemeinuse is None or icd10code is None:
            return None
        elif findschemeinuse == '01':

            icd10code_cleaned = "".join(filter(str.isalnum, icd10code)).upper()

            if icd10code_cleaned:
                icd10 = ICD10CodeProvider()

                if len(icd10code_cleaned) == 3:
                    icd10code_cleaned_with_x = f"{icd10code_cleaned}X"

                    if icd10.alt_code_is_valid(icd10code_cleaned_with_x):
                        return icd10code_cleaned_with_x

                    icd10code_cleaned_with_9 = f"{icd10code_cleaned}9"

                    if icd10.alt_code_is_valid(icd10code_cleaned_with_9):
                        return icd10code_cleaned_with_9

                else:
                    last_char = icd10code_cleaned[-1]

                    if last_char == 'A' or last_char == 'D':
                        return self._strip_last_char_and_validate(icd10code_cleaned, last_char, icd10)

                    if icd10.alt_code_is_valid(icd10code_cleaned):
                        return icd10code_cleaned

        return '-3'


class Sort(ModelExpression):

    def __init__(self, iterable: ModelExpression, sort_key_func: Callable, descending: bool = False):
        self.iterable = iterable
        self.sort_key_func = sort_key_func
        self.descending = descending

    def resolve_value(self, model: T):
        return sorted(self.iterable.resolve_value(model),
                      key=self.sort_key_func,
                      reverse=self.descending)


class FirstRanked(ModelExpression):

    def __init__(self,
                 repeating_attribute: ModelExpression,
                 attribute_to_select: str,
                 sort_func: Callable,
                 descending: bool = False):
        self.repeating_attribute = repeating_attribute
        self.attribute_to_select = attribute_to_select
        self.sort_func = sort_func
        self.descending = descending

    def resolve_value(self, model: T) -> U:
        return GetAttr(
            First(Sort(self.repeating_attribute, self.sort_func, self.descending)),
            Literal(self.attribute_to_select)
        ).resolve_value(model)


class DeriveSnomedTerm(ModelExpression):
    """ Calculate snomed term """

    def __init__(self, snomed_code_expr: ModelExpression, point_in_time_expr: ModelExpression):
        self.snomed_code_expr = snomed_code_expr
        self.point_in_time_expr = point_in_time_expr

    def resolve_value(self, model: T):
        code = self.snomed_code_expr.resolve_value(model)
        point_in_time = self.point_in_time_expr.resolve_value(model)
        return providers.SnomedTermProvider().lookup_term(code, point_in_time)


class StringToDateTime(ModelExpression):
    def __init__(self, expression: ModelExpression, datetime_format: str):
        self.expression = expression
        self.datetime_format = datetime_format

    def resolve_value(self, model: T):
        datetime_string = self.expression.resolve_value(model)
        return datetime.strptime(datetime_string, self.datetime_format)


class MinResolved(ModelExpression):
    """ Min function for ModelExpression that resolves to a list of comparable items. """

    def __init__(self, list_expr: ModelExpression):
        self.list_expr = list_expr

    def resolve_value(self, model):
        date_list = self.list_expr.resolve_value(model)
        if date_list:
            return min(date_list)
        return None


class FilterResolved(ModelExpression):
    """ Filter function for ModelExpression that resolves to a list of items. """

    def __init__(self, list_expr: ModelExpression, filter_func: Callable):
        self.list_expr = list_expr
        self.filter_func = filter_func

    def resolve_value(self, model):
        date_list = self.list_expr.resolve_value(model)
        return list(filter(self.filter_func, date_list))


class MinFilterResolved(ModelExpression):
    """ Optimised MinResolved(FilterResolved()) function for ModelExpression that resolves to a list of items. """

    def __init__(self, list_expr: ModelExpression, filter_func: Callable,
                 mapper_func: Callable = lambda x: x):
        self.list_expr = list_expr
        self.filter_func = filter_func
        self.mapper_func = mapper_func

    def resolve_value(self, model):
        min_val = None
        for item in self.list_expr.resolve_value(model):
            mapped_item = self.mapper_func(item)
            if self.filter_func(item):
                if min_val is None or mapped_item < min_val:
                    min_val = mapped_item
        return min_val


class UnitaryAuthorityFromGPPracticeCode(ModelExpression):
    """
    The LOCAL AUTHORITY DISTRICT/UNITARY AUTHORITY (OF GP PRACTICE)
    derived from the submitted GENERAL MEDICAL PRACTICE CODE (PATIENT REGISTRATION).
    Where a Null, Invalid or Default GPCD is submitted,
    LOCAL AUTHORITY DISTRICT/UNITARY AUTHORITY (OF GP PRACTICE) will appear as Null.
    """

    def __init__(self, gp_practice_code: ModelExpression, event_date: ModelExpression):
        self.gp_practice_code = gp_practice_code
        self.event_date = event_date

    def resolve_value(self, model):
        if self.gp_practice_code.resolve_value(model) is None:
            return None
        else:
            postcode = postcode_from_org_code(
                self.gp_practice_code.resolve_value(model),
                self.event_date.resolve_value(model)
            )
            return unitary_authority_at(
                postcode,
                self.event_date.resolve_value(model)
            )


class ProviderFromSiteCode(ModelExpression):
    '''
    Find a Provider ODS code (parent code) from a site code during a specific
    point in time
    '''

    def __init__(self,
                 source_code: ModelExpression,
                 point_in_time: ModelExpression) -> None:
        self.source_code = source_code
        self.point_in_time = point_in_time

    def resolve_value(self, model) -> Optional[str]:
        return provider_code_from_site_code(
            self.source_code.resolve_value(model),
            self.point_in_time.resolve_value(model)
        )


class Coalesce(ModelExpression):
    '''Return first non null field value from list of Model Expressions'''

    def __init__(self,
                 field_list: List[ModelExpression]):
        self.field_list = field_list

    def resolve_value(self, model) -> Optional[str]:
        try:
            return next(val.resolve_value(model) for val in self.field_list if val.resolve_value(model) is not None)
        except StopIteration:
            return None


class LowerLayerSuperOutputAreaFromPostcode(ModelExpression):
    '''
    Find the Lower Layer Super Output Area (LSOA) for the patients home address
    during a specific point in time
    '''

    def __init__(self,
                 postcode: ModelExpression,
                 point_in_time: ModelExpression,
                 alternative_point_in_time: ModelExpression = Literal(None)) -> None:
        self.postcode = postcode
        self.point_in_time = point_in_time
        self.alternative_point_in_time = alternative_point_in_time

    def resolve_value(self, model) -> Optional[str]:
        __case_args_dict = {
            "postcode": self.postcode,
            "point_in_time": self.point_in_time,
            "alternative_point_in_time": self.alternative_point_in_time
        }

        return MultiCase(
            case_args_dict=__case_args_dict,
            branches=[
                (lambda case_args: case_args["postcode"] is None, Literal(None)),
                (lambda case_args: case_args["point_in_time"] is None and
                 case_args["alternative_point_in_time"] is not None, Literal(lsoa_at(
                     self.postcode.resolve_value(model),
                     self.alternative_point_in_time.resolve_value(model)
                 ))
                 )
            ],
            default=Literal(lsoa_at(
                self.postcode.resolve_value(model),
                self.point_in_time.resolve_value(model)
            ))
        ).resolve_value(model)


class MultipleColumnsNotNull(ModelExpression):
    '''
    Provide multiple columns to be checked for null values
    '''

    def __init__(self, columns: List[ModelExpression]) -> None:
        self.columns = columns

    def resolve_value(self, model) -> bool:
        if None in ([c.resolve_value(model) for c in self.columns]):
            return False
        else:
            return True


class DerivePersonBMI(ModelExpression):
    """ Calculate body mass index.

    If a BMI value was explicitly supplied it is used, otherwise a calculation is performed
    via height/weight.
    Height and weight values can either be achieved from the same activity record, or from two
    different records if both height and weight do not appear in the same one.
    In all cases, the latest record containing valid information is used.

    The input value type may or may not be numeric. Where values are missing or not convertable to a float,
    no BMI value is calculated.
    """

    def __init__(
            self,
            care_activities_expr: ModelExpression,
            uniq_id_expr: ModelExpression,
            obs_code_expr: ModelExpression,
            obs_value_expr: ModelExpression,
            measurement_unit_expr: ModelExpression,
            height_expr: ModelExpression,
            weight_expr: ModelExpression,
    ):
        self._care_activities_expr = care_activities_expr
        self._uniq_id_field_name = get_field_name(uniq_id_expr)
        self._observ_code_field_name = get_field_name(obs_code_expr)
        self._observ_value_field_name = get_field_name(obs_value_expr)
        self._measurement_unit_field_name = get_field_name(measurement_unit_expr)
        self._height_field_name = get_field_name(height_expr)
        self._weight_field_name = get_field_name(weight_expr)

    def resolve_value(self, model: T):
        care_activities = self._care_activities_expr.resolve_value(model)
        sorted_records = sorted(care_activities, key=lambda x: getattr(x, self._uniq_id_field_name), reverse=True)

        height = None
        weight = None

        for record in sorted_records:

            if getattr(record, self._observ_code_field_name) == 60621009 \
                    and getattr(record, self._measurement_unit_field_name) \
                    and (getattr(record, self._measurement_unit_field_name).lower() in ('kg/m²', 'kg/m2')) \
                    and is_non_negative_numeric(getattr(record, self._observ_value_field_name)):
                return getattr(record, self._observ_value_field_name)

            if getattr(record, self._height_field_name) and height is None:
                height = getattr(record, self._height_field_name)

            if getattr(record, self._weight_field_name) and weight is None:
                weight = getattr(record, self._weight_field_name)

            if height and weight:
                height = as_type(float, height)
                weight = as_type(float, weight)

                if height and weight and height > 0 and weight > 0:
                    bmi = weight / ((height / 100) ** 2)
                    return "{0:.2f}".format(bmi)
