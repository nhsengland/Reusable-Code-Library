"""Tools for string templating of rules."""

import calendar
import datetime as dt
from functools import partial
from typing import NoReturn, TypeVar, Union

import jinja2
from typing_extensions import Literal

from dve.core_engine.type_hints import JSONable, TemplateVariables


class PreserveTemplateUndefined(jinja2.Undefined):  # pylint: disable=too-few-public-methods
    """
    Preserve the original template in instances where the value cannot be populated. Whilst this
    may result in templates coming back in the FeedbackMessage object, it's more useful to know
    exactly what should have been populated rather than just returning blank values.
    """

    def __str__(self):
        return "{{" + self._undefined_name + "}}"


class RuleTemplateError(ValueError):
    """A rule template error."""


def _raise_rule_templating_error(message: str) -> NoReturn:
    """Raise a configuration error from a template."""
    raise RuleTemplateError(message)


T = TypeVar("T", bound=JSONable)
ENVIRONMENT = jinja2.Environment(
    autoescape=jinja2.select_autoescape(default_for_string=False),
    undefined=PreserveTemplateUndefined,
)
ENVIRONMENT.globals["repr"] = repr
ENVIRONMENT.globals["str"] = str
ENVIRONMENT.globals["raise"] = _raise_rule_templating_error


def add_months(date: Union[dt.date, str], n_months: int) -> dt.date:
    """Add a number of months to a date object."""
    if isinstance(date, str):
        date = dt.date.fromisoformat(date)

    current_year, current_month, current_day = date.year, date.month, date.day

    year_change, months_to_add = divmod(n_months, 12)
    new_year = current_year + year_change

    new_month = current_month + months_to_add
    if new_month > 12:
        new_year += 1
        new_month = new_month - 12

    n_days_in_month = calendar.monthrange(new_year, new_month)[1]
    return dt.date(new_year, new_month, min(current_day, n_days_in_month))


def add_years(date: Union[dt.date, str], n_years: int) -> dt.date:
    """Add a number of years to a date object."""
    if isinstance(date, str):
        date = dt.date.fromisoformat(date)

    return date.replace(year=date.year + n_years)


def days_until(
    start_date: Union[dt.date, str], end_date: Union[dt.date, str], include_end: bool = True
) -> int:
    """Calculate the number of days from one date to another, optionally
    including the end date.

    """
    if isinstance(start_date, str):
        start_date = dt.datetime.fromisoformat(start_date).date()
    if isinstance(end_date, str):
        end_date = dt.datetime.fromisoformat(end_date).date()
    return (end_date - start_date).days + int(include_end)


ENVIRONMENT.filters["add_months"] = add_months
ENVIRONMENT.filters["add_years"] = add_years
ENVIRONMENT.filters["days_until"] = days_until


def template_object(
    object_: T, variables: TemplateVariables, method: Literal["jinja", "format"] = "jinja"
) -> T:
    """Parameterise strings within collections recursively."""
    # TODO: Add some way to identify missing template variables. This has been a source of bugs.
    # mypy 'ignore' statements due to
    # https://github.com/python/mypy/issues/10003#issuecomment-770929662

    if isinstance(object_, str):
        if method == "jinja":
            return ENVIRONMENT.from_string(object_).render(variables)  # type: ignore
        return object_.format(**variables)  # type: ignore

    parameterise = partial(template_object, variables=variables, method=method)

    if isinstance(object_, list):
        return type(object_)(map(parameterise, object_))  # type: ignore
    if isinstance(object_, dict):
        return type(object_)(  # type: ignore
            zip(
                map(parameterise, object_.keys()),  # type: ignore
                map(parameterise, object_.values()),  # type: ignore
            )
        )
    return object_
