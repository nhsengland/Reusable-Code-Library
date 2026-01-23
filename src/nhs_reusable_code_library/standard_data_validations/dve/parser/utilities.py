"""Useful helper functions.

This is functionality which may be useful for multiple readers,
but isn't quite universal enough to be in e.g. a base class.

"""

from collections import defaultdict
from collections.abc import Iterable, Iterator
from itertools import tee
from typing import TypeVar, Union, overload

from pyspark.sql.types import ArrayType, StringType, StructField, StructType

T = TypeVar("T")
TemplateElement = Union[None, list["TemplateElement"], dict[str, "TemplateElement"]]  # type: ignore
"""The base types used in the template row."""
TemplateRow = dict[str, "TemplateElement"]  # type: ignore
"""The type of a template row."""


def peek(iterable: Iterable[T]) -> tuple[T, Iterator[T]]:
    """Peek the first item from an iterable, returning the first item
    and an iterator representing the state of the iterable _before_
    the first item was taken.

    """
    current, clone = tee(iterable, 2)
    return next(clone), current


@overload
def template_row_to_spark_schema(template_element: TemplateRow) -> StructType: ...


@overload
def template_row_to_spark_schema(
    template_element: TemplateElement,
) -> Union[ArrayType, StringType, StructType]: ...


def template_row_to_spark_schema(template_element):
    """Get a Spark schema from a template row."""
    # Should we implement the full logic from dve.core_engine.spark_helpers here?
    if template_element is None:
        return StringType()
    if isinstance(template_element, list):
        if not template_element:
            nested_type = None
        elif len(template_element) == 1:
            nested_type = template_element[0]
        else:
            raise ValueError(f"Nested array longer than 1: {template_element!r}")
        return ArrayType(template_row_to_spark_schema(nested_type))
    if not isinstance(template_element, dict):
        raise TypeError(f"Must be dict, list, or None, got {template_element!r}")

    fields = []
    for field_name, nested_type in template_element.items():
        fields.append(StructField(str(field_name), template_row_to_spark_schema(nested_type)))
    return StructType(fields)


def parse_template_row(field_names: Iterable[str]) -> TemplateRow:
    """Parse a template row.

    Field names can be separated by level using '.', and wrapping the
    field name in square brackets indicates that the item is expected
    to be an array.

    >>> parse_template_row(['name'])
    {'name': None}
    >>> parse_template_row(['[name]'])
    {'name': [None]}
    >>> parse_template_row(['name', 'name.nested'])
    {'name': {'nested': None}}
    >>> parse_template_row(['[name]', 'name.nested'])
    {'name': [{'nested': None}]}
    >>> parse_template_row(['name', '[name.nested_list]'])
    {'name': {'nested_list': [None]}}
    >>> parse_template_row(['[name]', '[name.nested_list]'])
    {'name': [{'nested_list': [None]}]}

    """
    array_levels = set()
    sub_levels_by_level: dict[str, list[str]] = defaultdict(list)

    for name in field_names:
        is_array = name.startswith("[")
        name = name.strip("[]")

        if "." not in name:
            # Add the key to the defaultdict, if it's not already added
            sub_levels_by_level[name]  # pylint: disable=pointless-statement
            if is_array:
                array_levels.add(name)
        else:
            level, sub_level = name.split(".", 1)
            if is_array:
                sub_level = f"[{sub_level}]"
            sub_levels_by_level[level].append(sub_level)

    row = {}
    for level, sub_level_names in sub_levels_by_level.items():
        value: TemplateElement = None
        if sub_level_names:
            value = parse_template_row(sub_level_names)

        if level in array_levels:
            value = [value]
        row[level] = value

    return row
