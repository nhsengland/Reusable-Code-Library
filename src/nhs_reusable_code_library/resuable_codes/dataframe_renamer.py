import re
from copy import copy
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType, DataType, MapType, StructField, StructType

from src.dsp.common import canonical_name


class DataFrameRenamer:
    def __init__(self, renamer: Callable[[str], str]):
        self._apply_rename = renamer

    def _rename_field(self, field: StructField) -> StructField:
        field = copy(field)
        field.name = self._apply_rename(field.name)
        field.dataType = self._rename_nested_schema(field.dataType)
        return field

    def _rename_nested_schema(self, data_type: DataType) -> DataType:
        data_type = copy(data_type)

        if isinstance(data_type, StructType):
            naming_tracker = self.NamingConflictTracker()
            nested_fields = []
            for f in data_type.fields:
                renamed_field = self._rename_field(f)
                naming_tracker.set_renamed(f.name, renamed_field.name)
                nested_fields.append(renamed_field)
            data_type.fields = nested_fields

        elif isinstance(data_type, ArrayType):
            data_type.elementType = self._rename_nested_schema(data_type.elementType)

        elif isinstance(data_type, MapType):
            raise NotImplementedError("DataFrameRenamer: support for MapType not implemented")

        return data_type

    def _is_nested_schema(self, schema: DataType):
        nested_types = [StructType, ArrayType, MapType]
        return any(map(lambda t: isinstance(schema, t), nested_types))

    def rename(self, dataframe: DataFrame) -> DataFrame:
        col_expressions = []
        naming_tracker = self.NamingConflictTracker()

        for field in dataframe.schema:
            expr = col(field.name)

            if self._is_nested_schema(field.dataType):
                schema = self._rename_nested_schema(field.dataType)
                expr = expr.cast(schema)

            renamed = self._apply_rename(field.name)
            naming_tracker.set_renamed(field.name, renamed)
            expr = expr.alias(renamed)
            col_expressions.append(expr)

        return dataframe.select(*col_expressions)

    class NamingConflictTracker(dict):
        def set_renamed(self, original_name: str, new_name: str) -> None:
            if new_name not in self:
                self[new_name] = original_name
                return

            msg = f"DataFrameRenamer naming conflict: '{original_name}' cannot be renamed to '{new_name}'"
            if self[new_name] == new_name:
                msg = msg + f" ('{self[new_name]}' already exists)"
            else:
                msg = msg + f" ('{self[new_name]}' is already renamed to '{new_name}')"
            raise Exception(msg)


def canonical_dataframe(dataframe: DataFrame) -> DataFrame:
    renamer = DataFrameRenamer(canonical_name)
    return renamer.rename(dataframe)


def strip_xml_namespaces_in_dataframe(dataframe: DataFrame) -> DataFrame:
    def strip_ns(x: str) -> str:
        if re.match("^:", x) or len(re.findall(":", x)) > 1:
            # tag names not conforming with W3C recommendations are unsupported
            # https://www.w3.org/TR/2008/REC-xml-20081126/#sec-common-syn
            raise Exception(f"Unsupported: cannot rename xml tag '{x}'")
        return re.split("^[^:]*:", x)[-1]

    renamer = DataFrameRenamer(strip_ns)
    return renamer.rename(dataframe)
