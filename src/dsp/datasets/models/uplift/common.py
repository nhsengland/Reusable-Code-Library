from typing import Tuple, Dict, Optional, Iterable, FrozenSet

from pyspark.sql import DataFrame
from pyspark.sql.types import Row, StructType

from dsp.datasets.common import Fields as CommonFields


def simple_uplift(
        df: DataFrame,
        version_from: int,
        version_to: int,
        target_schema: StructType,
        field_mapping: Optional[Dict[str, str]] = None,
        new_fields: Iterable[str] = None,
        fields_to_remove: Optional[FrozenSet[str]] = None,
        add_if_missing_fields: Iterable[str] = None,
) -> Tuple[int, DataFrame]:
    """
    An uplift function whose sole responsibility is to uplift the record version and schema with no other manipulation.
    This is particularly useful when there may be a change to only one model for a dataset - the remaining models will
    just need to apply this no-op uplift while the changed table will have specific logic to uplift the data.

    Args:
        df: the dataframe to uplift
        version_from: the record version of the passed dataframe
        version_to: the record version we are uplifting to
        target_schema: the schema of the final dataframe
        field_mapping: optional mapping of updated field names
        fields_to_remove: a frozen set of field names to remove from the target row (includes nested fields).
        new_fields: optional iterable of added fields
        add_if_missing_fields: optional iterable of fields which will be added if do not exist in the dataframe

    Returns:
        the version_to and the uplifted dataframe
    """
    if new_fields is None:
        new_fields = []

    if add_if_missing_fields:
        missing_fields = [field for field in add_if_missing_fields if field not in df.columns]
        new_fields = list(new_fields) + missing_fields

    def uplift_row(row: Row) -> Row:
        row_dict = row.asDict(recursive=True)
        if field_mapping:
            row_dict = rename_fields(row_dict, field_mapping)
        if new_fields:
            row_dict = add_fields(row_dict, new_fields)
        if fields_to_remove:
            row_dict = remove_fields(row_dict, fields_to_remove)
        assert row_dict[CommonFields.META][CommonFields.RECORD_VERSION] == version_from
        row_dict[CommonFields.META][CommonFields.RECORD_VERSION] = version_to
        return Row(**row_dict)

    uplifted_rdd = df.rdd.map(uplift_row)
    df = uplifted_rdd.toDF(target_schema)

    return version_to, df


def remove_fields(row_dict: dict, fields_to_remove: FrozenSet[str]) -> dict:
    def conv(obj):
        if isinstance(obj, dict):
            return {k: conv(v) for k, v in obj.items() if k not in fields_to_remove}
        elif isinstance(obj, list):
            return [conv(o) for o in obj]
        else:
            return obj

    return conv(row_dict)


def rename_fields(row_dict: dict, field_mappings: Dict[str, str] = None) -> dict:
    def conv(obj):
        if isinstance(obj, dict):
            return {field_mappings.get(a, a): conv(b) for a, b in obj.items()}
        elif isinstance(obj, list):
            return [conv(o) for o in obj]
        else:
            return obj

    return conv(row_dict)


def add_fields(row_dict: dict, new_fields: Iterable[str] = ()):
    def conv(obj, new_field):
        if '.' in new_field:
            head, tail = new_field.split('.', 1)
            if head in obj and isinstance(obj[head], list):
                for item in obj[head]:
                    conv(item, tail)
            else:
                conv(obj.setdefault(head, {}), tail)
        else:
            obj.setdefault(new_field, None)

    for new_field in new_fields:
        conv(row_dict, new_field)
    return row_dict
