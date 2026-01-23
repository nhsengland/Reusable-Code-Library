import decimal
from typing import TypeVar, Any

T = TypeVar('T', bound=Any)


def replace_decimals(obj: T) -> T:
    """
        by default dynamodb / the client pulls all numbers out as decimals .. this
        recurses and undoes the badness
        https://github.com/boto/boto3/issues/369
    Args:
        obj (T):

    Returns:
        T: T with decimals replaced
    """

    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = replace_decimals(v)
        return obj
    elif isinstance(obj, decimal.Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj
