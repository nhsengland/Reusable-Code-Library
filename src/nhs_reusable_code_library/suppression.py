def central_suppression_method(valuein: int, rc: str = "5", upper: int = 5000000000) -> str:
    """
    Suppresses and rounds values using the central suppression method.

    If value is 0 then it will remain as 0.
    If value is 1-7 it will be suppressed and appear as 5.
    All other values will be rounded to the nearest 5.

    Parameters
    ----------
    valuein : int
        Metric value
    rc : str
        Replacement character if value needs suppressing
    upper : int
        Upper limit for suppression of numbers (5 billion)

    Returns
    -------
    out : str
        Suppressed value (5), 0 or rounded valuein if greater than 7

    Examples
    --------
    >>> central_suppression_method(3)
    '5'
    >>> central_suppression_method(24)
    '25'
    >>> central_suppression_method(0)
    '0'
    """
    base = 5

    if not isinstance(valuein, int):
        raise ValueError("The input: {} is not an integer.".format(valuein))

    if valuein < 0:
        raise ValueError("The input: {} is less than 0.".format(valuein))
    elif valuein == 0:
        valueout = str(valuein)
    elif valuein >= 1 and valuein <= 7:
        valueout = rc
    elif valuein > 7 and valuein <= upper:
        valueout = str(base * round(valuein / base))
    else:
        raise ValueError("The input: {} is greater than: {}.".format(valuein, upper))
    return valueout
