def nhs_number_format_check(nhs_number : str) -> bool:
    """This function checks if the NHS number is in the correct format - n10 - as described in the NHS Data Model and Dictionary.
    Args:
    nhs_number (str): The NHS number to be checked.
    Returns:
    bool: True if the NHS number is in the correct format, False otherwise.
    Examples:
    >>> nhs_number_format_check("8429141456")
    True
    >>> nhs_number_format_check("842914145")
    False
    """
    if not isinstance(nhs_number, str) or len(nhs_number) != 10 or not nhs_number.isdigit():
        return False
    else:
        return True