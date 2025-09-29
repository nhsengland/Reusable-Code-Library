import numpy as np


def mod11_check(nhsNumber :str) -> bool:
    """This function checks if the NHS number passes the MOD 11 check.
    Args:
    nhsNumber (str): The NHS number to be checked.
    Returns:
    bool: True if the NHS number passes the MOD 11 check, False otherwise.
    Examples:
    >>> mod11_check("8429141456")
    True
    >>> mod11_check("8429141457")
    False
    """
    # here would go the mod11 code, taking the NHS Number as the input, then doing the calculation, returning true if it passes, or false if it does not.abs
    # e.g. (taken from NHS Codon Python)

    if not isinstance(nhsNumber, str):
        raise ValueError("Please input a 10 character string (all integers) to validate (input not a string).")
    if len(nhsNumber) != 10:
        raise ValueError("Please input a 10 character string (all integers) to validate (input not 10 characters).")
    # ToDo: another check that all characters all integers

    digits = [int(digit) for digit in nhsNumber]
    # Apply weighting to first 9 digits
    weighted_digits = np.dot(np.array(digits[:9]), np.arange(10, 1, -1))
    # Validity is based on the check digit, which has to be equal to `remainder`
    remainder = weighted_digits % 11
    check_digit = 11 - remainder
    if check_digit == 11:
        check_digit = 0
    if check_digit == digits[-1]:
        return True
    else:
        return False
