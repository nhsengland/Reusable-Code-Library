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


def sensitive_legally_restricted(decision_to_admit :str, nhs_number_status_indicator :str, witheld_identity_reason :str) -> bool:
    """ This function checks if the NHS number is sensitive or legally restricted.
    Args:
    decision_to_admit (str): The decision to admit the patient.
    nhs_number_status_indicator (str): The NHS number status indicator.
    witheld_identity_reason (str): The reason for withholding the identity.
    Returns:
    bool: True if the NHS number is sensitive or legally restricted, False otherwise.
    """
    # here would go the code for determining sensitive/legally restricted, which is a common exclusion criteria in many CDS completeness rules. 
    # It would take in the decision to admint, the nhs_number status indicator and the witheld identity reason, and output true if these indicate
    # the data is sensitive / legally restricted.   

    return True


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
    >>> nhs_number_format_check("84291414567")
    False
    >>> nhs_number_format_check("84291414A6")
    False
    >>> nhs_number_format_check(8429141456)
    ValueError: Please input a 10 character string (all integers) to validate (input not a string).
    """
    if not isinstance(nhs_number, str):
        raise ValueError("Please input a 10 character string (all integers) to validate (input not a string).")
    if len(nhs_number) != 10:
        return False
    if not nhs_number.isdigit():
        return False
    return True


def palindromic_nhs_number_check(nhs_number : str) -> bool:
    """This function checks if the NHS number is in a list of palindromic numbers which though not valid as NHS Numbers, 
    can pass the validation checks.

    Args:
    nhs_number (str): The NHS number to be checked.
    Returns:
    bool: True if the NHS number is palindromic, False otherwise.
    Examples:
    >>> palindromic_nhs_number("2000000002")
    True
    >>> palindromic_nhs_number("1234567890")
    False
    """
    disallowed_nhs_numbers = ('0000000000','1111111111','2222222222','3333333333','4444444444','5555555555',
                          '6666666666','7777777777','8888888888','9999999999','1000000001','2000000002',
                          '3000000003','4000000004','5000000005','6000000006','7000000007','8000000008','9000000009')

    if not isinstance(nhs_number, str):
        raise ValueError("Please input a string to validate (input not a string).")
    return nhs_number in disallowed_nhs_numbers