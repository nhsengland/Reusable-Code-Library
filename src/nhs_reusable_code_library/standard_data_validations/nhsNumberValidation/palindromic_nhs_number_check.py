def palindromic_nhs_number_check(nhs_number : str) -> bool:
    """This function checks if the NHS number is in a list of palindromic numbers which though not valid as NHS Numbers, 
    can pass the validation checks.

    Args:
    nhs_number (str): The NHS number to be checked.
    Returns:
    bool: True if the NHS number is palindromic, False otherwise.
    Examples:
    >>> palindromic_nhs_number_check("2000000002")
    True
    >>> palindromic_nhs_number_check("1234567890")
    False
    """
    disallowed_nhs_numbers = ('0000000000','1111111111','2222222222','3333333333','4444444444','5555555555',
                          '6666666666','7777777777','8888888888','9999999999','1000000001','2000000002',
                          '3000000003','4000000004','5000000005','6000000006','7000000007','8000000008','9000000009')

    if not isinstance(nhs_number, str):
        raise ValueError("Please input a string to validate (input not a string).")
    return nhs_number in disallowed_nhs_numbers