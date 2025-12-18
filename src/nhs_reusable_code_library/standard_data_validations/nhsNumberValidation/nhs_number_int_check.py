def nhs_number_int_check(nhs_number : str):
    """This function checks if the NHS number entered is integer and having all integer numbers as described in the NHS Data Model and Dictionary.
    Args:
    nhs_number (str): The NHS number to be checked.
    Returns:
    bool: True if the NHS number is a number, False otherwise.
    Examples:
    >>> nhs_number_int_check("8429141456")
    True
    >>> nhs_number_int_check("8A42914145")
    False
    """
    #Another way of writing the test 
    # if  nhs_number.isdigit():
    #     return True
    # else:
    #     return False
    try:
        int(nhs_number)
        return True #"True - The entered nhs_number is a number."
    except ValueError:
        return False #"False - The entered nhs_number is not a number."
