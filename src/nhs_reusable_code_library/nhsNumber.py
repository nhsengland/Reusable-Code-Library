import random
import numpy as np


def nhsNumberValidator(number: int) -> bool:
    """
    Validate NHS Number according to modulus 11 checks as recorded in the data dictionary.
    https://www.datadictionary.nhs.uk/data_dictionary/attributes/n/nhs/nhs_number_de.asp?shownav=1

    Parameters
    ----------
    number : int
        10 digit integer to validate.

    Returns
    ----------
    bool
        If the number passes modulus 11 checks a.k.a. is valid.

    Examples
    ---------
    >>> nhsNumberValidator(8429141456)
    True
    >>> nhsNumberValidator(8429141457)
    False
    """

    if not isinstance(number, int):
        raise ValueError("Please input a positive 10 digit integer to validate.")
    if number < 0:
        raise ValueError("Please input a postitive 10 digit integer to validate.")
    digits = [int(digit) for digit in str(number)]
    # NHS Numbers are 10 digits long.
    if not len(digits) == 10:
        raise ValueError("Please input a postitive 10 digit integer to validate.")
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


def nhsNumberGenerator(to_generate: int, random_state: int = None) -> list:
    """
    Generates up to 1M random NHS numbers compliant with modulus 11 checks as recorded
    in the data dictonary.
    https://www.datadictionary.nhs.uk/data_dictionary/attributes/n/nhs/nhs_number_de.asp?shownav=1

    Parameters
    ----------
    to_generate : int
        number of NHS numbers to generate
    random_state : int, default : None
        Optional seed for random number generation, for testing and reproducibility.

    Returns
    ----------
    generated : list
        List of randomly generated NHS numbers

    Examples
    ---------
    >>> nhsNumberGenerator(2, random_state=42)
    [8429141456, 2625792787]
    """

    if random_state:
        random.seed(random_state)
    if not isinstance(to_generate, int):
        raise ValueError("Please input a positive integer to generate numbers.")
    if to_generate > 1000000:
        raise ValueError("More than one million values requested")
    if to_generate < 0:
        raise ValueError("Please input a postitive integer to generate numbers.")

    generated = []
    while len(generated) < to_generate:
        # Random 10 digit integer, starting with non-zero digit
        number = random.randint(1000000000, 9999999999)
        if nhsNumberValidator(number):
            generated.append(number)
    return generated
