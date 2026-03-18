# pylint: disable=invalid-name

# Algorithm details: https://en.wikipedia.org/wiki/Verhoeff_algorithm


def checksum(digits: str) -> int:
    """
    Retrieve the Verhoeff checksum of a string of digits

    Args:
        digits (str): The string of digits to check

    Returns:
        int: The checksum of the given string
    """
    c = 0
    for index, char in enumerate("0" + digits[::-1]):
        c = _d(c, _p(index, int(char)))
    return _inv(c)


def verify(digits: str):
    """
    Validate a string of digits with an appended Verhoeff checksum

    Args:
        digits (str): The string of digits to verify

    Returns:
        bool: Whether the last digit of the string is the correct checksum
    """
    return checksum(digits[:-1]) == int(digits[-1])


_MULTIPLICATION_TABLE = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    [1, 2, 3, 4, 0, 6, 7, 8, 9, 5],
    [2, 3, 4, 0, 1, 7, 8, 9, 5, 6],
    [3, 4, 0, 1, 2, 8, 9, 5, 6, 7],
    [4, 0, 1, 2, 3, 9, 5, 6, 7, 8],
    [5, 9, 8, 7, 6, 0, 4, 3, 2, 1],
    [6, 5, 9, 8, 7, 1, 0, 4, 3, 2],
    [7, 6, 5, 9, 8, 2, 1, 0, 4, 3],
    [8, 7, 6, 5, 9, 3, 2, 1, 0, 4],
    [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
]

_INVERSE_TABLE = [0, 4, 3, 2, 1, 5, 6, 7, 8, 9]

_PERMUTATION_TABLE = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    [1, 5, 7, 6, 2, 8, 3, 0, 9, 4],
    [5, 8, 0, 3, 7, 9, 6, 1, 4, 2],
    [8, 9, 1, 6, 0, 4, 3, 5, 2, 7],
    [9, 4, 5, 3, 1, 2, 6, 8, 7, 0],
    [4, 2, 8, 6, 5, 7, 3, 9, 0, 1],
    [2, 7, 9, 3, 8, 0, 6, 4, 1, 5],
    [7, 0, 4, 6, 9, 1, 3, 2, 5, 8]
]


def _d(j: int, k: int) -> int:
    return _MULTIPLICATION_TABLE[j][k]


def _p(pos: int, num: int) -> int:
    return _PERMUTATION_TABLE[pos % 8][num]


def _inv(j: int) -> int:
    return _INVERSE_TABLE[j]
