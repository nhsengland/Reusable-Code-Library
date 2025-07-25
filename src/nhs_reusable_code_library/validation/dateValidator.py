import re


def validDate(date_string: str) -> bool:
    """
    Validates stringtype dates of type `dd/mm/yyyy`, `dd-mm-yyyy` or `dd.mm.yyyy` from
    years 1900-9999. Leap year support included.

    Parameters
    ----------
    date_string : str
        Date to be validated

    Returns
    ----------
    boolean
        Whether the date is valid or not

    Examples
    ---------
    >>> validDate("11/02/1996")
    True
    >>> validDate("29/02/2016")
    True
    >>> validDate("43/01/1996")
    False
    """

    # Let TypeError be.

    # This regex string will validate dates of type `dd/mm/yyyy`, `dd-mm-yyyy` or `dd.mm.yyyy`
    # from years 1900 - 2049. Leap year support included. Original Regex string based on
    # https://stackoverflow.com/questions/15491894/regex-to-validate-date-format-dd-mm-yyyy
    # modified to confine the year dates.
    if re.match(
        r"^(?:(?:31(\/|-|\.)(?:0?[13578]|1[02]))\1"
        + r"|(?:(?:29|30)(\/|-|\.)(?:0?[13-9]|1[0-2])\2"
        + r"))(?:(?:1[9]..|2[0][0-4].))$|^(?:29(\/|-|\.)0?2\3"
        + r"(?:(?:(?:1[6-9]|[2-9]\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]"
        + r"|[3579][26])00))))$|^(?:0?[1-9]|1\d|2[0-8])(\/|-|\.)(?:(?:0?[1-9])|(?:1[0-2]))\4"
        + r"(?:(?:1[9]..|2[0][0-4].))$",
        date_string,
        flags=0,
    ):
        return True
    else:
        return False
