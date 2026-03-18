from datetime import datetime

from dsp.shared.content_types import NO_SEPARATOR_DATE_FORMAT


def string_is_valid_date_format(value, date_format=NO_SEPARATOR_DATE_FORMAT, allow_blank=False) -> bool:

    if not value and allow_blank:
        return True

    if not value and not allow_blank:
        return False

    print(value, type(value))
    try:
        datetime.strptime(value, date_format)
        return True
    except ValueError:
        return False
