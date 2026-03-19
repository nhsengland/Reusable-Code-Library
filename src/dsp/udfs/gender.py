from typing import Union

from dsp.common import enum


class Gender(enum.LabelledEnum):
    NOT_KNOWN = '0'
    MALE = '1'
    FEMALE = '2'
    NOT_SPECIFIED = '9'

    __labels__ = {
        NOT_KNOWN: 'Not Known',
        MALE: 'Male',
        FEMALE: 'Female',
        NOT_SPECIFIED: 'Not Specified',
    }


def gender_description(code):
    if code is None:
        return None

    code = (str(code) or '').strip()
    if not code:
        return None

    return Gender(code).label


def gender_code_isvalid(gender_code: Union[None, int]):
    if gender_code is None:
        return True

    return any(gender_code == item.value for item in Gender)
