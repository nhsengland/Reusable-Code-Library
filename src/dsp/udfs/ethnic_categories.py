from typing import Optional

from dsp.common import enum


class EthnicCategory(enum.LabelledEnum):
    WHITE_BRITISH = 'A'
    WHITE_IRISH = 'B'
    WHITE_OTHER = 'C'
    MIXED_WHITE_BLACK_CARIBBEAN = 'D'
    MIXED_WHITE_BLACK_AFRICAN = 'E'
    MIXED_WHITE_ASIAN = 'F'
    MIXED_OTHER = 'G'
    ASIAN_INDIAN = 'H'
    ASIAN_PAKISTANI = 'J'
    ASIAN_BANGLADESHI = 'K'
    ASIAN_OTHER = 'L'
    BLACK_CARIBBEAN = 'M'
    BLACK_AFRICAN = 'N'
    BLACK_OTHER = 'P'
    OTHER_CHINESE = 'R'
    OTHER = 'S'
    NOT_STATED = 'Z'
    NOT_KNOWN = '99'

    __labels__ = {
        WHITE_BRITISH: 'British',
        WHITE_IRISH: 'Irish',
        WHITE_OTHER: 'Any other White background',
        MIXED_WHITE_BLACK_CARIBBEAN: 'White and Black Caribbean',
        MIXED_WHITE_BLACK_AFRICAN: 'White and Black African',
        MIXED_WHITE_ASIAN: 'White and Asian',
        MIXED_OTHER: 'Any other mixed background',
        ASIAN_INDIAN: 'Indian',
        ASIAN_PAKISTANI: 'Pakistani',
        ASIAN_BANGLADESHI: 'Bangladeshi',
        ASIAN_OTHER: 'Any other Asian background',
        BLACK_CARIBBEAN: 'Caribbean',
        BLACK_AFRICAN: 'African',
        BLACK_OTHER: 'Any other Black background',
        OTHER_CHINESE: 'Chinese',
        OTHER: 'Any other ethnic group',
        NOT_STATED: 'Not stated',
        NOT_KNOWN: 'Not Known',
    }

    @property
    def group(self):
        if self.value in ['A', 'B', 'C']:
            return EthnicGroup.WHITE
        if self.value in ['D', 'E', 'F', 'G']:
            return EthnicGroup.MIXED
        if self.value in ['H', 'J', 'K', 'L']:
            return EthnicGroup.ASIAN_OR_ASIAN_BRIT
        if self.value in ['M', 'N', 'P']:
            return EthnicGroup.BLACK_OR_BLACK_BRIT
        if self.value in ['R', 'S']:
            return EthnicGroup.OTHER
        if self.value in ['Z']:
            return EthnicGroup.NOT_STATED
        raise ValueError('No valid EthnicGroup')


class EthnicGroup(enum.Enum):
    WHITE = 'White'
    MIXED = 'Mixed'
    ASIAN_OR_ASIAN_BRIT = 'Asian or Asian British'
    BLACK_OR_BLACK_BRIT = 'Black or Black British'
    OTHER = 'Other Ethnic Groups'
    NOT_STATED = 'Not stated'


def ethnic_category_description(code) -> Optional[str]:
    code = (code or '').strip().upper()
    if not code:
        return None
    return EthnicCategory(code).label


def ethnic_category_group(code, z_as_none=False) -> Optional[str]:
    code = (code or '').strip().upper()
    none_codes = ['99', 'Z'] if z_as_none else ['99']
    if not code or code in none_codes:
        return None
    return EthnicCategory(code).group.value
