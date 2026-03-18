from enum import Enum


class ExtractCountry:
    ENGLAND = 'England'
    SCOTLAND = 'Scotland'
    NORTHERN_IRELAND = 'Northern Ireland'
    WALES = 'Wales'
    ALL = 'All'


class DeltaExtractCategory(Enum):
    ENGLAND = ExtractCountry.ENGLAND
    SCOTLAND = ExtractCountry.SCOTLAND
    NORTHERN_IRELAND = ExtractCountry.NORTHERN_IRELAND
    PPDS = 'PPDS'
    WALES = ExtractCountry.WALES
    DELOITTE = 'DELOITTE'
    EDGE = 'EDGE'
    EDGE_SCOTLAND = 'EDGE_Scotland'
    EDGE_WALES = 'EDGE_Wales'
    EDGE_NORTHERN_IRELAND = 'EDGE_Northern_Ireland'
    SANGER = 'SANGER'
    WALES_LFT = "Wales_LFT"
    WALES_NON_LFT = "Wales_non_LFT"
    SCOTLAND_LFT = "Scotland_LFT"
    SCOTLAND_NON_LFT = "Scotland_non_LFT"
    NTP = "NTP"
    LIVERPOOL = 'Liverpool'
    FRIMLEY = 'Frimley'
    RTTS = 'RTTS'


SUBJECT_REGISTRATION_COUNTRY_CODE_MAP = {
    ExtractCountry.ENGLAND: 'GB-ENG',
    ExtractCountry.SCOTLAND: 'GB-SCT',
    ExtractCountry.NORTHERN_IRELAND: 'GB-NIR',
    ExtractCountry.WALES: 'GB-WLS'
}
