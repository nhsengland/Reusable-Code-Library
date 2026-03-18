from typing import List

from dsp.datasets.common import Fields as CommonFields
from dsp.datasets.cquin.output import Fields as OutputFields
from dsp.datasets.cquin.submitted import Fields as SubmittedFields
from dsp.common.expressions import (
    Get,
    Literal,
    Select
)
from dsp.common.structured_model import (
    DerivedAttribute,
    DerivedAttributePlaceholder,
    DSPStructuredModel,
    META,
    _META,
    SubmittedAttribute,
)

CCG_TO_ELIGIBLE_ORGS = {
    '00C': ['00C'],
    '00D': ['00C'],
    '00J': ['00C'],
    '00Q': ['0CX'],
    '00R': ['0CX'],
    '00X': ['0CX', '00X'],
    '01A': ['0CX'],
    '01C': ['0CX'],
    '01E': ['0CX', '00X'],
    '01F': ['0CX'],
    '01J': ['0CX', '01J'],
    '01K': ['0CX'],
    '01R': ['0AD', '0CX', '02D'],
    '01T': ['0CX', '01T'],
    '01V': ['0CX', '01T'],
    '01X': ['0CX', '01J'],
    '02A': ['02A'],
    '02D': ['0AD', '0CX', '02D'],
    '02E': ['0CX'],
    '02F': ['0CX'],
    '02G': ['0CX'],
    '02M': ['0CX'],
    '02R': ['02R'],
    '02W': ['02R'],
    '03T': ['03T'],
    '03V': ['0DJ'],
    '03W': ['0CX'],
    '04C': ['0CX', '04C'],
    '04D': ['03T'],
    '04E': ['04E'],
    '04G': ['0DJ'],
    '04H': ['04E'],
    '04L': ['04L'],
    '04M': ['04L'],
    '04N': ['04L'],
    '04Q': ['03T'],
    '04V': ['0CX', '04C'],
    '04X': ['0CX'],
    '04Y': ['0CX', '04Y'],
    '05C': ['0CX'],
    '05D': ['0CX'],
    '05F': ['0CX'],
    '05G': ['0CX', '05G'],
    '05J': ['0CX', '05J'],
    '05L': ['0CX'],
    '05N': ['0CX'],
    '05P': ['0CX'],
    '05Q': ['0CX', '04Y'],
    '05T': ['0CX', '05J'],
    '05V': ['0CX', '04Y'],
    '05W': ['0CX', '05G'],
    '05X': ['0CX'],
    '05Y': ['0CX'],
    '06A': ['0CX'],
    '06D': ['0CX', '05J'],
    '06F': ['0DF'],
    '06L': ['06L'],
    '06M': ['0DJ'],
    '06P': ['0DJ'],
    '06V': ['0DJ'],
    '06W': ['0DJ'],
    '06Y': ['0DJ'],
    '07G': ['0DJ'],
    '07J': ['0DJ'],
    '07K': ['06L'],
    '07L': ['0DJ', '08F'],
    '07M': ['0DJ'],
    '07N': ['0DJ'],
    '07Q': ['0DJ'],
    '07R': ['0DJ'],
    '07T': ['0DJ'],
    '07V': ['0DJ'],
    '07X': ['0DJ'],
    '08A': ['0DJ'],
    '08D': ['0DJ'],
    '08F': ['0DJ', '08F'],
    '08H': ['0DJ'],
    '08J': ['0DJ'],
    '08K': ['0DJ'],
    '08L': ['0DJ'],
    '08M': ['0DJ'],
    '08N': ['0DJ'],
    '08P': ['0DJ'],
    '08Q': ['0DJ'],
    '08R': ['0DJ'],
    '08T': ['0DJ'],
    '08V': ['0DJ'],
    '08W': ['0DJ'],
    '08X': ['0DJ'],
    '09C': ['0DJ'],
    '09D': ['0DF'],
    '09E': ['0DJ'],
    '09F': ['0DF', '09F'],
    '09G': ['0DF'],
    '09H': ['0DF', '09H'],
    '09J': ['09J'],
    '09L': ['0DF'],
    '09P': ['0DF', '09F'],
    '09X': ['0DF', '09H'],
    '09Y': ['0DJ'],
    '10C': ['0DF'],
    '10D': ['09J'],
    '10G': ['0DF', '10G'],
    '10H': ['0DF', '10H'],
    '10J': ['0DF'],
    '10K': ['0DF', '10V'],
    '10L': ['0DF'],
    '10M': ['0DF', '10M'],
    '10N': ['0DF', '10M'],
    '10Q': ['0DF'],
    '10R': ['0DF'],
    '10T': ['0DF', '10G'],
    '10V': ['0DF', '10V'],
    '10W': ['0DF', '10M'],
    '10X': ['0DF'],
    '10Y': ['0DF', '10H'],
    '11A': ['0DF'],
    '11C': ['0DF', '10G'],
    '11D': ['0DF', '10M'],
    '11E': ['0DF'],
    '11H': ['0DF', '11H'],
    '11J': ['0DF'],
    '11M': ['0DF'],
    '11T': ['0DF', '11H'],
    '11X': ['0DF'],
    '12A': ['0DF', '11H'],
    '13P': ['0CX'],
    '14L': ['02A'],
    '99A': ['0CX'],
    '99D': ['03T'],
    '99F': ['0DJ'],
    '99G': ['0DJ'],
    '99H': ['0DJ'],
    '99J': ['0DJ'],
    '99K': ['0DF'],
    '99M': ['0DF'],
    '99N': ['0DF']
}


class _CQUIN(DSPStructuredModel):
    META = SubmittedAttribute(CommonFields.META, _META)  # type: SubmittedAttribute
    PROVIDER = SubmittedAttribute(SubmittedFields.PROVIDER, str)  # type: SubmittedAttribute
    TYPE = SubmittedAttribute(SubmittedFields.TYPE, str)  # type: SubmittedAttribute
    SERVICE = SubmittedAttribute(SubmittedFields.SERVICE, str)  # type: SubmittedAttribute
    LEAD_CCG = SubmittedAttribute(SubmittedFields.LEAD_CCG, str)  # type: SubmittedAttribute
    COLLECTION_NAME = SubmittedAttribute(SubmittedFields.COLLECTION_NAME, str)  # type: SubmittedAttribute
    TO_LINK = SubmittedAttribute(SubmittedFields.TO_LINK, str)  # type: SubmittedAttribute
    CQUIN = SubmittedAttribute(SubmittedFields.CQUIN, str)  # type: SubmittedAttribute
    DATA_ITEM_REF = SubmittedAttribute(SubmittedFields.DATA_ITEM_REF, str)  # type: SubmittedAttribute
    MEASUREMENT_PERIOD = SubmittedAttribute(SubmittedFields.MEASUREMENT_PERIOD, str)  # type: SubmittedAttribute
    DATA = SubmittedAttribute(SubmittedFields.DATA, str)  # type: SubmittedAttribute

    ELIGIBLE_ORGS = DerivedAttributePlaceholder(OutputFields.ELIGIBLE_ORGS,
                                                List[str])  # type: DerivedAttributePlaceholder


class CQUIN(_CQUIN):
    __concrete__ = True

    META = SubmittedAttribute('META', META)  # type: SubmittedAttribute

    ELIGIBLE_ORGS = DerivedAttribute(OutputFields.ELIGIBLE_ORGS, List[str],
                                     Get(Literal(CCG_TO_ELIGIBLE_ORGS),
                                         Select(_CQUIN.LEAD_CCG)))  # type: DerivedAttribute
