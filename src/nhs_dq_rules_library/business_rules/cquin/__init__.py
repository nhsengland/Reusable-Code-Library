from functools import partial

from pyspark.sql.functions import col, lit

from dsp.dam.dq_errors import DQErrs
from dsp.datasets.common import DQRule, Fields as CommonFields
from dsp.enrichments import enrich_with_meta
from dsp.datasets.cquin.output import Fields as Output
from dsp.datasets.cquin.submitted import Fields as Submitted
from dsp.loaders import cquin_xlsx
from dsp.udfs import CCGCodeValid
from dsp.shared.constants import FT, METADATA

DATATYPE_LOADERS = {
    FT.XLSX: cquin_xlsx.load
}

RESULT = [
    (Output.META, lambda: col(CommonFields.META)),
    (Output.PROVIDER, lambda: col(Submitted.PROVIDER)),
    (Output.TYPE, lambda: col(Submitted.TYPE)),
    (Output.SERVICE, lambda: col(Submitted.SERVICE)),
    (Output.LEAD_CCG, lambda: col(Submitted.LEAD_CCG)),
    (Output.COLLECTION_NAME, lambda: col(Submitted.COLLECTION_NAME)),
    (Output.TO_LINK, lambda: col(Submitted.TO_LINK)),
    (Output.CQUIN, lambda: col(Submitted.CQUIN)),
    (Output.DATA_ITEM_REF, lambda: col(Submitted.DATA_ITEM_REF)),
    (Output.MEASUREMENT_PERIOD, lambda: col(Submitted.MEASUREMENT_PERIOD)),
    (Output.DATA, lambda: col(Submitted.DATA)),
    (Output.ELIGIBLE_ORGS, lambda: col(Output.ELIGIBLE_ORGS)),
]

STRIP_FIELDS = [
    Submitted.PROVIDER,
    Submitted.TYPE,
    Submitted.SERVICE,
    Submitted.LEAD_CCG,
    Submitted.COLLECTION_NAME,
    Submitted.TO_LINK,
    Submitted.CQUIN,
    Submitted.DATA_ITEM_REF,
    Submitted.MEASUREMENT_PERIOD,
    Submitted.DATA
]

ENRICHMENTS = [
    partial(
        enrich_with_meta, enrichments=[
            (Output.PROVIDER, lambda metadata: lit(metadata[METADATA.FILENAME][:3]))
        ]
    )
]

DQ_RULES = [
    # @TODO: we had to use a lambda here to wrap the CCGCodeValid call,
    # @TODO: as otherwise, META.EVENT_RECEIVED_TS would be canonicalised to META_EVENT_RECEIVED_TS
    # @TODO: and such field could not be resolved -- see https://nhsd-jira.digital.nhs.uk/browse/DSP-1941
    DQRule(Submitted.LEAD_CCG, [Submitted.LEAD_CCG], True,
           lambda c1: CCGCodeValid(c1, col('META.EVENT_RECEIVED_TS')), DQErrs.DQ_CCG_CODE_INVALID),
]

DELTA_JOIN_COLUMNS = [
    Output.PROVIDER,
    Output.MEASUREMENT_PERIOD
]
