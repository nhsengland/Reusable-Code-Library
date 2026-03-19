from dsp.dam.conversions import to_tinyint, to_str
from dsp.datasets.mps.response import Fields as Output
from dsp.shared.constants import FT

TYPE_COERCIONS = [
    (Output.ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC, to_tinyint, to_str),
    (Output.ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC, to_tinyint, to_str),
    (Output.ALGORITHMIC_TRACE_DOB_SCORE_PERC, to_tinyint, to_str),
    (Output.ALGORITHMIC_TRACE_GENDER_SCORE_PERC, to_tinyint, to_str),
    (Output.ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC, to_tinyint, to_str)
]

DATATYPE_LOADERS = {
    FT.CSV: lambda spark, path, metadata: spark.read.option("header", "true").option("inferSchema", "false").csv(path)
}
