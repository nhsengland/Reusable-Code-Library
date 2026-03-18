from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.uplift.csds.cyp301.version_2 import schema as CSDS_V2
from dsp.datasets.models.uplift.csds.cyp301.version_3 import schema as CSDS_V3

UPLIFTS = {
    1: lambda df: simple_uplift(df, 1, 2, CSDS_V2),
    2: lambda df: simple_uplift(df, 2, 3, CSDS_V3)
}
