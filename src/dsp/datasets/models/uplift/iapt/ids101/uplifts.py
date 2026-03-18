from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.uplift.iapt.ids101.version_2 import schema as IAPT_V2

UPLIFTS = {
    1: lambda df: simple_uplift(df, 1, 2, IAPT_V2)
}
