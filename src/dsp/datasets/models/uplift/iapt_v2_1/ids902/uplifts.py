from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.uplift.iapt_v2_1.ids902.version_4 import schema as IAPT_V2_1_V4
from dsp.datasets.models.uplift.iapt_v2_1.ids902.version_5 import schema as IAPT_V2_1_V5

UPLIFTS = {
    3: lambda df: simple_uplift(df, 3, 4, IAPT_V2_1_V4),
    4: lambda df: simple_uplift(df, 4, 5, IAPT_V2_1_V5),
}
