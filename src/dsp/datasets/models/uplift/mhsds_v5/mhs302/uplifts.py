from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.uplift.mhsds_v5.mhs302.version_1 import schema as MHSDS_V5_V1
from dsp.datasets.models.uplift.mhsds_v5.mhs302.version_2 import schema as MHSDS_V5_V2
from dsp.datasets.models.uplift.mhsds_v5.mhs302.version_3 import schema as MHSDS_V5_V3
from dsp.datasets.models.uplift.mhsds_v5.mhs302.version_4 import schema as MHSDS_V5_V4

UPLIFTS = {
    0: lambda df: simple_uplift(df, 0, 1, MHSDS_V5_V1),
    1: lambda df: simple_uplift(df, 1, 2, MHSDS_V5_V2),
    2: lambda df: simple_uplift(df, 2, 3, MHSDS_V5_V3),
    3: lambda df: simple_uplift(df, 3, 4, MHSDS_V5_V4)
}
