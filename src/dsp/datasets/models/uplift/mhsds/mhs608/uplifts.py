from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.uplift.mhsds.common.uplifts import filetype_uplift
from dsp.datasets.models.uplift.mhsds.mhs608.version_1 import schema as MHSDS_V1
from dsp.datasets.models.uplift.mhsds.mhs608.version_2 import schema as MHSDS_V2

UPLIFTS = {
    0: lambda df: simple_uplift(df, 0, 1, MHSDS_V1),
    1: lambda df: filetype_uplift(df, 1, 2, MHSDS_V2),
}
