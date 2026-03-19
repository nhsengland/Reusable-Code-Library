from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.uplift.p2c.p2c_detail.version_2 import schema as P2C_V2


UPLIFTS = {
    1: lambda df: simple_uplift(
        df, 1, 2, P2C_V2, new_fields=["IsWelshPreferredLanguage"]
    )
}
