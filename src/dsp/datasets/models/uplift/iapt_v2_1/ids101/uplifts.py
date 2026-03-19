from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.uplift.iapt_v2_1.ids101.version_4 import (
    schema as IAPT_V2_1_V4, new_fields as IAPT_V2_1_FIELDS_TO_ADD)
from dsp.datasets.models.uplift.iapt_v2_1.ids101.version_5 import (
    schema as IAPT_V2_1_V5, new_fields as IAPT_V2_1_FIELDS_TO_ADD_V5)

UPLIFTS = {
    3: lambda df: simple_uplift(df, 3, 4, IAPT_V2_1_V4, new_fields=IAPT_V2_1_FIELDS_TO_ADD),
    4: lambda df: simple_uplift(df, 4, 5, IAPT_V2_1_V5, new_fields=IAPT_V2_1_FIELDS_TO_ADD_V5)
}
