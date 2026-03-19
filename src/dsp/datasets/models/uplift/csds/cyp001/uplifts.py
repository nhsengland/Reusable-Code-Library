from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.uplift.csds.cyp001.version_2 import \
    schema as CSDS_V2, \
    new_fields as new_fields_v2, \
    fields_to_remove as fields_to_remove_v2
from dsp.datasets.models.uplift.csds.cyp001.version_3 import \
    schema as CSDS_V3, \
    new_fields as new_fields_v3

UPLIFTS = {
    1: lambda df: simple_uplift(df, 1, 2, CSDS_V2, new_fields=new_fields_v2, fields_to_remove=fields_to_remove_v2),
    2: lambda df: simple_uplift(df, 2, 3, CSDS_V3, new_fields=new_fields_v3)
}
