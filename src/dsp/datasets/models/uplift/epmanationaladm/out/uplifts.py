from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.uplift.epmanationaladm.out.version_2 import schema as EPMANATIONALADM_V2

UPLIFTS = {
    1: lambda df: simple_uplift(df, 1, 2, EPMANATIONALADM_V2,
                                add_if_missing_fields=['CCG_OF_REGISTRATION', 'CCG_OF_RESIDENCE', 'LA_DISTRICT_OF_REGISTRATION',
                                                       'LA_DISTRICT_OF_RESIDENCE', 'LSOA_OF_REGISTRATION', 'LSOA_OF_RESIDENCE',
                                                       'INTEGRATED_CARE_SYSTEM_OF_REGISTRATION', 'INTEGRATED_CARE_SYSTEM_OF_RESIDENCE'],
                                fields_to_remove={'LOWER_LAYER_SUPER_OUTPUT_AREA', 'PersonBirthDate'}),
}
