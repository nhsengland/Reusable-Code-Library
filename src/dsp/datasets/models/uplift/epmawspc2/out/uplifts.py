from dsp.datasets.models.uplift.epma_common import convert_mddf, add_temp_mddf_and_redact_check
from dsp.datasets.models.uplift.epmawspc2.out import version_2 as EPMAWSPC2_V2
from dsp.datasets.models.uplift.epmawspc2.out import version_3 as EPMAWSPC2_V3
from dsp.datasets.models.uplift.common import simple_uplift
from dsp.datasets.models.epma_constants import EPMAPDSEnrichmentFields

UPLIFTS = {
    1: lambda df: add_temp_mddf_and_redact_check(
        df, 1, 2, EPMAWSPC2_V2.schema
    ),
    2: lambda df: simple_uplift(df, version_from=2, version_to=3, target_schema=EPMAWSPC2_V3.schema,
                                add_if_missing_fields=[EPMAPDSEnrichmentFields.PDSDateOfBirth,
                                                       EPMAPDSEnrichmentFields.PDSPostcode,
                                                       EPMAPDSEnrichmentFields.PDSGPCode,
                                                       EPMAPDSEnrichmentFields.PDSGender,
                                                       EPMAPDSEnrichmentFields.PatientCCG,
                                                       EPMAPDSEnrichmentFields.PatientLSOA,
                                                       EPMAPDSEnrichmentFields.PatientGPCCG,
                                                       EPMAPDSEnrichmentFields.PatientGPLA,
                                                       EPMAPDSEnrichmentFields.PatientAge]
                                ),
}
