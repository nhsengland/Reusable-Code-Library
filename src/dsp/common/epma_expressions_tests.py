from dsp.datasets.models.epmawspc import EPMAWellSkyPrescriptionModel

from dsp.datasets.models.epma_tests.epma_helper_tests import epmawspc_test_data
from dsp.datasets.models.epma_constants import MDDFLegallyExcludedCodes, DmdLegallyExcludedCodes

MDDF_LS_CODE = next(iter(MDDFLegallyExcludedCodes.MDDF_Legally_Sensitive_Lookup), "")
DMD_LS_CODE = next(iter(DmdLegallyExcludedCodes.Dmd_Legally_Sensitive_Lookup), "")

def test_nhs_mddf_legally_sensitive():
    record = epmawspc_test_data()
    record['META']['EVENT_ID'] = '1:1'
    record['MDDF'] = MDDF_LS_CODE
    post_derivs = EPMAWellSkyPrescriptionModel(record)

    assert post_derivs.NHSLegallyExcluded == 'Removed' if not MDDF_LS_CODE == "" else str(record['NHS'])


def test_nhs_dmd_legally_sensitive():
    record = epmawspc_test_data()
    record['META']['EVENT_ID'] = '1:1'
    record['DmdId'] = DMD_LS_CODE
    post_derivs = EPMAWellSkyPrescriptionModel(record)

    assert post_derivs.NHSLegallyExcluded == 'Removed' if not DMD_LS_CODE == "" else str(record['NHS'])


def test_nhs_retained():
    record = epmawspc_test_data()
    record['META']['EVENT_ID'] = '1:1'
    post_derivs = EPMAWellSkyPrescriptionModel(record)

    assert post_derivs.NHS == str(record['NHS'])
