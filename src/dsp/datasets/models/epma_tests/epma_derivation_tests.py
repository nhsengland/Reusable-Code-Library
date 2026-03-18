from datetime import date, datetime
import pytest

from dsp.datasets.models.epmawspc import EPMAWellSkyPrescriptionModel
# noinspection PyUnresolvedReferences
from dsp.datasets.models.epma_tests.epma_helper_tests import epmawspc_test_data
from dsp.datasets.models.epma_constants import MDDFLegallyExcludedCodes, DmdLegallyExcludedCodes

MDDF_LS_CODE = next(iter(MDDFLegallyExcludedCodes.MDDF_Legally_Sensitive_Lookup), "")
DMD_LS_CODE = next(iter(DmdLegallyExcludedCodes.Dmd_Legally_Sensitive_Lookup), "")

@pytest.mark.parametrize("mddf, dmdid, expected_nhslegallyexcluded",
                         [
                             (MDDF_LS_CODE, 'DmdId1', 'Removed' if not MDDF_LS_CODE == "" else '9910231042'),
                             ('MDDF1', DMD_LS_CODE, 'Removed' if not DMD_LS_CODE == "" else '9910231042'),
                             (MDDF_LS_CODE,
                              DMD_LS_CODE,
                              'Removed' if not MDDF_LS_CODE == "" or not DMD_LS_CODE == "" else '9910231042'),
                             ('MDDF1', 'DmdId1', '9910231042')
                         ])
def test_nhs_legally_excluded(mddf, dmdid, expected_nhslegallyexcluded):
    record = epmawspc_test_data()
    record['MDDF'] = mddf
    record['DmdId'] = dmdid
    post_derivs = EPMAWellSkyPrescriptionModel(record)
    assert expected_nhslegallyexcluded == post_derivs.NHSLegallyExcluded
