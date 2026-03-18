from dsp.datasets.models.epmawspc2 import EPMAWellSkyPrescriptionModel2
from dsp.datasets.models.epmawsad2 import EPMAWellSkyAdministrationModel2
from datetime import datetime
from dsp.datasets.models.epma_tests.epma_helper_tests2 import epmawspc2_test_data
from dsp.datasets.models.epma_tests.epma_helper_tests2 import epmawsad2_test_data
from dsp.datasets.models.epma_constants import MDDFLegallyExcludedCodes, DmdLegallyExcludedCodes
from dsp.datasets.models.epmawspc2 import EPMAWellSkyPrescriptionModel2
from dsp.datasets.models.epmawsad2 import EPMAWellSkyAdministrationModel2
from dsp.datasets.models.epmawspc import EPMAWellSkyPrescriptionModel
from datetime import datetime
from dsp.datasets.models.epma_tests.epma_helper_tests import epmawspc_test_data
from dsp.datasets.models.epma_tests.epma_helper_tests2 import epmawspc2_test_data
from dsp.datasets.models.epma_constants import MDDFLegallyExcludedCodes, DmdLegallyExcludedCodes
from dsp.datasets.models.epmawspc2 import EPMAWellSkyPrescriptionModel2
import pytest

# MDDF_LS_CODE = next(iter(MDDFLegallyExcludedCodes.MDDF_Legally_Sensitive_Lookup), "")
# DMD_LS_CODE = next(iter(DmdLegallyExcludedCodes.Dmd_Legally_Sensitive_Lookup), "")

from dsp.common.expressions import ValToDateTime


# @pytest.mark.parametrize("ODS, ADMLink, Version, expectedPPK",
#                          [("ODS", "ADMLink", "Version", "ODSADMLinkVersion")])
# def test_PrescriptionPrimaryKey(ODS, ADMLink, Version, expectedPPK):
#     record = epmawspc2_test_data()
#     record['ODS'] = ODS
#     record['ADMLink'] = ADMLink
#     record['Version'] = Version
#     post_derivs = EPMAWellSkyPrescriptionModel2(record)
#     assert post_derivs.PrescriptionPrimaryKey == expectedPPK
#
#
# @pytest.mark.parametrize("RunDate, RunTime, expectedReportedDateTime",
#                          [("30/01/2020", "12:00:00", "30/01/2020 12:00:00")])
# def test_reporteddatetime(RunDate, RunTime, expectedReportedDateTime):
#     record = epmawspc2_test_data()
#     record["RunDate"] = RunDate
#     record["RunTime"] = RunTime
#     post_derivs = EPMAWellSkyPrescriptionModel2(record)
#
#     assert post_derivs.ReportedDateTime == ValToDateTime((expectedReportedDateTime),'%d/%m/%Y %H:%M:%S')
#
#
# @pytest.mark.parametrize("expectedSourceSystemType", [("WellSky")])
# def test_SourceSystemType(expectedSourceSystemType):
#     record = epmawspc2_test_data()
#     post_derivs = EPMAWellSkyPrescriptionModel2(record)
#
#     assert post_derivs.SourceSystemType == expectedSourceSystemType
#
#
@pytest.mark.parametrize("DmdId, TempMddf, expectedNHSlr",
                         [("Fine", "Fine", "9910231042"), ("Fine", "1007333", "Removed"), ("27479000", "Fine", "Removed"),
                          ("20577002", "1007333", "Removed"), ("20577002", "Fine", "Removed"),
                          ("Fine", "1007333", "Removed"), ("20577002", "1007333", "Removed")])
def test_NHSlrRemoved(DmdId, TempMddf, expectedNHSlr):
    record = epmawspc2_test_data()
    record['DmdId'] = DmdId
    record['TempMddf'] = TempMddf
    post_derivs = EPMAWellSkyPrescriptionModel2(record)

    assert post_derivs.NHSlrRemoved == expectedNHSlr


#
#
# @pytest.mark.parametrize("NHSlrRemoved, NHS, expectedNHSorCHINumber",
#                          [("Removed", 'P123', 'Removed'), ('P910231042', 'P910231042', None),
#                           ("Removed", '9910231042', 'Removed'), ("9910231042", "9910231042", '9910231042'),
#                           ])
# def test_NHSorCHINumber(NHSlrRemoved, NHS, expectedNHSorCHINumber):
#     record = epmawspc2_test_data()
#     record["NHSlrRemoved"] = NHSlrRemoved
#     record["NHS"] = NHS
#     post_derivs = EPMAWellSkyPrescriptionModel2(record)
#
#     assert post_derivs.NHSorCHINumber == expectedNHSorCHINumber
#
#
# @pytest.mark.parametrize("NHSlrRemoved, NHS, expectedLocalId",
#                          [("Removed", "P123", 'Removed'), ('P910231042', 'P910231042', 'P910231042'),
#                           ("Removed", "9910231042", 'Removed'), ("9910231042", "9910231042", None),
#                           (None, None, None)
#                           ])
# def test_LocalPatientIdentifier(NHSlrRemoved, NHS, expectedLocalId):
#     record = epmawspc2_test_data()
#     record["NHSlrRemoved"] = NHSlrRemoved
#     record["NHS"] = NHS
#     post_derivs = EPMAWellSkyPrescriptionModel2(record)
#
#     assert post_derivs.LocalPatientIdentifier == expectedLocalId
#
#
# @pytest.mark.parametrize("Type, expectedTypeAbbr", [("Inpatient", "I"), ("Outpatient", "O"), ("TTA", "D"),
#                                                     ("Other", "Other")])
# def test_TypeAbbr(Type, expectedTypeAbbr):
#     record = epmawspc2_test_data()
#     record["Type"] = Type
#     post_derivs = EPMAWellSkyPrescriptionModel2(record)
#
#     assert post_derivs.TypeAbbr == expectedTypeAbbr
#
#
# @pytest.mark.parametrize("expectedSourceDMD", ["Recorded by Trust"])
# def test_SourceDMD(expectedSourceDMD):
#     record = epmawspc2_test_data()
#     post_derivs = EPMAWellSkyPrescriptionModel2(record)
#
#     assert post_derivs.SourceDMD == expectedSourceDMD
#
#
# @pytest.mark.parametrize("expectedSourceMDDF", ["Mapped from Multilex"])
#
# def test_SourceMDDF(expectedSourceMDDF):
#     record = epmawspc2_test_data()
#     post_derivs = EPMAWellSkyPrescriptionModel2(record)
#
#     assert post_derivs.SourceMDDF == expectedSourceMDDF
#
#
# @pytest.mark.parametrize("expectedDosageSeq", ["1"])
# def test_DosageSequence(expectedDosageSeq):
#     record = epmawspc2_test_data()
#     post_derivs = EPMAWellSkyPrescriptionModel2(record)
#
#     assert post_derivs.DosageSequence == expectedDosageSeq
#
#
# @pytest.mark.parametrize("PRN, expectedDosageBool", [("Y", "TRUE"), ("N", "FALSE"), ("Other", "Other")])
# def test_DosageAsNeededBoolean(PRN, expectedDosageBool):
#     record = epmawspc2_test_data()
#     record["PRN"] = PRN
#     post_derivs = EPMAWellSkyPrescriptionModel2(record)
#
#     assert post_derivs.DosageAsNeededBoolean == expectedDosageBool
#
# @pytest.mark.parametrize("ODS, ADMLink, ScheduledAdmin, expectedAPK",
#                          [("ODS", "ADMLink", datetime(2019, 10, 14, 12, 0), "ODSADMLink2019-10-14 12:00:00")])
# def test_AdministrationPrimaryKey(ODS, ADMLink, ScheduledAdmin, expectedAPK):
#     record = epmawsad2_test_data()
#     record['ODS'] = ODS
#     record['ADMLink'] = ADMLink
#     record['ScheduledAdmin'] = ScheduledAdmin
#     post_derivs = EPMAWellSkyAdministrationModel2(record)
#     assert post_derivs.AdministrationPrimaryKey == expectedAPK
#
#
# @pytest.mark.parametrize("RunDate, RunTime, expectedReportedDateTimead",
#                          [("30/01/2020", "12:00:00", "30/01/2020 12:00:00")])
# def test_reporteddatetimead(RunDate, RunTime, expectedReportedDateTimead):
#     record = epmawsad2_test_data()
#     record["RunDate"] = RunDate
#     record["RunTime"] = RunTime
#     post_derivs = EPMAWellSkyAdministrationModel2(record)
#
#     assert post_derivs.ReportedDateTime == ValToDateTime((expectedReportedDateTimead),'%d/%m/%Y %H:%M:%S')
#
#
# @pytest.mark.parametrize("expectedSourceSystemType", ["WellSky"])
# def test_SourceSystemTypead(expectedSourceSystemType):
#     record = epmawsad2_test_data()
#     post_derivs = EPMAWellSkyAdministrationModel2(record)
#
#     assert post_derivs.SourceSystemType == expectedSourceSystemType
#
#
@pytest.mark.parametrize("DmdId, TempMddf, expectedNHSlr",
                         [("Fine", "Fine", "9910231042"), ("Fine", "1007333", "Removed"), ("27479000", "Fine", "Removed"),
                          ("20577002", "1007333", "Removed"), ("20577002", "Fine", "Removed"),
                          ("Fine", "1007333", "Removed"), ("20577002", "1007333", "Removed")])
def test_NHSlrRemovedad(DmdId, TempMddf, expectedNHSlr):
    record = epmawsad2_test_data()
    record['DmdId'] = DmdId
    record['TempMddf'] = TempMddf
    post_derivs = EPMAWellSkyAdministrationModel2(record)

    assert post_derivs.NHSlrRemoved == expectedNHSlr
# #
#
# @pytest.mark.parametrize("NHSlrRemoved, NHS, expectedNHSorCHINumber",
#                          [("Removed", 'P123', 'Removed'), ('P910231042', 'P910231042', None),
#                           ("Removed", '9910231042', 'Removed'), ("9910231042", "9910231042", '9910231042'),
#                           ])
# def test_NHSorCHINumberad(NHSlrRemoved, NHS, expectedNHSorCHINumber):
#     record = epmawsad2_test_data()
#     record["NHSlrRemoved"] = NHSlrRemoved
#     record["NHS"] = NHS
#     post_derivs = EPMAWellSkyAdministrationModel2(record)
#
#     assert post_derivs.NHSorCHINumber == expectedNHSorCHINumber
#
#
# @pytest.mark.parametrize("NHSlrRemoved, NHS, expectedLocalId",
#                          [("Removed", "P123", 'Removed'), ('P910231042', 'P910231042', 'P910231042'),
#                           ("Removed", "9910231042", 'Removed'), ("9910231042", "9910231042", None),
#                           ])
# def test_LocalPatientIdentifierad(NHSlrRemoved, NHS, expectedLocalId):
#     record = epmawsad2_test_data()
#     record["NHSlrRemoved"] = NHSlrRemoved
#     record["NHS"] = NHS
#     post_derivs = EPMAWellSkyAdministrationModel2(record)
#
#     assert post_derivs.LocalPatientIdentifier == expectedLocalId
#
#
# @pytest.mark.parametrize("expectedSource", ['Recorded by Trust'])
# def test_Sourcead(expectedSource):
#     record = epmawsad2_test_data()
#     post_derivs = EPMAWellSkyAdministrationModel2(record)
#
#     assert post_derivs.Source == expectedSource
#
# @pytest.mark.parametrize("PRN, expectedDosageBool", [("Y", "TRUE"), ("N", "FALSE"), ("Other", "Other")])
# def test_DosageAsNeededBooleanad(PRN, expectedDosageBool):
#      record = epmawsad2_test_data()
#      record["PRN"] = PRN
#      post_derivs = EPMAWellSkyAdministrationModel2(record)
#
#      assert post_derivs.DosageAsNeededBoolean == expectedDosageBool


# @pytest.mark.parametrize("MDDF, ODS, expectedTempMddf", [(None, "RKE", None), ("3009", "NAN", "3009"),
#                                                          ("3009", "RKE", "9000003"), ("MDDF", "RKE", "MDDF"),
#                                                          (None, "NAN", None)])
# def test_ConvertLegacyMddf(MDDF, ODS, expectedTempMddf):
#     record = epmawspc2_test_data()
#     record["MDDF"] = MDDF
#     record["ODS"] = ODS
#     post_derivs = EPMAWellSkyPrescriptionModel2(record)
#
#     assert post_derivs.TempMddf == expectedTempMddf


# @pytest.mark.parametrize("MDDF, ODS, expectedTempMddf", [(None, "RKE", None), ("3009", "NAN", "3009"),
#                                                          ("7333001", "RKE", "1007333"), ("MDDF", "RKE", "MDDF"),
#                                                          (None, "NAN", None)])
# def test_ConvertLegacyMddfAdmin(MDDF, ODS, expectedTempMddf):
#     record = epmawsad2_test_data()
#     record["MDDF"] = MDDF
#     record["ODS"] = ODS
#     post_derivs = EPMAWellSkyAdministrationModel2(record)
#
#     assert post_derivs.TempMddf == expectedTempMddf