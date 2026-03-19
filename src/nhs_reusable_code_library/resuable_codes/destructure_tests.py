import uuid
from typing import Tuple, Dict

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *

from dsp.datasets.common import Fields as Common
from dsp.datasets.models.mhsds_v5 import CareContact, AccommodationStatus, MasterPatientIndex, get_anonymous_models, \
    Referral
# noinspection PyUnresolvedReferences
from dsp.datasets.models.mhsds_v5_tests.mhsds_v5_helper_tests import referral
from dsp.datasets.schema.helpers.destructure import Destructurer
from dsp.validation.validator import compare_results
from shared.constants import DS

table_fields = {
    'MasterPatientIndex': [
        "AgeDeath", "AgeRepPeriodEnd", "AgeRepPeriodStart", "CCGGPRes", "County", "DefaultPostcode", "ElectoralWard",
        "EthnicCategory", "Gender", "IMDQuart", "LADistrictAuth", "LSOA2011", "LanguageCodePreferred",
        "LocalPatientId", "MHS001UniqID", "MPSConfidence", "MaritalStatus", "NHSDEthnicity", "NHSNumber",
        "NHSNumberStatus", "OrgIDCCGRes", "OrgIDEduEstab", "OrgIDLocalPatientId", "OrgIDResidenceResp", "PatMRecInRP",
        "PersDeathDate", "PersonBirthDate", "Person_ID", "Postcode", "PostcodeDistrict", "RecordEndDate",
        "RecordNumber", "RecordStartDate", "RowNumber", "GenderIDCode", "GenderSameAtBirth", "EthnicCategory2021", "GPs",
        "AccommodationStatuses", "EmploymentStatuses", "PatientIndicators", "MentalHealthCareCoordinators",
        "DisabilityTypes", "CarePlanTypes", "AssistiveTechnologiesToSupportDisabilityTypes",
        "SocialAndPersonalCircumstances", "OverseasVisitorChargingCategories", "MentalHealthCurrencyModels",
        "MentalHealthActLegalStatusClassificationAssignmentPeriods", "AccommodationNationalLatest",
        "AccommodationProviderLatest" ,"EmploymentNationalLatest","EmploymentProviderLatest", "EthnicityHigher",
        "EthnicityLow", "ICRECCCG", "ICRECCCGNAME", "LDAFlag", "STPICRECCCG", "RegionICRECCCG",	"OrgIDICBRes",
        "ICRECICB","OrgIDSubICBLocResidence","ICRECSUBICB","ICRECSUBICBNAME","RegionICRECSUBICB","RegionICRECSUBICBName",
        "ICRECICBName"
    ],
    'AccommodationStatus': [
        "AccommodationType", "SettledAccommodationInd", "AccommodationTypeDate", "LocalPatientId", "MHS003UniqID",
        "RowNumber", "SCHPlacementType", "AccommodationTypeStartDate", "AccommodationTypeEndDate", "AgeAccomTypeDate"
    ],
    'CareContact': [
        "ActLocTypeCode", "AdminCatCode", "AgeCareContDate", "AttendOrDNACode", "CareContCancelDate",
        "CareContCancelReas", "CareContDate", "CareContSubj", "CareContTime", "CareContactId", "CareProfTeamLocalId",
        "ClinContDurOfCareCont", "ConsMediumUsed", "ConsType", "ContLocDistanceHome", "EarliestClinAppDate",
        "EarliestReasonOfferDate", "GroupTherapyInd", "MHS201UniqID", "OrgIDComm", "PlaceOfSafetyInd",
        "RepApptBookDate", "RepApptOfferDate", "RowNumber", "ServiceRequestId", "SiteIDOfTreat",
        "SpecialisedMHServiceCode", "TimeReferAndCareContact", "UniqCareContID", "UniqCareProfTeamID", "UniqServReqID",
        "CareContPatientTherMode", "ConsMechanismMH", "ComPeriMHPartAssessOfferInd", "PlannedCareContIndicator",
	    "ReasonableAdjustmentMade", "CareActivities", "OtherAttendances"
    ]
}

table_postfix = '_{}'.format(uuid.uuid4().hex)
table_dict = {'MasterPatientIndex': 'Referral:Patient',
              'AccommodationStatus': 'Referral:Patient:AccommodationStatuses',
              'CareContact': 'Referral:CareContacts'}
use_temp = True


@pytest.fixture
def destructurer() -> Destructurer:
    return Destructurer(DS.MHSDS_V5, postfix=table_postfix,
                        definition='dsp.datasets.mhsds_v5.pipelines.phase_2.phase_2_definition')


@pytest.fixture(scope='function')
def root_table_and_data(spark: SparkSession, referral: Dict) -> Tuple[str, DataFrame]:
    df = spark.createDataFrame([referral], Referral.get_struct())
    view_name = 'mhsds_v5_{}'.format(table_postfix)
    df.createTempView('{}'.format(view_name))
    yield view_name, df
    spark.catalog.dropTempView(view_name)


def test_get_sql_for_struct_type(
        spark: SparkSession, destructurer: Destructurer, root_table_and_data: Tuple[str, DataFrame]
):
    table_name = 'MasterPatientIndex'
    root_table, root_data = root_table_and_data
    for statement in destructurer.get_sql_for_struct_type(use_temp, table_dict, table_name, "",
                                                          table_fields, "", root_table):
        spark.sql(statement)
    actual_df = spark.table('{}{}'.format(MasterPatientIndex.__table__, table_postfix))
    expected_df = root_data.select('Patient.*')
    compare_results(actual_df, expected_df, ['RowNumber'])


def test_get_sql_for_array_type(
        spark: SparkSession, destructurer: Destructurer, root_table_and_data: Tuple[str, DataFrame]
):
    table_name = 'CareContact'
    root_table, root_data = root_table_and_data
    for statement in destructurer.get_sql_for_struct_type(use_temp, table_dict, table_name, "",
                                                          table_fields, "", root_table):
        spark.sql(statement)
    actual_df = spark.table('{}{}'.format(CareContact.__table__, table_postfix))
    expected_df = root_data.select(explode('CareContacts'))
    compare_results(actual_df, expected_df, ['RowNumber'])


def test_get_sql_for_struct_array_type(
        spark: SparkSession, destructurer: Destructurer, root_table_and_data: Tuple[str, DataFrame]
):
    table_name = 'AccommodationStatus'
    root_table, root_data = root_table_and_data
    for statement in destructurer.get_sql_for_struct_type(use_temp, table_dict, table_name, "",
                                                          table_fields, "", root_table):
        spark.sql(statement)
    actual_df = spark.table('{}{}'.format(AccommodationStatus.__table__, table_postfix))
    expected_df = root_data.select('Patient.*').select(explode('AccommodationStatuses')).select('col.*')
    compare_results(actual_df, expected_df, ['RowNumber'])


def test_get_sql_for_anonymous_table(spark: SparkSession, temp_db: str):
    anon = get_anonymous_models()

    for model in anon:
        table_name = model.__table__[:6]
        spark.sql(
            """
            CREATE TABLE {}.{} (
                {},
                EFFECTIVE_FROM TIMESTAMP,
                EFFECTIVE_TO TIMESTAMP
            )""".format(
                temp_db, table_name, model.get_fields_sql().replace("`", "")
            )
        )

    destruct = Destructurer(DS.MHSDS_V5, {m.__table__: [Common.META] for m in anon})

    for model in anon:
        sql = destruct.get_sql_for_anonymous_table(model, use_temp=False, source_db=temp_db, target_db=temp_db)
        spark.sql(sql)

    assert True


def test_field_locations_are_mapped_correctly(spark: SparkSession, temp_db: str, referral: Dict):
    # create the view to destructure
    referral['UniqMonthID'] = 12345
    referral['Patient']['Person_ID'] = '123'
    postfix = '_{}'.format(uuid.uuid4().hex)
    df = spark.createDataFrame([referral], Referral.get_struct())
    view_name = 'mhsdsv5{}'.format(postfix)
    df.createTempView('{}'.format(view_name))
    # perform destructuring
    destruct = Destructurer(DS.MHSDS_V5, postfix=postfix,
                            definition="dsp.datasets.mhsds_v5.pipelines.phase_2.phase_2_definition")
    for statement in destruct.generate_view_sql(None, None, view_name, use_temp, explicit_anonymous={}):
        spark.sql(statement)
    # check in the destructured tables that the column was mapped
    data = spark.table('mhs106dischargeplanagreement{}'.format(postfix)).collect()
    assert data[0]['UniqMonthID'] == referral['UniqMonthID']
    data = spark.table('mhs401mhactperiod{}'.format(postfix)).collect()
    assert data[0]['Person_ID'] == referral['Patient']['Person_ID']
    # cleanup
    spark.catalog.dropTempView(view_name)
