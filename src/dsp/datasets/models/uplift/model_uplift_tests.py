import copy
import importlib
import json
from hashlib import sha1
from typing import Dict, List
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField
from dsp.dam import current_record_version
from dsp.datasets.common import Fields as CommonFields
from dsp.datasets.models.uplift.model_uplift import uplift
from dsp.common.structured_model import DSPStructuredModel
from dsp.shared.constants import DS

import dsp.datasets.models.csds as csds
from testdata.csds import model_uplift_tests_input_data as csds_testdata
from dsp.datasets.definitions.csds.submission_constants import ALL_TABLE_PREFIXES as CSDS_TP
from dsp.datasets.models.uplift.csds.cyp001.version_1 import schema as CYP001_V1

import dsp.datasets.models.iapt as iapt
from dsp.datasets.definitions.iapt.submission_constants import ALL_TABLE_PREFIXES as IAPT_TP

import dsp.datasets.models.iapt_v2_1 as iapt_v2_1
from testdata.iapt_v2_1 import model_uplift_tests_input_data as iapt_v2_1_testdata
from dsp.datasets.definitions.iapt_v2_1.submission_constants import ALL_TABLE_PREFIXES as IAPT_V2_1_TP
from dsp.datasets.models.uplift.iapt_v2_1.ids101.version_3 import schema as IDS101_V2_1_V3
from dsp.datasets.models.uplift.iapt_v2_1.ids902.version_3 import schema as IDS902_V2_1_V3

# noinspection PyUnresolvedReferences
import dsp.datasets.models.mhsds_v5 as mhsds_v5
from testdata.mhsds_v5 import model_uplift_tests_input_data as mhsds_v5_testdata
import dsp.datasets.models.msds as msds
# noinspection PyUnresolvedReferences
import dsp.datasets.models.msds_tests.msds_helper_tests as msds_helper
from testdata.msds import model_uplift_tests_input_data as msds_testdata

import dsp.datasets.models.pcaremeds as pcaremeds
import dsp.datasets.models.p2c as p2c
import dsp.datasets.models.p2c_tests.p2c_helper_tests as p2c_helper
import dsp.datasets.models.epmawspc2 as epmawspc2
import dsp.datasets.models.epmawsad2 as epmawsad2
import dsp.datasets.models.epmanationaladm as epmanationaladm
import dsp.datasets.models.epmanationalpres as epmanationalpres

# noinspection PyUnresolvedReferences
import dsp.datasets.models.pcaremeds_tests.pcaremeds_helper_tests as pcaremeds_helper
from dsp.datasets.models.epma_tests.epma_helper_tests2 import epmawspc2_test_data, \
    epmawsad2_test_data
from dsp.datasets.models.epma_national_tests.epma_admin_helper_tests import epmanationaladm_test_data
from dsp.datasets.models.epma_national_tests.epma_presc_helper_tests import epmanationalpresc_test_data
from dsp.datasets.definitions.mhsds_v5.submission_constants import ALL_TABLE_PREFIXES as MHSDS_V5_TP
from dsp.datasets.definitions.msds.submission_constants import ALL_TABLE_PREFIXES as MSDS_TP
from dsp.datasets.definitions.pcaremeds.submission_constants import \
    ALL_TABLE_PREFIXES as PCAREMEDS_TP
from dsp.datasets.definitions.epma.epmawspc2.submission_constants import \
    ALL_TABLE_PREFIXES as EPMAWSPC2_TP
from dsp.datasets.definitions.epma.epmawsad2.submission_constants import \
    ALL_TABLE_PREFIXES as EPMAWSAD2_TP
from dsp.datasets.definitions.epma_national.epmanationaladm.submission_constants import ALL_TABLE_PREFIXES as EPMANATIONALADM_TP
from dsp.datasets.definitions.epma_national.epmanationalpres.submission_constants import ALL_TABLE_PREFIXES as EPMANATIONALPRES_TP
from dsp.datasets.models.uplift.mhsds_v5.mhs101.version_1 import schema as MHS101_V5_V1
from dsp.datasets.models.uplift.mhsds_v5.mhs301.version_1 import schema as MHS301_V5_V1
from dsp.datasets.models.uplift.mhsds_v5.mhs608.version_1 import schema as MHS601_V5_V1
from dsp.datasets.models.uplift.mhsds_v5.mhs901.version_1 import schema as MHS901_V5_V1
from dsp.datasets.models.uplift.mhsds_v5.mhs302.version_1 import schema as MHS302_V5_V1

# noinspection PyUnresolvedReferences
from dsp.datasets.models.uplift.msds.msd101.version_5 import schema as MSD101_V5
from dsp.datasets.models.uplift.msds.msd601.version_5 import schema as MSD601_V5
from dsp.datasets.models.uplift.msds.msd602.version_5 import schema as MSD602_V5
from dsp.datasets.models.uplift.msds.msd901.version_5 import schema as MSD901_V5

# noinspection PyUnresolvedReferences
from dsp.datasets.models.uplift.pcaremeds.out.version_1 import schema as PCAREMEDS_V1

from dsp.datasets.models.uplift.p2c.p2c_detail.version_2 import schema as P2C_V2
from dsp.datasets.p2c.schema import TABLE_PREFIXES as P2C_TP

from dsp.datasets.models.uplift.epmawsad2.out.version_1 import schema as EPMAWSAD2_V1
from dsp.datasets.models.uplift.epmanationaladm.out.version_1 import schema as EPMANATIONALADM_V1
from dsp.datasets.models.uplift.epmanationalpres.out.version_1 import schema as EPMANATIONALPRES_V1

from dsp.datasets.epma.epmawspc2.epmawspc2 import EXCLUDED_FIELDS as epmawspc2_EXCLUDED_FIELDS
from dsp.datasets.epma.epmawsad2.epmawsad2 import EXCLUDED_FIELDS as epmawsad2_EXCLUDED_FIELDS
from dsp.datasets.epma_national.constants import ExcludedColumns as EPMA_NATIONAL_EXCLUDED_FIELDS

from dsp.datasets.models.uplift.epmawspc2.out.version_1 import schema as EPMAWSPC2_V1



@pytest.mark.parametrize(
    "dataset_id, table_prefixes, model, data, first_live_schema, "
    "first_live_version, model_excluded_fields",
    [
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.Referral, copy.deepcopy(mhsds_v5_testdata.referral_dict),
         MHS101_V5_V1, 0, list()),
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.GroupSession,
         copy.deepcopy(mhsds_v5_testdata.group_session_dict), MHS301_V5_V1, 0, list()),
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.AnonymousSelfAssessment,
         copy.deepcopy(mhsds_v5_testdata.anonymous_self_assessment_dict), MHS601_V5_V1, 0, list()),
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.StaffDetails,
         copy.deepcopy(mhsds_v5_testdata.staff_details_dict), MHS901_V5_V1, 0, list()),
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.MentalHealthDropInContact,
         copy.deepcopy(mhsds_v5_testdata.mental_health_drop_in_contact_dict), MHS302_V5_V1, 0,
         list()),

        (DS.MSDS, MSDS_TP, msds.PregnancyAndBookingDetails,
         copy.deepcopy(msds_testdata.pregnancy_and_booking_details_dict), MSD101_V5, 5, list()),
        (DS.MSDS, MSDS_TP, msds.AnonSelfAssessment,
         copy.deepcopy(msds_testdata.anonymous_self_assessment_dict), MSD601_V5, 5, list()),
        (DS.MSDS, MSDS_TP, msds.AnonFindings, copy.deepcopy(msds_testdata.anonymous_finding_dict),
         MSD602_V5, 5, list()),
        (DS.MSDS, MSDS_TP, msds.StaffDetails, copy.deepcopy(msds_testdata.staff_details_dict),
         MSD901_V5, 5, list()),

        (DS.IAPT_V2_1, IAPT_V2_1_TP, iapt_v2_1.Referral,
         copy.deepcopy(iapt_v2_1_testdata.referral_dict), IDS101_V2_1_V3,
         3, list()),
        (DS.IAPT_V2_1, IAPT_V2_1_TP, iapt_v2_1.CarePersonnelQualification,
         copy.deepcopy(iapt_v2_1_testdata.care_personnel_qualification_dict), IDS902_V2_1_V3,
         3, list()),

        (DS.CSDS, CSDS_TP, csds.MPI, copy.deepcopy(csds_testdata.master_patient_index_dict), CYP001_V1, 1, list()),

        (DS.PCAREMEDS, PCAREMEDS_TP, pcaremeds.PrimaryCareMedicineModel,
         pcaremeds_helper.pcaremeds_test_data(include_derivations=True,
                                              derivations_to_exclude=['AgeBands', 'ItemCount',
                                                                      'PaidIndicator']),
         PCAREMEDS_V1, 1, list()),

        (DS.P2C, P2C_TP, p2c.P2CModel, copy.deepcopy(p2c_helper.p2c_dict), P2C_V2, 1, list()),

        (DS.EPMAWSPC2, EPMAWSPC2_TP, epmawspc2.EPMAWellSkyPrescriptionModel2, epmawspc2_test_data(),
         EPMAWSPC2_V1, 1,
         epmawspc2_EXCLUDED_FIELDS),
        (DS.EPMAWSAD2, EPMAWSAD2_TP, epmawsad2.EPMAWellSkyAdministrationModel2,
         epmawsad2_test_data(), EPMAWSAD2_V1, 1,
         epmawsad2_EXCLUDED_FIELDS),
        (DS.EPMANATIONALADM, EPMANATIONALADM_TP, epmanationaladm.EPMAAdministrationModel,
         epmanationaladm_test_data(derivations_to_exclude=['LSOA_OF_REGISTRATION', 'LSOA_OF_RESIDENCE',
                                                           'CCG_OF_REGISTRATION', 'CCG_OF_RESIDENCE',
                                                           'LA_DISTRICT_OF_REGISTRATION', 'LA_DISTRICT_OF_RESIDENCE',
                                                           'INTEGRATED_CARE_SYSTEM_OF_REGISTRATION',
                                                           'INTEGRATED_CARE_SYSTEM_OF_RESIDENCE']),
         EPMANATIONALADM_V1, 1, EPMA_NATIONAL_EXCLUDED_FIELDS.ADMIN_COLS),
        (DS.EPMANATIONALPRES, EPMANATIONALPRES_TP, epmanationalpres.EPMAPrescriptionModel,
         epmanationalpresc_test_data(derivations_to_exclude=['LSOA_OF_REGISTRATION', 'LSOA_OF_RESIDENCE',
                                                           'CCG_OF_REGISTRATION', 'CCG_OF_RESIDENCE',
                                                           'LA_DISTRICT_OF_REGISTRATION', 'LA_DISTRICT_OF_RESIDENCE',
                                                           'INTEGRATED_CARE_SYSTEM_OF_REGISTRATION',
                                                           'INTEGRATED_CARE_SYSTEM_OF_RESIDENCE']),
         EPMANATIONALPRES_V1, 1, EPMA_NATIONAL_EXCLUDED_FIELDS.PRESC_COLS)
    ]
)
def test_generic_uplift_chain_to_latest(spark: SparkSession, dataset_id: str, table_prefixes: Dict,
                                        model: DSPStructuredModel, data: Dict,
                                        first_live_schema: StructType,
                                        first_live_version: int,
                                        model_excluded_fields: List):
    """Just tests that we can uplift from given version to the latest version"""
    data[CommonFields.META][CommonFields.RECORD_VERSION] = first_live_version

    df = spark.createDataFrame([data], first_live_schema)

    head = df.head()
    assert head[CommonFields.META][CommonFields.RECORD_VERSION] == first_live_version

    df = uplift(spark, df, dataset_id, table_prefixes[model.__table__])

    head = df.head()

    latest_version = current_record_version(dataset_id)

    assert head[CommonFields.META][CommonFields.RECORD_VERSION] == latest_version

    assert df.schema.jsonValue() == StructType(
        [sfields for sfields in model.get_struct() if
         sfields.name not in model_excluded_fields]).jsonValue()


@pytest.mark.parametrize(
    "dataset_id, table_prefixes, model, model_excluded_fields",
    [
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.Referral, list()),
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.GroupSession, list()),
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.AnonymousSelfAssessment, list()),
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.StaffDetails, list()),
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.MentalHealthDropInContact, list()),

        (DS.MSDS, MSDS_TP, msds.PregnancyAndBookingDetails, list()),
        (DS.MSDS, MSDS_TP, msds.AnonSelfAssessment, list()),
        (DS.MSDS, MSDS_TP, msds.AnonFindings, list()),
        (DS.MSDS, MSDS_TP, msds.StaffDetails, list()),

        (DS.IAPT, IAPT_TP, iapt.Referral, list()),
        (DS.IAPT, IAPT_TP, iapt.CarePersonnelQualification, list()),

        (DS.IAPT_V2_1, IAPT_V2_1_TP, iapt_v2_1.Referral, list()),
        (DS.IAPT_V2_1, IAPT_V2_1_TP, iapt_v2_1.CarePersonnelQualification, list()),

        (DS.CSDS, CSDS_TP, csds.MPI, list()),
        (DS.CSDS, CSDS_TP, csds.GroupSession, list()),
        (DS.CSDS, CSDS_TP, csds.AnonSelfAssessment, list()),
        (DS.CSDS, CSDS_TP, csds.StaffDetails, list()),

        (DS.PCAREMEDS, PCAREMEDS_TP, pcaremeds.PrimaryCareMedicineModel, list()),

        (DS.P2C, P2C_TP, p2c.P2CModel, list()),

        (DS.EPMAWSPC2, EPMAWSPC2_TP, epmawspc2.EPMAWellSkyPrescriptionModel2,
         epmawspc2_EXCLUDED_FIELDS),
        (DS.EPMAWSAD2, EPMAWSAD2_TP, epmawsad2.EPMAWellSkyAdministrationModel2,
         epmawsad2_EXCLUDED_FIELDS),
        (DS.EPMANATIONALADM, EPMANATIONALADM_TP, epmanationaladm.EPMAAdministrationModel, EPMA_NATIONAL_EXCLUDED_FIELDS.ADMIN_COLS),
        (DS.EPMANATIONALPRES, EPMANATIONALPRES_TP, epmanationalpres.EPMAPrescriptionModel, EPMA_NATIONAL_EXCLUDED_FIELDS.PRESC_COLS),
    ]
)
def test_that_schema_for_current_version_matches_latest_numbered_version(dataset_id: str,
                                                                         table_prefixes: Dict,
                                                                         model: DSPStructuredModel,
                                                                         model_excluded_fields: List):
    """
    Checks that we have defined the model schema for the latest version so that, when the current version is uplifted,
    we know that we have model schema for this version
    """
    table_prefix = table_prefixes[model.__table__]
    latest_version = current_record_version(dataset_id)
    module = importlib.import_module("dsp.datasets.models.uplift.{}.{}.version_{}".format(
        dataset_id, table_prefix.lower(), latest_version)
    )
    versioned_json = getattr(module, 'schema_json')
    current_json = StructType(
        [sfields for sfields in model.get_struct() if
         not sfields.name in model_excluded_fields]).jsonValue()
    versioned_hash = sha1(json.dumps(versioned_json).encode('utf-8')).hexdigest()
    current_hash = sha1(json.dumps(current_json).encode('utf-8')).hexdigest()
    assert versioned_hash == current_hash


@pytest.mark.parametrize(
    "dataset_id, table_prefix, version, sha1_hash",
    [
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.Referral.__table__], 1,
         "1bee1dbc8031432e948270a63e326661bec28557"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.Referral.__table__], 2,
         "d1aeb8ead8edd73388ca34d18781f05cfc5f3e7c"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.Referral.__table__], 3,
         "8e5db7f912f514433c6e55a36d9ce48d6c7f365f"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.Referral.__table__], 4,
         "8dca6d878da00b970efb2fb48d33600e0db4075f"),

        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.GroupSession.__table__], 1,
         "5c4b872cb1da6deb11bd002bce39bcebd5cd3424"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.GroupSession.__table__], 2,
         "5c4b872cb1da6deb11bd002bce39bcebd5cd3424"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.GroupSession.__table__], 3,
         "5c4b872cb1da6deb11bd002bce39bcebd5cd3424"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.GroupSession.__table__], 4,
         "5c4b872cb1da6deb11bd002bce39bcebd5cd3424"),
        #
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.AnonymousSelfAssessment.__table__], 1,
         "cd9af5f9505f41b6b61834a3dac07acf41305801"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.AnonymousSelfAssessment.__table__], 2,
         "cd9af5f9505f41b6b61834a3dac07acf41305801"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.AnonymousSelfAssessment.__table__], 3,
         "cd9af5f9505f41b6b61834a3dac07acf41305801"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.AnonymousSelfAssessment.__table__], 4,
         "cd9af5f9505f41b6b61834a3dac07acf41305801"),

        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.StaffDetails.__table__], 1,
         "bc5868106eca313df8e3eee8d39fbef75f03666d"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.StaffDetails.__table__], 2,
         "bc5868106eca313df8e3eee8d39fbef75f03666d"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.StaffDetails.__table__], 3,
         "bc5868106eca313df8e3eee8d39fbef75f03666d"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.StaffDetails.__table__], 4,
         "bc5868106eca313df8e3eee8d39fbef75f03666d"),

        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.MentalHealthDropInContact.__table__], 1,
         "8b249b9ca0c4e52630e2f88b0e0c1abf8fdee549"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.MentalHealthDropInContact.__table__], 2,
         "8b249b9ca0c4e52630e2f88b0e0c1abf8fdee549"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.MentalHealthDropInContact.__table__], 3,
         "8b249b9ca0c4e52630e2f88b0e0c1abf8fdee549"),
        (DS.MHSDS_V5, MHSDS_V5_TP[mhsds_v5.MentalHealthDropInContact.__table__], 4,
         "8b249b9ca0c4e52630e2f88b0e0c1abf8fdee549"),

        (DS.MSDS, MSDS_TP[msds.PregnancyAndBookingDetails.__table__], 0,
         "d334d75f3fdeb178c6597fc8b240f6fac1b2ea6f"),
        (DS.MSDS, MSDS_TP[msds.PregnancyAndBookingDetails.__table__], 1,
         "5920e5d319d603d387d2a2af2bf1fa91f8cd2369"),
        (DS.MSDS, MSDS_TP[msds.PregnancyAndBookingDetails.__table__], 2,
         "44f31047a48c60dda2d93b7386b93215856d31d3"),
        (DS.MSDS, MSDS_TP[msds.PregnancyAndBookingDetails.__table__], 3,
         "550d16cbdbcbc80bf997733b160c14458b5b11a3"),
        (DS.MSDS, MSDS_TP[msds.PregnancyAndBookingDetails.__table__], 4,
         "550d16cbdbcbc80bf997733b160c14458b5b11a3"),
        (DS.MSDS, MSDS_TP[msds.PregnancyAndBookingDetails.__table__], 5,
         "7a1b2a723189bed76e0a271b3fbc94890b0e30a6"),
        (DS.MSDS, MSDS_TP[msds.PregnancyAndBookingDetails.__table__], 6,
         "a774b17a3ba0cbc92e51842644aab16faa24ea5c"),

        (DS.MSDS, MSDS_TP[msds.AnonSelfAssessment.__table__], 0,
         "29141415003410757b15f54d4fa68b924984c03e"),
        (DS.MSDS, MSDS_TP[msds.AnonSelfAssessment.__table__], 1,
         "29141415003410757b15f54d4fa68b924984c03e"),
        (DS.MSDS, MSDS_TP[msds.AnonSelfAssessment.__table__], 2,
         "29141415003410757b15f54d4fa68b924984c03e"),
        (DS.MSDS, MSDS_TP[msds.AnonSelfAssessment.__table__], 3,
         "6ae82720ee3a5d07ebbcee93cc17fb68892c9704"),
        (DS.MSDS, MSDS_TP[msds.AnonSelfAssessment.__table__], 4,
         "6ae82720ee3a5d07ebbcee93cc17fb68892c9704"),
        (DS.MSDS, MSDS_TP[msds.AnonSelfAssessment.__table__], 5,
         "6ae82720ee3a5d07ebbcee93cc17fb68892c9704"),
        (DS.MSDS, MSDS_TP[msds.AnonSelfAssessment.__table__], 6,
         "cd7ee40496ac3bec1a10fe7d5151f9b6a7e0a31a"),

        (DS.MSDS, MSDS_TP[msds.AnonFindings.__table__], 0,
         "7f7c6e6b88e3f9fa9e0bad5087e5dfb33cb3fc16"),
        (DS.MSDS, MSDS_TP[msds.AnonFindings.__table__], 1,
         "7f7c6e6b88e3f9fa9e0bad5087e5dfb33cb3fc16"),
        (DS.MSDS, MSDS_TP[msds.AnonFindings.__table__], 2,
         "7f7c6e6b88e3f9fa9e0bad5087e5dfb33cb3fc16"),
        (DS.MSDS, MSDS_TP[msds.AnonFindings.__table__], 3,
         "b78da698ae6c89d6a16043263fa3e1ce66ca1051"),
        (DS.MSDS, MSDS_TP[msds.AnonFindings.__table__], 4,
         "b78da698ae6c89d6a16043263fa3e1ce66ca1051"),
        (DS.MSDS, MSDS_TP[msds.AnonFindings.__table__], 5,
         "b78da698ae6c89d6a16043263fa3e1ce66ca1051"),
        (DS.MSDS, MSDS_TP[msds.AnonFindings.__table__], 6,
         "763d4222100dd7bd1762beb069f79eae1ceec18a"),

        (DS.MSDS, MSDS_TP[msds.StaffDetails.__table__], 0,
         "35b425768c6025105f6e05a4727197ff26547f9f"),
        (DS.MSDS, MSDS_TP[msds.StaffDetails.__table__], 1,
         "35b425768c6025105f6e05a4727197ff26547f9f"),
        (DS.MSDS, MSDS_TP[msds.StaffDetails.__table__], 2,
         "35b425768c6025105f6e05a4727197ff26547f9f"),
        (DS.MSDS, MSDS_TP[msds.StaffDetails.__table__], 3,
         "35b425768c6025105f6e05a4727197ff26547f9f"),
        (DS.MSDS, MSDS_TP[msds.StaffDetails.__table__], 4,
         "1abf472cccf26717137d2c9f651e76881b42f2e4"),
        (DS.MSDS, MSDS_TP[msds.StaffDetails.__table__], 5,
         "1abf472cccf26717137d2c9f651e76881b42f2e4"),
        (DS.MSDS, MSDS_TP[msds.StaffDetails.__table__], 6,
         "4f32642fb63cbd50cd44f7465138e4f1582757f0"),

        (DS.IAPT, IAPT_TP[iapt.Referral.__table__], 1, "21c0428c84870d4011b118899481662f4cf5cee2"),
        (DS.IAPT, IAPT_TP[iapt.CarePersonnelQualification.__table__], 1,
         "be0beb75845d506e6dbccb7c50e1fbd4d31e7a07"),
        (DS.IAPT, IAPT_TP[iapt.Referral.__table__], 2, "5e78c4dab0d2f81165c618c8b550a9fffc072dca"),
        (DS.IAPT, IAPT_TP[iapt.CarePersonnelQualification.__table__], 2,
         "be0beb75845d506e6dbccb7c50e1fbd4d31e7a07"),

        (DS.IAPT_V2_1, IAPT_V2_1_TP[iapt_v2_1.Referral.__table__], 2,
         "fdaa5d16cb22f98689a6e695a5cec05a83723372"),
        (DS.IAPT_V2_1, IAPT_V2_1_TP[iapt_v2_1.Referral.__table__], 3,
         "6320eca0f9473dd08af7815c2a420f6a95777689"),
        (DS.IAPT_V2_1, IAPT_V2_1_TP[iapt_v2_1.Referral.__table__], 4,
         "7637f87b2a9aff3cd76282b1b0cc5674fe77c0e9"),
        (DS.IAPT_V2_1, IAPT_V2_1_TP[iapt_v2_1.Referral.__table__], 5,
         "51ab9df517c8df5af07db4968437ea70b56cfef9"),
        (DS.IAPT_V2_1, IAPT_V2_1_TP[iapt_v2_1.CarePersonnelQualification.__table__], 2,
         "be0beb75845d506e6dbccb7c50e1fbd4d31e7a07"),
        (DS.IAPT_V2_1, IAPT_V2_1_TP[iapt_v2_1.CarePersonnelQualification.__table__], 3,
         "ee11336a1af25979aeec73540d559dadaae7989d"),
        (DS.IAPT_V2_1, IAPT_V2_1_TP[iapt_v2_1.CarePersonnelQualification.__table__], 4,
         "ee11336a1af25979aeec73540d559dadaae7989d"),
        (DS.IAPT_V2_1, IAPT_V2_1_TP[iapt_v2_1.CarePersonnelQualification.__table__], 5,
         "ee11336a1af25979aeec73540d559dadaae7989d"),
        (DS.CSDS, CSDS_TP[csds.MPI.__table__], 1, "f57249671935a8d6cc0d93e3c225916fa728a14d"),
        (DS.CSDS, CSDS_TP[csds.GroupSession.__table__], 1,
         "cd8bd5c9c88a47794f5c5a625d816501c7825ed4"),
        (DS.CSDS, CSDS_TP[csds.AnonSelfAssessment.__table__], 1,
         "026b023ca5a0b3212a2d1f339d3e498bdcd70b04"),
        (DS.CSDS, CSDS_TP[csds.StaffDetails.__table__], 1,
         "83c0e588e59fec9c68a4e8efd19bb951f4490749"),

        (DS.CSDS, CSDS_TP[csds.MPI.__table__], 2, "851340b0a6f46499d89b515d83eb3d62a7a06fc2"),
        (DS.CSDS, CSDS_TP[csds.GroupSession.__table__], 2,
         "cd8bd5c9c88a47794f5c5a625d816501c7825ed4"),
        (DS.CSDS, CSDS_TP[csds.AnonSelfAssessment.__table__], 2,
         "026b023ca5a0b3212a2d1f339d3e498bdcd70b04"),
        (DS.CSDS, CSDS_TP[csds.StaffDetails.__table__], 2,
         "83c0e588e59fec9c68a4e8efd19bb951f4490749"),

        (DS.CSDS, CSDS_TP[csds.MPI.__table__], 3, "65d4579e75286acb330e0e14b3cf4f2133a24042"),
        (DS.CSDS, CSDS_TP[csds.GroupSession.__table__], 3,
         "cd8bd5c9c88a47794f5c5a625d816501c7825ed4"),
        (DS.CSDS, CSDS_TP[csds.AnonSelfAssessment.__table__], 3,
         "026b023ca5a0b3212a2d1f339d3e498bdcd70b04"),
        (DS.CSDS, CSDS_TP[csds.StaffDetails.__table__], 3,
         "83c0e588e59fec9c68a4e8efd19bb951f4490749"),

        (DS.PCAREMEDS, PCAREMEDS_TP[pcaremeds.PrimaryCareMedicineModel.__table__], 1,
         "0fc234ef74dffa1eadffd3505140e5a0d91835d9"),
        (DS.PCAREMEDS, PCAREMEDS_TP[pcaremeds.PrimaryCareMedicineModel.__table__], 2,
         "8ec3b7cf242b8daf936d20050f9b9f4d1d8e5940"),
        (DS.PCAREMEDS, PCAREMEDS_TP[pcaremeds.PrimaryCareMedicineModel.__table__], 3,
         "8fd54a85c748ecfd8b85e3722ecbd6d4d5a47e77"),
        (DS.PCAREMEDS, PCAREMEDS_TP[pcaremeds.PrimaryCareMedicineModel.__table__], 4,
         "d81b6a726dbda4d302500b9c079a40224f233231"),

        (DS.P2C, P2C_TP[p2c.P2CModel.__table__], 1, '7b20ccfb89559d48c1544f8c547297678bc2c045'),
        (DS.P2C, P2C_TP[p2c.P2CModel.__table__], 2, '7d68f3964b06a10a4ef00787e2c8cdc580035964'),

        (DS.EPMAWSPC2, EPMAWSPC2_TP[epmawspc2.EPMAWellSkyPrescriptionModel2.__table__], 1,
         '187ce86486538db06e54bd1ef8c6aab58a5ba152'),
        (DS.EPMAWSAD2, EPMAWSAD2_TP[epmawsad2.EPMAWellSkyAdministrationModel2.__table__], 1,
         'f438616076ecac6a507ac878af5a3c2998b3367d'),
        (DS.EPMAWSPC2, EPMAWSPC2_TP[epmawspc2.EPMAWellSkyPrescriptionModel2.__table__], 2,
         '1fa7f66a40960a7b2d3fc8543b5158d3b1142095'),
        (DS.EPMAWSAD2, EPMAWSAD2_TP[epmawsad2.EPMAWellSkyAdministrationModel2.__table__], 2,
         'c4600a419a3d2665c5a2e37d0e0d669fe646eae2'),
        (DS.EPMAWSPC2, EPMAWSPC2_TP[epmawspc2.EPMAWellSkyPrescriptionModel2.__table__], 3,
         '9ca169877d90fa03f834fa8737238c14ddba4939'),
        (DS.EPMAWSAD2, EPMAWSAD2_TP[epmawsad2.EPMAWellSkyAdministrationModel2.__table__], 3,
         '8389187e2f4e764a5fba25119caaa164f350fc13'),
        (DS.EPMANATIONALADM, EPMANATIONALADM_TP[epmanationaladm.EPMAAdministrationModel.__table__], 1,
         "88b7e012342069a6d87556e2ff890945230b156e"),
        (DS.EPMANATIONALADM, EPMANATIONALADM_TP[epmanationaladm.EPMAAdministrationModel.__table__], 2,
         "9bf22d72a5acaa6da3c47588bbcb6c640d99f4fe"),
        (DS.EPMANATIONALPRES, EPMANATIONALPRES_TP[epmanationalpres.EPMAPrescriptionModel.__table__], 1,
         "7c659b0dc39fd42e23ec9b55bbfa896cc811abad"),
        (DS.EPMANATIONALPRES, EPMANATIONALPRES_TP[epmanationalpres.EPMAPrescriptionModel.__table__], 2,
         "200fcd66cfc69aeb5818542005173edddb75e69a"),
    ]
)
def test_model_version_corruption(dataset_id: str, table_prefix: str, version: int, sha1_hash: str):
    """
    This is a test to highlight where someone changes the model schema for a version.
    Once the model schema is fixed for a version, it should never be changed.
    This test will highlight when that happens
    """
    module = importlib.import_module(
        "dsp.datasets.models.uplift.{}.{}.version_{}".format(dataset_id, table_prefix.lower(),
                                                             version))
    schema_json = getattr(module, 'schema_json')
    calculated = sha1(json.dumps(schema_json).encode('utf-8')).hexdigest()
    assert calculated == sha1_hash


@pytest.mark.parametrize(
    "dataset_id, table_prefixes, model, model_excluded_fields",
    [
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.Referral, list()),
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.GroupSession, list()),
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.AnonymousSelfAssessment, list()),
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.StaffDetails, list()),
        (DS.MHSDS_V5, MHSDS_V5_TP, mhsds_v5.MentalHealthDropInContact, list()),

        (DS.MSDS, MSDS_TP, msds.PregnancyAndBookingDetails, list()),
        (DS.MSDS, MSDS_TP, msds.AnonSelfAssessment, list()),
        (DS.MSDS, MSDS_TP, msds.AnonFindings, list()),
        (DS.MSDS, MSDS_TP, msds.StaffDetails, list()),

        (DS.IAPT, IAPT_TP, iapt.Referral, list()),
        (DS.IAPT, IAPT_TP, iapt.CarePersonnelQualification, list()),

        (DS.IAPT_V2_1, IAPT_V2_1_TP, iapt_v2_1.Referral, list()),
        (DS.IAPT_V2_1, IAPT_V2_1_TP, iapt_v2_1.CarePersonnelQualification, list()),

        (DS.CSDS, CSDS_TP, csds.MPI, list()),
        (DS.CSDS, CSDS_TP, csds.GroupSession, list()),
        (DS.CSDS, CSDS_TP, csds.AnonSelfAssessment, list()),
        (DS.CSDS, CSDS_TP, csds.StaffDetails, list()),

        (DS.PCAREMEDS, PCAREMEDS_TP, pcaremeds.PrimaryCareMedicineModel, list()),

        (DS.P2C, P2C_TP, p2c.P2CModel, list()),

        (DS.EPMAWSPC2, EPMAWSPC2_TP, epmawspc2.EPMAWellSkyPrescriptionModel2,
         epmawspc2_EXCLUDED_FIELDS),
        (DS.EPMAWSAD2, EPMAWSAD2_TP, epmawsad2.EPMAWellSkyAdministrationModel2,
         epmawsad2_EXCLUDED_FIELDS),
        (DS.EPMANATIONALADM, EPMANATIONALADM_TP, epmanationaladm.EPMAAdministrationModel,
          EPMA_NATIONAL_EXCLUDED_FIELDS.ADMIN_COLS),
        (DS.EPMANATIONALPRES, EPMANATIONALPRES_TP, epmanationalpres.EPMAPrescriptionModel,
          EPMA_NATIONAL_EXCLUDED_FIELDS.PRESC_COLS)
    ]
)
def test_empty_dataframe(spark: SparkSession, dataset_id: str, table_prefixes: Dict,
                         model: DSPStructuredModel, model_excluded_fields: List):
    # Create a dataframe with a dummy schema that will get replaced
    df = spark.createDataFrame([], StructType([
        StructField("dummy", StringType(), nullable=True),
    ]))

    head = df.head()
    assert not head

    df = uplift(spark, df, dataset_id, table_prefixes[model.__table__])

    head = df.head()
    assert not head

    # Check that the schema has been updated
    assert df.schema.jsonValue() == StructType(
        [sfields for sfields in model.get_struct() if
         not sfields.name in model_excluded_fields]).jsonValue()
