from datetime import datetime, date
import copy
from decimal import Decimal
from dsp.datasets.models.epmanationaladm import EPMAAdministrationModel

meta_dict = {
    'DATASET_VERSION': '1',
    'EVENT_ID': '1:1',
    'EVENT_RECEIVED_TS': datetime(2021, 11, 2),
    'RECORD_INDEX': 0,
    'RECORD_VERSION': '1'
}

epma_admin_header_dict = {
            "DataSetCreatedTimestamp": datetime(2022, 2, 11, 15, 59, 26),
            "OrganisationSiteIdentifierSystemLocation": "RX101",
            "PrimSystemInUse": "TDT Eddie Gen",
            "ReportingPeriodEndDate": date(2022, 2, 28),
            "ReportingPeriodStartDate": date(2022, 2, 1),
}

epma_admin_medication_administration_active_ingredient_substance_dict = [
            {
                "MedicationAdminIdentifier": "LOCALADMTEST01",
                "MedicationAdministeredActiveIngredientSubstanceDesc": "AL3UOQ12199K5LCF6SXKO2XWBE6FSRXCNXN91I1DAPMBECFIGZZFJUTL0AAGHYFO7XT63ETHHJW3FCFRE6VBMQYN7XQIGG2HMGIIR9ZXJVJY79CFG6S4PDCFVKPKUWY4ZEM6MFPY2F9T0B2O3IM5UFXH072M6Y2JA54TO2DIL2XPOF4Y2CPPBWM0MOSSDRICLEMODG7PKKUJU1G2JSBHUOJWUWM2TWV6GKD2VBOMWK85Y325G3A8G28FA9Q7D2W",
                "MedicationAdministeredActiveIngredientSubstanceStrengthDesc": "AL3UOQ12199K5LCF6SXKO2XWBE6FSRXCNXN91I1DAPMBECFIGZZFJUTL0AAGHYFO7XT63ETHHJW3FCFRE6VBMQYN7XQIGG2HMGIIR9ZXJVJY79CFG6S4PDCFVKPKUWY4ZEM6MFPY2F9T0B2O3IM5UFXH072M6Y2JA54TO2DIL2XPOF4Y2CPPBWM0MOSSDRICLEMODG7PKKUJU1G2JSBHUOJWUWM2TWV6GKD2VBOMWK85Y325G3A8G28FA9Q7D2W",
                "OrganisationSiteIdentifierOfTreatment": "123456789",
                "RowNumber": 0
            },
            {
                "MedicationAdminIdentifier": "LOCALADMTEST01",
                "MedicationAdministeredActiveIngredientSubstanceDesc": "HQJ879SX8K1MR57B4997S4IQS4HIBHQ2IJ3FRALZW3GNLZYJU9HFGGI9U1QIQI0Q351JGSA0DPUD3G9MDZXRX8CDKUYITLPZ9I61S8K7ZCTKXSW80OAORC4NZZ19EZ0M71FWPTUDCQCU08MLI86KYUN9H8B68KU7E8GMLQMCFMJSKQ8I88UDUNVPAQ3YEOKQZSHMZ8YYVQDJKVRQG5K28PWOZJL0F73EF1EKZ6G4Y7WEJNGRWN1NDHFYC2MEG1C",
                "MedicationAdministeredActiveIngredientSubstanceStrengthDesc": "HQJ879SX8K1MR57B4997S4IQS4HIBHQ2IJ3FRALZW3GNLZYJU9HFGGI9U1QIQI0Q351JGSA0DPUD3G9MDZXRX8CDKUYITLPZ9I61S8K7ZCTKXSW80OAORC4NZZ19EZ0M71FWPTUDCQCU08MLI86KYUN9H8B68KU7E8GMLQMCFMJSKQ8I88UDUNVPAQ3YEOKQZSHMZ8YYVQDJKVRQG5K28PWOZJL0F73EF1EKZ6G4Y7WEJNGRWN1NDHFYC2MEG1C",
                "OrganisationSiteIdentifierOfTreatment": "123456789",
                "RowNumber": 1
            }
        ]


epma_admin_medication_administration_dict = {
        'META': meta_dict,
        'Header': epma_admin_header_dict,
        'MedicationAdministrationActiveIngredientSubstance': epma_admin_medication_administration_active_ingredient_substance_dict,
        "AGE": 38,
        "BodySiteOfAdministrationActualDesc": "Anatomical or acquired body structure (body structure)",
        "BodySiteOfAdministration_SnomedCt": "442083009",
        "CodedProcedureTimeStamp_MedicationAdmin": datetime(2022, 2, 15, 12, 0, 0),
        "DATE_OF_BIRTH": date(1983, 8, 6),
        "DEPRIVATION_IMD_VERSION": "2015",
        "DOSE_QUANTITY_UNIT_CHECK": "02",
        "DOSE_QUANTITY_UNIT_FILTERED": "Milligram/meter2/day (qualifier value)",
        "DOSE_QUANTITY_UNIT_FILTERED_CHECK": "03",
        "FORM_FILTERED": "Cream (basic dose form)",
        "FORM_FILTERED_CHECK": "03",
        "FORM_SNOMED_CT_CHECK": "01",
        "FORM_SNOMED_CT_FILTERED": "385099005",
        "IMD_DECILE": "03",
        "LOWER_LAYER_SUPER_OUTPUT_AREA": "E01033010",
        "LSOA_VERSION": "LSOA11",
        "MedicationAdminIdentifier": "LOCALADMTEST01",
        "MedicationAdminStatusDesc": "on-hold",
        "MedicationAdministeredName": "Hydralazine 2mg/5ml oral suspension (Special Order) 1 ml",
        "MedicationAdministered_DmD": "12522511000001101",
        "MedicationAdministrationDoseActualDesc": "BNACMNCJRH43YOE4KYNEECQDSWUZA6LRA3Z00N1PYNEF7493PRNG2O23K5CWTXA1L43ZBGTIL2UMHZGHNIGCVOVU9CWULCQKSIFJK2LATH3JOJK8OT8PM5WVH0UWSK2DK95121RH0KVK7G3Y55K7HNF9I9GCMNODR9KQLCPPD39ULZUM6E497HM3JLDPPGVRTGBTSSNJ9U23N9VOOKMU5AGAVYGBSKH7KLNESH6F96DURIR760JVRB2PER4LD4R",
        "MedicationAdministrationDoseFormDesc": "Cream (basic dose form)",
        "MedicationAdministrationDoseForm_SnomedCt": "385099005",
        "MedicationAdministrationDoseQuantityValue": Decimal(9686015877.715833),
        "MedicationAdministrationDoseQuantityValueUnitOfMeasurementDesc": "Milligram/meter2/day (qualifier value)",
        "MedicationAdministrationRecordLastUpdatedTimeStamp": datetime(2022, 1, 31, 12, 0, 0),
        "MedicationAdministrationRecordedTimeStamp": datetime(2022, 2, 25, 12, 0, 0),
        "MedicationAdministrationSettingType_Actual": "3",
        "METHOD_FILTERED": "Apply",
        "METHOD_FILTERED_CHECK": "03",
        "MethodOfAdministrationActualDesc": "Apply",
        "MethodOfAdministrationActual_SnomedCt": "417924000",
        "METHOD_SNOMED_CT_CHECK": "01",
        "METHOD_SNOMED_CT_FILTERED": "417924000",
        "NHS_NUMBER_PDS_CHECK": "01",
        "NHSNumberStatusIndicatorCode": "04",
        "NHS_NUMBER_LEGALLY_RESTRICTED": "9961981944",
        "AUTOMAPPED_DMD": "12522511000001101",
        "OrganisationSiteIdentifierOfTreatment": "123456789",
        "OtherMedicationAdminSettingDesc": "4X3JBQ8KHBCW24OPW3D6UEB309N6ZWE5AB714DUB843AR16U71AYNGT47QPBNNQSMKY6II157HHLGIY7BX4MD8AH1XDR1K3VO1H9J8ZEIRK3YRCINUY0V1ANHBYJ350VIYV0QXMUPOQBZN0PRBRWLDI6DGTNEZA1QG3PPZCDIOCVOAD9NQGOJJ2WEPN4XXVG4U1OMC2A04RAOWF069CYJCY55RUMW8TNJDKAMFJNU8Y8FHXLYZ3G9ULALVBIKWX",
        "PersonBirthDate": date(1983, 8, 6),
        "PrescribedItemIdentifier": "LOCALTEST01",
        "PrescribedMedicationDoseNotAdministeredBoolean": False,
        "PrescribedMedicationDoseNotAdministeredReasonDescription": "BEM3PPS3JTBTRSHQKQGYUJ5N4EVSNSS7RY1FM4J9LWBIDFZHSJNPZ2HGTIVCHKBBSC9EX106MRG7Y54WYJHNU9OKGWS4PVLE8KXAG0NEB4XY7MDR1D0CUZW981JMWLLVCEIF5SB9NEL2W1CH9SHJUHNHLGXDL8ANIFDCEP0OSLEQ18RIT05R38DXUFSR1MW6UZFARC62EV9VPE0FG34PHOLDV63FCWVMKT1SLXL7VFMSNEDAIXGIBHJQ5JQ392P",
        "PrescribedMedicationDoseToBeAdministeredTimestamp": datetime(2022, 1, 31, 12, 0, 0),
        "RP_START_MONTH": 202201,
        "ROUTE_FILTERED": "Oral use",
        "ROUTE_FILTERED_CHECK": "03",
        "RouteOfAdministrationActualDesc": "Oral use",
        "RouteOfAdministration_SnomedCt": "26643006",
        "ROUTE_SNOMED_CT_CHECK": "01",
        "ROUTE_SNOMED_CT_FILTERED": "26643006",
        "RowNumber": 0,
        "SITE_FILTERED": "Anatomical or acquired body structure (body structure)",
        "SITE_FILTERED_CHECK": "03",
        "SITE_SNOMED_CT_CHECK": "01",
        "SITE_SNOMED_CT_FILTERED": "442083009",
        "SOURCE_SYSTEM_TRUST_ODS_CODE": "RX1",
        "TREATMENT_SITE_ODS_CODE_CHECK": "03",
        "TREATMENT_SITE_ODS_CODE_FILTERED": None,
        "TREATMENT_TRUST_ODS_CODE": None,
        "TRUST_DMD_CHECK": "01",
        "AUTOMAPPED_DMD_CHECK": "01",
        "PRIMARY_DMD": "12522511000001101",
        "PRIMARY_DMD_CHECK": "01",
        "PRIMARY_DMD_SOURCE": "01",
        "PRIMARY_DMD_NAME": "Hydralazine 2mg/5ml oral suspension (Special Order) 1 ml",
        "PRIMARY_DMD_LEVEL": "ampp",
        "VTM": "22696000",
        "VTM_TERM_TEXT": "Hydralazine",
        "VMP": "12537211000001104",
        "VMP_TERM_TEXT": "Hydralazine 2mg/5ml oral suspension",
        "VMPP": "12522311000001107",
        "VMPP_TERM_TEXT": "Hydralazine 2mg/5ml oral suspension 1 ml",
        "AMP": "12522411000001100",
        "AMP_TERM_TEXT": "Hydralazine 2mg/5ml oral suspension",
        "AMPP": "12522511000001101",
        "AMPP_TERM_TEXT": "Hydralazine 2mg/5ml oral suspension (Special Order) 1 ml",
        "PERSON_ID": "9961981944",
        "PATIENT_POSTCODE": "LS1 5HR",
        "REGISTERED_GP_PRACTICE": "B82105",
        "GENDER": "1",
        "MPS_CONFIDENCE": {
            "MatchedAlgorithmIndicator": "1",
            "MatchedConfidencePercentage": Decimal(100.00),
            "FamilyNameScorePercentage": None,
            "GivenNameScorePercentage": None,
            "DateOfBirthScorePercentage": None,
            "GenderScorePercentage": None,
            "PostcodeScorePercentage": None
        }
    }


def epmanationaladm_test_data(include_derivations: bool = False,
                              derivations_to_exclude: list = []):
    if include_derivations:
        data = {field: val for field, val in EPMAAdministrationModel(epma_admin_medication_administration_dict).as_dict().items() if field not in derivations_to_exclude}
    else:
        data = epma_admin_medication_administration_dict
    return copy.deepcopy(data)
