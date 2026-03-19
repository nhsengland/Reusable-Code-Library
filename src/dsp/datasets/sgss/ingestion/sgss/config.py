from dsp.datasets.common import Fields as CommonFields
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType, StructField

SGSS_MPS_FIELDS = ['UNIQUE_REFERENCE', 'PERSON_ID', 'MPSConfidence', 'ERROR_SUCCESS_CODE']

SGSS_DELTA_MPS_FIELDS = ['GIVEN_NAME', 'FAMILY_NAME', 'GENDER', 'DATE_OF_BIRTH', 'POSTCODE', 'EMAIL_ADDRESS', 'MOBILE_NUMBER']


META_SCHEMA = StructType(
    [
        StructField(CommonFields.DATASET_VERSION, StringType(), False),
        StructField(CommonFields.EVENT_ID, StringType(), False),
        StructField(CommonFields.EVENT_RECEIVED_TS, TimestampType(), False),
        StructField(CommonFields.RECORD_INDEX, IntegerType(), False),
        StructField(CommonFields.RECORD_VERSION, IntegerType(), False),
    ]
)


class Fields:
    PERSON_ID = 'PERSON_ID'
    Patient_Forename = 'Patient_Forename'
    Patient_Surname = 'Patient_Surname'
    Patient_Sex = 'Patient_Sex'
    Patient_NHS_Number = 'Patient_NHS_Number'
    Reporting_Lab = 'Reporting_Lab'
    CDR_Specimen_Request_SK = 'CDR_Specimen_Request_SK'
    Organism_Species_Name = 'Organism_Species_Name'
    Specimen_Date = 'Specimen_Date'
    Lab_Report_Date = 'Lab_Report_Date'
    Patient_Date_Of_Birth = 'Patient_Date_Of_Birth'
    Patient_PostCode = 'Patient_PostCode'
    Age_in_Years = 'Age_in_Years'
    MPSConfidence = 'MPSConfidence'
    Ethnicity_Description = 'Ethnicity_Description'
    County_Description = 'County_Description'
    ODS_Location_Code = 'ODS_Location_Code'
    Lower_Super_Output_Area_Code = 'Lower_Super_Output_Area_Code'
    PostCode_Source = 'PostCode_Source'
    Reporting_Lab_ID = 'Reporting_Lab_ID'
    P2_email = 'P2_email'
    P2_mobile = 'P2_mobile'
    P2_landline = 'P2_landline'
    care_home = 'care_home'
    specimen_id = 'specimen_id'
    test_type = 'test_type'
    SGTF = 'SGTF'
    Uniq_ID = 'UNIQ_ID'
    pcds = 'pcds'
    pcds_sector = 'pcds_sector'
    latlong = 'latlong'
    latitude = 'latitude'
    longitude = 'longitude'
    MPS_TRACE_SUCCESSFUL = 'MPS_TRACE_SUCCESSFUL'
    Indicators = 'Indicators'
    test_kit = 'test_kit'
    specimen_type = 'specimen_type'
    test_result_sct = 'test_result_sct'
    test_result = 'test_result'
    method_of_detection = 'method_of_detection'
    Lab_Pillar = 'Lab_Pillar'
    SendToKeystone = 'SendToKeystone'
    lab_pillar = 'lab_pillar'
    gp_code = 'gp_code'
    PdsCrossCheckCondition = 'PdsCrossCheckCondition'


class PostprocessingFields:
    NormalisedPostcode = 'NormalisedPostcode'
    PostcodeCountry = 'PostcodeCountry'


class ExtractCountry:
    ENGLAND = 'england'
    SCOTLAND = 'scotland'
    NORTHERN_IRELAND = 'northern_ireland'
    WALES = 'wales'
    DMS = 'dms'
    CTAS = 'ctas'
    NTP_PILLAR_ONE = 'ntp_pillar_one'
    NTP_PRIVATE = 'ntp_private'
    RTTS = 'rtts'
    UNMATCHED = 'unmatched'


MPS_INCLUDE_FILTER = f"{Fields.Reporting_Lab} <> 'PILLAR 2 TESTING' OR {Fields.Patient_NHS_Number} IS NOT NULL"

POSITIVE_ORGANISM_SPECIES_NAME = 'SARS-CoV-2 CORONAVIRUS (Covid-19)'
NEGATIVE_ORGANISM_SPECIES_NAME = 'SARS-CoV-2 CORONAVIRUS (Covid-19) NEGATIVE'
INDETERMINATE_ORGANISM_SPECIES_NAME = 'SARS-CoV-2 CORONAVIRUS (Covid-19) INDETERMINATE'
VOID_ORGANISM_SPECIES_NAME = 'SARS-CoV-2 CORONAVIRUS (Covid-19) VOID'


class TestTypes:
    CULTURE = '1'
    ANTIGEN_DETECTION = '5'
    GENOMIC_PCR_LCR_DETECTION = '6'
    POINT_OF_CARE_TESTING = '11'
    PCR_ROCHE8800 = '12'
    PCR_ROCHEFLOW = '13'
    PCR_ROCHE6800 = '14'
    PCR_LAMPORE = '20'
    SAMBA_II = '21'
    MENARINI = '22'
    PCR_RESIBIO = '23'
    POCT_GENOMIC = '24'
    POCT_LATERAL_FLOW = '25'
    POCT_ANTIGEN = '26'
    PCR_NEUMODX = '29'
    RT_QPCR = '30'
    PCR_PRIMERDESIGNQ = '31'
    RNA_LAMP = '42'
    DIRECT_LAMP = '43'


PCR_SCT_MAPPINGS = {
    'sct_mappings': {
        POSITIVE_ORGANISM_SPECIES_NAME: {'sct': '1324601000000106', 'standardised_desc': 'SARS-CoV-2-ORGY'},
        NEGATIVE_ORGANISM_SPECIES_NAME: {'sct': '1324581000000102', 'standardised_desc': 'SARS-CoV-2-ORGN'},
        VOID_ORGANISM_SPECIES_NAME: {'sct': '1321691000000102', 'standardised_desc': 'SARS-CoV-2-ORGU'},
        INDETERMINATE_ORGANISM_SPECIES_NAME: {'sct': '1321691000000102', 'standardised_desc': 'SARS-CoV-2-ORGU'}
    }
}

LFT_SCT_MAPPINGS = {
    'sct_mappings': {
        POSITIVE_ORGANISM_SPECIES_NAME: {'sct': '1322781000000102', 'standardised_desc': 'SARS-CoV-2-ANGY'},
        NEGATIVE_ORGANISM_SPECIES_NAME: {'sct': '1322791000000100', 'standardised_desc': 'SARS-CoV-2-ANGN'},
        VOID_ORGANISM_SPECIES_NAME: {'sct': '1322821000000105', 'standardised_desc': 'SARS-CoV-2-ANGU'},
        INDETERMINATE_ORGANISM_SPECIES_NAME: {'sct': '1322801000000101', 'standardised_desc': 'SARS-CoV-2-ANGQ'}
    }
}

RNA_NASOPHARYN_COMPOSITE = {
    'method_of_detection': 'rna',
    'specimen_type': 'Nasopharyngeal swab'
}

ANTIGEN_NASOPHARYN_COMPOSITE = {
    'method_of_detection': 'antigen',
    'specimen_type': 'Nasopharyngeal swab'
}

RNA_SALIVA_COMPOSITE = {
    'method_of_detection': 'rna',
    'specimen_type': 'saliva'
}

RNA_NASOPHARYN_PCR_MAPPING = {
    **PCR_SCT_MAPPINGS,
    **RNA_NASOPHARYN_COMPOSITE,
    'test_kit': 'pcr'
}

RNA_NASOPHARYN_LFT_MAPPING = {
    **LFT_SCT_MAPPINGS,
    **RNA_NASOPHARYN_COMPOSITE,
    'test_kit': 'lft'
}

ANTIGEN_NASOPHARYN_LFT_MAPPING = {
    **LFT_SCT_MAPPINGS,
    **ANTIGEN_NASOPHARYN_COMPOSITE,
    'test_kit': 'lft'
}

ANTIGEN_NASOPHARYN_UNKNOWN_LFT_MAPPING = {
    **LFT_SCT_MAPPINGS,
    **ANTIGEN_NASOPHARYN_COMPOSITE,
    'test_kit': 'Unknown'
}

RNA_NASOPHARYN_UNKNOWN_LFT_MAPPING = {
    **LFT_SCT_MAPPINGS,
    **RNA_NASOPHARYN_COMPOSITE,
    'test_kit': 'Unknown'
}

RNA_NASOPHARYN_UNKNOWN_PCR_MAPPING = {
    **PCR_SCT_MAPPINGS,
    **RNA_NASOPHARYN_COMPOSITE,
    'test_kit': 'Unknown'
}

RNA_NASOPHARYN_RNALAMP_MAPPING = {
    **PCR_SCT_MAPPINGS,
    **RNA_NASOPHARYN_COMPOSITE,
    'test_kit': 'rnalamp'
}

RNA_SALIVA_DIRECTLAMP_MAPPING = {
    **PCR_SCT_MAPPINGS,
    **RNA_SALIVA_COMPOSITE,
    'test_kit': 'directlamp'
}

RNA_NASOPHARYN_LAMPORE_MAPPING = {
    **PCR_SCT_MAPPINGS,
    **RNA_NASOPHARYN_COMPOSITE,
    'test_kit': 'lampore'
}

RNA_NASOPHARYN_RTPCR_MAPPING = {
    **PCR_SCT_MAPPINGS,
    **RNA_NASOPHARYN_COMPOSITE,
    'test_kit': 'rtPCR'
}

POCT_NASOPHARYN_SAMBA_II_MAPPING = {
    **PCR_SCT_MAPPINGS,
    **RNA_NASOPHARYN_COMPOSITE,
    'test_kit': 'SAMBAII',
}

TEST_TYPE_TO_MAPPING = {
    TestTypes.CULTURE: RNA_NASOPHARYN_PCR_MAPPING,
    TestTypes.ANTIGEN_DETECTION: RNA_NASOPHARYN_UNKNOWN_PCR_MAPPING,  # What's the test kit for this?
    TestTypes.GENOMIC_PCR_LCR_DETECTION: RNA_NASOPHARYN_PCR_MAPPING,
    TestTypes.POINT_OF_CARE_TESTING: ANTIGEN_NASOPHARYN_LFT_MAPPING,
    TestTypes.PCR_ROCHE8800: RNA_NASOPHARYN_PCR_MAPPING,
    TestTypes.PCR_ROCHEFLOW: RNA_NASOPHARYN_PCR_MAPPING,
    TestTypes.PCR_ROCHE6800: RNA_NASOPHARYN_PCR_MAPPING,
    TestTypes.PCR_LAMPORE: RNA_NASOPHARYN_LAMPORE_MAPPING,
    TestTypes.SAMBA_II: POCT_NASOPHARYN_SAMBA_II_MAPPING,
    TestTypes.MENARINI: RNA_NASOPHARYN_RTPCR_MAPPING,
    TestTypes.PCR_RESIBIO: RNA_NASOPHARYN_PCR_MAPPING,
    TestTypes.POCT_GENOMIC: RNA_NASOPHARYN_UNKNOWN_PCR_MAPPING,  # What's the test kit for this?
    TestTypes.POCT_LATERAL_FLOW: ANTIGEN_NASOPHARYN_LFT_MAPPING,
    TestTypes.POCT_ANTIGEN: RNA_NASOPHARYN_UNKNOWN_PCR_MAPPING,
    TestTypes.PCR_NEUMODX: RNA_NASOPHARYN_PCR_MAPPING,
    TestTypes.RT_QPCR: RNA_NASOPHARYN_RTPCR_MAPPING,
    TestTypes.PCR_PRIMERDESIGNQ: RNA_NASOPHARYN_PCR_MAPPING,
    TestTypes.RNA_LAMP: RNA_NASOPHARYN_RNALAMP_MAPPING,
    TestTypes.DIRECT_LAMP: RNA_SALIVA_DIRECTLAMP_MAPPING
}

TEST_TYPE_TO_MAPPING_EXPLODED = [
    {'test_type': test_type, 'organism_key': organism_key, 'test_result_sct': test_result['sct'],
     'test_result': test_result['standardised_desc'], 'method_of_detection': test_desc['method_of_detection'],
     'test_kit': test_desc['test_kit'], 'specimen_type': test_desc['specimen_type']}
    for test_type, test_desc in TEST_TYPE_TO_MAPPING.items()
    for organism_key, test_result in test_desc['sct_mappings'].items()
]