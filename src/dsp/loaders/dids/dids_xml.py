from typing import Tuple, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import explode

from dsp.loaders import LoaderRejectionError
from dsp.loaders.dids.dids_xml_schema import schema
from dsp.pipeline import ValidationResult


def loader(spark, path, _) -> Tuple[DataFrame, Optional[ValidationResult]]:
    """
        Returns dids dataframe for xml, raises LoaderRejectionError if there are no reocrds in xml or xml file is not
        loaded as per the schema
        Added df.count insteaad of df.head(1) or df.take(1) because df.count() takes 50% less time  for large xml files.
        Args:
            spark (SparkSession): current spark session
            path (str): the path to the xml file

       Returns:
           df_with_dids_columns (DataFrame)
    """
    df = (
        spark
        .read
        .format("com.databricks.spark.xml")
        .option('rowTag', 'DIDSRecord')
        .option("mode", "FAILFAST")
        .option('treatEmptyValuesAsNulls', 'true')
        .schema(schema)
        .load(path)
    )
    try:
        df.collect()
    except:
        raise LoaderRejectionError

    return df.withColumn('act', explode('ImagingActivity')).selectExpr(*[
        "PersonalAndDemographics.NHSNumber._extension as NHS_NUMBER",
        "PersonalAndDemographics.NHSNumberStatus._code as NHS_NUMBER_STATUS_IND",
        "PersonalAndDemographics.EthnicCategory._code as ETHNIC_CATEGORY",
        "PersonalAndDemographics.PersonBirthDate as PERSON_BIRTH_DATE",
        "PersonalAndDemographics.PersonGenderCode._code as PERSON_GENDER_CODE",
        "PersonalAndDemographics.Address.postalCode as POSTCODE",
        "PersonalAndDemographics.GPCodeRegistration._extension as PRACTICE_CODE",
        "Referrals.PatientSourceSetting._code as PATIENT_SOURCE_SETTING",
        "Referrals.ReferrerCode._extension as REFERRER",
        "Referrals.ReferringOrgCode._extension as REFERRING_ORG_CODE",
        "Referrals.DiagnosticTestReqDate as DIAGNOSTIC_TEST_REQUEST_DATE",
        "Referrals.DiagnosticTestReqRecDate as DIAG_TEST_REQUEST_REC_DATE",
        "act.DiagnosticTestDate as DIAGNOSTIC_TEST_DATE",
        "act.ImagingCodeNICIP._code as IMAGING_CODE_NICIP",
        "act.ImagingCodeSNOMEDCT._code as IMAGING_CODE_SNOMED",
        "act.ServiceReportIssueDate as SERVICE_REPORT_DATE",
        "act.ImagingSiteCode._extension as SITE_CODE_IMAGING",
        "act.RadiologicalAccessionNumber._extension as RADIOLOGICAL_ACCESSION_NUM"
    ]), None
