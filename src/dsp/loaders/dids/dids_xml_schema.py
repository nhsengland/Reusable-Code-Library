from pyspark.sql.types import StructType, StructField, ArrayType, StringType

schema = StructType([
    StructField("ImagingActivity", ArrayType(
        StructType([
            StructField("DiagnosticTestDate", StringType(), True),
            StructField("ImagingCodeNICIP", StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_code", StringType(), True)]), True),
            StructField("ImagingCodeSNOMEDCT", StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_code", StringType(), True)]), True),
            StructField("ImagingSiteCode", StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_extension", StringType(), True)]), True),
            StructField("RadiologicalAccessionNumber", StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_extension", StringType(), True)]), True),
            StructField("ServiceReportIssueDate", StringType(), True)]), True), True),
    StructField("PersonalAndDemographics", StructType([
        StructField("Address", StructType([
            StructField("postalCode", StringType(), True)]), True),
        StructField("EthnicCategory", StructType([
            StructField("_VALUE", StringType(), True),
            StructField("_code", StringType(), True)]), True),
        StructField("GPCodeRegistration", StructType([
            StructField("_VALUE", StringType(), True),
            StructField("_extension", StringType(), True)]), True),
        StructField("NHSNumber", StructType([
            StructField("_VALUE", StringType(), True),
            StructField("_extension", StringType(), True)]), True),
        StructField("NHSNumberStatus", StructType([
            StructField("_VALUE", StringType(), True),
            StructField("_code", StringType(), True)]), True),
        StructField("PersonBirthDate", StringType(), True),
        StructField("PersonGenderCode", StructType([
            StructField("_VALUE", StringType(), True),
            StructField("_code", StringType(), True)]), True)]), True),
    StructField("Referrals", StructType([
        StructField("DiagnosticTestReqDate", StringType(), True),
        StructField("DiagnosticTestReqRecDate", StringType(), True),
        StructField("PatientSourceSetting", StructType([
            StructField("_VALUE", StringType(), True),
            StructField("_code", StringType(), True)]), True),
        StructField("ReferrerCode", StructType([
            StructField("_VALUE", StringType(), True),
            StructField("_extension", StringType(), True)]), True),
        StructField("ReferringOrgCode", StructType([
            StructField("_VALUE", StringType(), True),
            StructField("_extension", StringType(), True)]), True)]), True)])
