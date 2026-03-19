from dsp.common.structured_model import MPSConfidenceScores
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType


class SharedDerivationFields:
    LandingTimestamp = 'LandingTimestamp'
    LandingFilename = 'LandingFilename'
    TestCentreCountry = 'TestCentreCountry'
    TestCentreRefCountry = 'TestCentreRefCountry'
    TestCentreCountryName = 'TestCentreCountryName'
    TestCentreRefDataPostcode = 'TestCentreRefDataPostcode'
    TestCentreRefCountryCode = 'TestCentreRefCountryCode'
    TestCentreLowerTierLocalAuthority = 'TestCentreLowerTierLocalAuthority'
    NormalisedPostcode = 'NormalisedPostcode'
    County = 'County'
    LocalAuthority = 'LocalAuthority'
    Country = 'Country'
    CountryCode = 'CountryCode'
    CommissioningOrganisation = 'CommissioningOrganisation'
    LLSOA2011 = 'LLSOA2011'
    STP = 'STP'
    LowerTierLocalAuthority = 'LowerTierLocalAuthority'
    Easting = 'Easting'
    Northing = 'Northing'
    ParliamentaryConstituency = 'ParliamentaryConstituency'
    NHSRegion = 'NHSRegion'
    PostCodeCountry = 'PostCodeCountry'
    OccupationTitle = 'OccupationTitle'
    MPSConfidence = 'MPSConfidence'
    Person_ID = 'Person_ID'
    MPSGPCode = 'MPSGPCode'
    MPSFirstName = 'MPSFirstName'
    MPSLastName = 'MPSLastName'
    MPSDateOfBirth = 'MPSDateOfBirth'
    MPSGender = 'MPSGender'
    MPSPostcode = 'MPSPostcode'
    MPSEmailAddress = 'MPSEmailAddress'
    MPSMobileNumber = 'MPSMobileNumber'
    MPSCountry = 'MPSCountry'
    MPSCountryCode = 'MPSCountryCode'
    MPSPostCodeCountry = 'MPSPostCodeCountry'
    MPSPostCodeCountryMatched = 'MPSPostCodeCountryMatched'
    TestResultKeystone = 'TestResultKeystone'
    SendToKeystone = 'SendToKeystone'
    CCGOfRegistration = 'CCGOfRegistration'
    DerivedNotes = 'DerivedNotes'


SHARED_DERIVATION_SCHEMA = StructType([
    StructField(SharedDerivationFields.TestCentreCountry, StringType()),
    StructField(SharedDerivationFields.TestCentreCountryName, StringType()),
    StructField(SharedDerivationFields.TestCentreRefDataPostcode, StringType()),
    StructField(SharedDerivationFields.TestCentreLowerTierLocalAuthority, StringType()),
    StructField(SharedDerivationFields.County, StringType()),
    StructField(SharedDerivationFields.LocalAuthority, StringType()),
    StructField(SharedDerivationFields.LowerTierLocalAuthority, StringType()),
    StructField(SharedDerivationFields.CountryCode, StringType()),
    StructField(SharedDerivationFields.CommissioningOrganisation, StringType()),
    StructField(SharedDerivationFields.LLSOA2011, StringType()),
    StructField(SharedDerivationFields.STP, StringType()),
    StructField(SharedDerivationFields.PostCodeCountry, StringType()),
    StructField(SharedDerivationFields.Easting, StringType()),
    StructField(SharedDerivationFields.Northing, StringType()),
    StructField(SharedDerivationFields.ParliamentaryConstituency, StringType()),
    StructField(SharedDerivationFields.NHSRegion, StringType()),
    StructField(SharedDerivationFields.NormalisedPostcode, StringType()),
    StructField(SharedDerivationFields.MPSConfidence, MPSConfidenceScores.get_struct()),
    StructField(SharedDerivationFields.Person_ID, StringType()),
    StructField(SharedDerivationFields.OccupationTitle, StringType()),
    StructField(SharedDerivationFields.MPSGPCode, StringType()),
    StructField(SharedDerivationFields.MPSFirstName, StringType()),
    StructField(SharedDerivationFields.MPSLastName, StringType()),
    StructField(SharedDerivationFields.MPSDateOfBirth, DateType()),
    StructField(SharedDerivationFields.MPSGender, StringType()),
    StructField(SharedDerivationFields.MPSPostcode, StringType()),
    StructField(SharedDerivationFields.MPSEmailAddress, StringType()),
    StructField(SharedDerivationFields.MPSMobileNumber, StringType()),
    StructField(SharedDerivationFields.MPSPostCodeCountryMatched, StringType()),
    StructField(SharedDerivationFields.TestResultKeystone, StringType()),
    StructField(SharedDerivationFields.SendToKeystone, BooleanType()),
    StructField(SharedDerivationFields.CCGOfRegistration, StringType()),
    StructField(SharedDerivationFields.DerivedNotes, StringType())
])
