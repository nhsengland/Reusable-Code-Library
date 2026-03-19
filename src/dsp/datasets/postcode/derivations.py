from dsp.common.structured_model import MPSConfidenceScores
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType
from dsp.datasets.postcode.shared.derivations import SharedDerivationFields


class NpexDerivationFields(SharedDerivationFields):
    OrganisationCountry = 'OrganisationCountry'
    OrganisationPostcode = "OrganisationPostcode"
    OrganisationType = 'OrganisationType'
    OrganisationRole = 'OrganisationRole'
    PdsCrossCheckCondition = 'PdsCrossCheckCondition'
    OrganisationRefCountry = "OrganisationRefCountry"
    OrganisationRefDataCountry = "OrganisationRefDataCountry"
    PdsMatchedFilter = "PdsMatchedFilter"
    PdsMatched = "PdsMatched"
    PdsMatchedCondition = "PdsMatchedCondition"
    DerivedNotes = "DerivedNotes"
    IsStaleSpecimen = 'IsStaleSpecimen'
    HomeCountryMapped = 'HomeCountryMapped'
    NormalisedHomePostcode = 'NormalisedHomePostcode'
    DeliveryCountryMapped = 'DeliveryCountryMapped'
    NormalisedDeliveryPostcode = 'NormalisedDeliveryPostcode'
    TestResultRtts = 'TestResultRtts'


NPEX_MPS_ENRICHMENT_ADDITIONAL_SCHEMA = StructType([
    StructField(NpexDerivationFields.TestCentreCountry, StringType()),
    StructField(NpexDerivationFields.TestCentreCountryName, StringType()),
    StructField(NpexDerivationFields.TestCentreRefDataPostcode, StringType()),
    StructField(SharedDerivationFields.TestCentreLowerTierLocalAuthority, StringType()),
    StructField(NpexDerivationFields.County, StringType()),
    StructField(NpexDerivationFields.LocalAuthority, StringType()),
    StructField(NpexDerivationFields.LowerTierLocalAuthority, StringType()),
    StructField(NpexDerivationFields.CountryCode, StringType()),
    StructField(NpexDerivationFields.CommissioningOrganisation, StringType()),
    StructField(NpexDerivationFields.LLSOA2011, StringType()),
    StructField(NpexDerivationFields.STP, StringType()),
    StructField(NpexDerivationFields.PostCodeCountry, StringType()),
    StructField(NpexDerivationFields.Easting, StringType()),
    StructField(NpexDerivationFields.Northing, StringType()),
    StructField(NpexDerivationFields.ParliamentaryConstituency, StringType()),
    StructField(NpexDerivationFields.NHSRegion, StringType()),
    StructField(NpexDerivationFields.NormalisedPostcode, StringType()),
    StructField(NpexDerivationFields.MPSConfidence, MPSConfidenceScores.get_struct()),
    StructField(NpexDerivationFields.Person_ID, StringType()),
    StructField(NpexDerivationFields.OccupationTitle, StringType()),
    StructField(NpexDerivationFields.MPSGPCode, StringType()),
    StructField(NpexDerivationFields.MPSFirstName, StringType()),
    StructField(NpexDerivationFields.MPSLastName, StringType()),
    StructField(NpexDerivationFields.MPSDateOfBirth, DateType()),
    StructField(NpexDerivationFields.MPSGender, StringType()),
    StructField(NpexDerivationFields.MPSPostcode, StringType()),
    StructField(NpexDerivationFields.MPSEmailAddress, StringType()),
    StructField(NpexDerivationFields.MPSMobileNumber, StringType()),
    StructField(NpexDerivationFields.MPSPostCodeCountryMatched, StringType()),
    StructField(NpexDerivationFields.TestResultKeystone, StringType()),
    StructField(NpexDerivationFields.SendToKeystone, BooleanType()),
    StructField(NpexDerivationFields.OrganisationCountry, StringType()),
    StructField(NpexDerivationFields.OrganisationPostcode, StringType()),
    StructField(NpexDerivationFields.HomeCountryMapped, StringType()),
    StructField(NpexDerivationFields.DeliveryCountryMapped, StringType()),    
    StructField(NpexDerivationFields.PdsCrossCheckCondition, StringType()),
    StructField(NpexDerivationFields.PdsMatchedFilter, BooleanType()),
    StructField(NpexDerivationFields.PdsMatchedCondition, StringType()),
    StructField(NpexDerivationFields.PdsMatched, BooleanType()),
    StructField(NpexDerivationFields.CCGOfRegistration, StringType()),
    StructField(NpexDerivationFields.DerivedNotes, StringType()),
    StructField(NpexDerivationFields.IsStaleSpecimen, BooleanType()),
    StructField(NpexDerivationFields.TestResultRtts, StringType()),
])
