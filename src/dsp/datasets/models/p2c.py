from dsp.common.structured_model import DSPStructuredModel, SubmittedAttribute, _META, META, \
    AssignableAttribute
from datetime import datetime
from dsp.datasets.common import Fields as CommonFields


class _P2CModel(DSPStructuredModel):
    META = SubmittedAttribute(CommonFields.META, _META)
    # These columns copied straight out of the test file.
    # We may not want to flow all of them
    CreatedBy = SubmittedAttribute('createdby', str)
    CreatedOn = SubmittedAttribute('createdon', datetime)
    CreatedOnBehalfBy = SubmittedAttribute('createdonbehalfby', str)
    CancerTreatmentStatus = SubmittedAttribute('cts_areyoucurrentlyhavingtreatmentforcancer', int)
    OfferedFluJab = SubmittedAttribute('cts_areyouusuallyofferedafreeseasonalflujab', int)
    DateOfBirth = SubmittedAttribute('cts_dateofbirth', datetime)
    HasBleedingDisorder = SubmittedAttribute('cts_doyouknowifyouhaveableedingdisorder', int)
    HealthOrSocialCareWorker = SubmittedAttribute('cts_doyouworkinhealthorsocialcare', int)
    Ethnicity = SubmittedAttribute('cts_ethnicity', int)
    ExpiresOn = SubmittedAttribute('cts_expireson', str)
    ExtendedEthnicity = SubmittedAttribute('cts_extendedethnicity', str)
    DailyVariedF2FContact = SubmittedAttribute('cts_facetofacecontactwithdifferentpeopledaily', int)
    FamilyName = SubmittedAttribute('cts_familyname', str)
    GivenName = SubmittedAttribute('cts_givenname', str)
    HasDiabetes = SubmittedAttribute('cts_hasadoctortoldyouthatyouhavediabetes', int)
    HadCovidTest = SubmittedAttribute('cts_haveyouhadacoronovirustest', int)
    GenderSameAsBirthAssignedSex = SubmittedAttribute('cts_isgendersameassexregisteredatbirth', int)
    GenderSameAsBirthAssignedSexExtended = SubmittedAttribute('cts_isgendersameassexregisteredatbirthextende', str)
    HasLungCondNotAsthma = SubmittedAttribute('cts_longtermlungconditionnotasthma', int)
    NamePrefix = SubmittedAttribute('cts_nameprefix', str)
    NameSuffix = SubmittedAttribute('cts_namesuffix', str)
    NHSOtherResearchContactPermission = SubmittedAttribute('cts_permissionfornhsdcontactaboutotherresearc', int)
    PermissionId = SubmittedAttribute('cts_permissionid', str)
    ContactPermissionForCVD19VaccineStudy = SubmittedAttribute('cts_permissiontocontactaboutcvd19vaccinestudy', int)
    PermissionToEmailForOtherHealthResearch = SubmittedAttribute('cts_permissiontoemailforotherhealthresearch', int)
    PermissionToEmailForCovidVaccineStudies = SubmittedAttribute('cts_permissiontoemailforcovidvaccinestudies', int)
    Postcode = SubmittedAttribute('cts_postcode', str)
    PregnantBreastfeedingOrPlanningInNext6mos = SubmittedAttribute('cts_pregnantbreastfeedingorplanninginnext6mos', int)
    SexRegisteredAtBirth = SubmittedAttribute('cts_sexregisteredatbirth', int)
    SubscriptionURL = SubmittedAttribute('cts_subscriptionurl', str)
    Telephone = SubmittedAttribute('cts_telephone', str)
    ToBeUnsubscribedURL = SubmittedAttribute('cts_tobeunsubscribedurl', str)
    InfectionPotentiallyVerySerious = SubmittedAttribute('cts_toldaninfectionmightbeveryserious', int)
    HasLiverCondition = SubmittedAttribute('cts_toldhasalivercondition', int)
    HasSeriousHeartCondition = SubmittedAttribute('cts_toldhasaseriousheartcondition', int)
    HasModerateToSevereAsthma = SubmittedAttribute('cts_toldhasmoderateorsevereasthma', int)
    Type = SubmittedAttribute('cts_type', int)
    TypeOfWork = SubmittedAttribute('cts_typeofwork', int)
    UnsubscribeBy = SubmittedAttribute('cts_unsubscribeby', str)
    UnsubscriptionURL = SubmittedAttribute('cts_unsubscriptionurl', str)
    TestResults = SubmittedAttribute('cts_whatwereyourtestresults', int)
    EmailAddress = SubmittedAttribute('emailaddress', str)
    ImportSequenceNumber = SubmittedAttribute('importsequencenumber', int)
    LastOnHoldTime = SubmittedAttribute('lastonholdtime', datetime)
    ModifiedBy = SubmittedAttribute('modifiedby', str)
    ModifiedOn = SubmittedAttribute('modifiedon', datetime)
    ModifiedOnBehalfBy = SubmittedAttribute('modifiedonbehalfby', str)
    OnHoldTime = SubmittedAttribute('onholdtime', int)
    OverriddenCreatedOn = SubmittedAttribute('overriddencreatedon', datetime)
    OwnerId = SubmittedAttribute('ownerid', str)
    OwnerIdType = SubmittedAttribute('owneridtype', str)
    OwningBusinessUnit = SubmittedAttribute('owningbusinessunit', str)
    OwningTeam = SubmittedAttribute('owningteam', str)
    OwningUser = SubmittedAttribute('owninguser', str)
    SlaId = SubmittedAttribute('slaid', str)
    SlaInvokedId = SubmittedAttribute('slainvokedid', str)
    StateCode = SubmittedAttribute('statecode', int)
    StatusCode = SubmittedAttribute('statuscode', int)
    TimezoneRuleVersionNumber = SubmittedAttribute('timezoneruleversionnumber', int)
    UtcConversionTimezoneCode = SubmittedAttribute('utcconversiontimezonecode', int)
    VersionNumber = SubmittedAttribute('versionnumber', int)
    IsWelshPreferredLanguage = SubmittedAttribute('cts_iswelshpreferredlanguage', int)

    County = AssignableAttribute('County', str)  # type: AssignableAttribute
    LocalAuthority = AssignableAttribute('LocalAuthority', str)  # type: AssignableAttribute
    LowerTierLocalAuthority = AssignableAttribute('LowerTierLocalAuthority', str)  # type: AssignableAttribute
    CountryCode = AssignableAttribute('CountryCode', str)  # type: AssignableAttribute
    CommissioningOrganisation = AssignableAttribute('CommissioningOrganisation', str)  # type: AssignableAttribute
    LLSOA2011 = AssignableAttribute('LLSOA2011', str)  # type: AssignableAttribute
    MSOA01 = AssignableAttribute('MSOA01', str)  # type: AssignableAttribute
    STP = AssignableAttribute('STP', str)  # type: AssignableAttribute
    PostCodeCountry = AssignableAttribute('PostCodeCountry', str)  # type: AssignableAttribute
    CreatedOnWeekOfYear = AssignableAttribute('CreatedOnWeekOfYear', str)  # type: AssignableAttribute
    NormalisedPostcode = AssignableAttribute('NormalisedPostcode', str)  # type: AssignableAttribute


class P2CModel(_P2CModel):
    __concrete__ = True
    __table__ = 'p2c'

    META = SubmittedAttribute('META', META)  # type: SubmittedAttribute
