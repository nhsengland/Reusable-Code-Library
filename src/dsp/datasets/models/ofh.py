from datetime import datetime

from dsp.common.structured_model import DSPStructuredModel, SubmittedAttribute, META, MPSConfidenceScores, AssignableAttribute


class OFH_Participant_Model(DSPStructuredModel):
    __concrete__ = True
    __table__ = 'participant'

    META = SubmittedAttribute('META', META)  # type: META
    participantId = SubmittedAttribute('participantId', str)
    firstName = SubmittedAttribute('firstName', str)
    lastName = SubmittedAttribute('lastName', str)
    dob = SubmittedAttribute('dob', str)
    phoneNumber = SubmittedAttribute('phoneNumber', str)
    emailAddress = SubmittedAttribute('emailAddress', str)
    postcode = SubmittedAttribute('postcode', str)
    addressLine1 = SubmittedAttribute('addressLine1', str)
    addressLine2 = SubmittedAttribute('addressLine2', str)
    addressLine3 = SubmittedAttribute('addressLine3', str)
    postTown = SubmittedAttribute('postTown', str)
    county = SubmittedAttribute('county', str)
    postalCounty = SubmittedAttribute('postalCounty', str)
    administrativeCounty = SubmittedAttribute('administrativeCounty', str)
    traditionalCounty = SubmittedAttribute('traditionalCounty', str)
    administrativeDistrict = SubmittedAttribute('administrativeDistrict', str)
    administrativeWard = SubmittedAttribute('administrativeWard', str)
    country = SubmittedAttribute('country', str)
    registrationDate = SubmittedAttribute('registrationDate', str)


class OFH_Consent_Model(DSPStructuredModel):
    __concrete__ = True
    __table__ = 'consent'

    META = SubmittedAttribute('META', META)  # type: META
    participantId = SubmittedAttribute('participantId', str)
    consentId = SubmittedAttribute('consentId', str)
    consentText = SubmittedAttribute('consentText', str)
    acceptanceDate = SubmittedAttribute('acceptanceDate', str)


class OFH_Questionnaire_Submission_Model(DSPStructuredModel):
    __concrete__ = True
    __table__ = 'questionnaireSubmission'

    META = SubmittedAttribute('META', META)  # type: META
    participantId = SubmittedAttribute('participantId', str)
    templateSectionId = SubmittedAttribute('templateSectionId', str)
    submissionDate = SubmittedAttribute('submissionDate', str)
    questionId = SubmittedAttribute('questionId', str)
    answer = SubmittedAttribute('answer', str)


class OFH_Question_Coding_Model(DSPStructuredModel):
    __concrete__ = True
    __table__ = 'questionCoding'

    META = SubmittedAttribute('META', META)  # type: META
    questionId = SubmittedAttribute('questionId', str)
    coding = SubmittedAttribute('coding', str)
    codedQuestionId = SubmittedAttribute('codedQuestionId', str)


class OFH_Questionnaire_Template_Model(DSPStructuredModel):
    __concrete__ = True
    __table__ = 'questionnaireTemplate'

    META = SubmittedAttribute('META', META)  # type: META
    templateSectionId = SubmittedAttribute('templateSectionId', str)
    templateSection = SubmittedAttribute('templateSection', str)

class OFH_MPS_Model(DSPStructuredModel):
    __concrete__ = True
    __table__ = 'participant'

    META = SubmittedAttribute('META', META)  # type: META
    participantId = SubmittedAttribute('participantId', str)
    firstName = SubmittedAttribute('firstName', str)
    lastName = SubmittedAttribute('lastName', str)
    dob = SubmittedAttribute('dob', str)
    phoneNumber = SubmittedAttribute('phoneNumber', str)
    emailAddress = SubmittedAttribute('emailAddress', str)
    postcode = SubmittedAttribute('postcode', str)
    addressLine1 = SubmittedAttribute('addressLine1', str)
    addressLine2 = SubmittedAttribute('addressLine2', str)
    addressLine3 = SubmittedAttribute('addressLine3', str)
    postTown = SubmittedAttribute('postTown', str)
    county = SubmittedAttribute('county', str)
    postalCounty = SubmittedAttribute('postalCounty', str)
    administrativeCounty = SubmittedAttribute('administrativeCounty', str)
    traditionalCounty = SubmittedAttribute('traditionalCounty', str)
    administrativeDistrict = SubmittedAttribute('administrativeDistrict', str)
    administrativeWard = SubmittedAttribute('administrativeWard', str)
    country = SubmittedAttribute('country', str)
    registrationDate = SubmittedAttribute('registrationDate', str)
    MPSConfidence = AssignableAttribute('MPSConfidence', MPSConfidenceScores)  # type: AssignableAttribute
    Person_ID = SubmittedAttribute('Person_ID', str)

