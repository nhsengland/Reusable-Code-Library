import re
from datetime import datetime, date
from dateutil.parser import isoparse
from typing import Optional

from pyspark import Row  # noqa
from dsp.datasets.models.covax.fhir import *
import dsp.ref_data


GENDERS = {
    "0": "unknown",
    "1": "male",
    "2": "female",
    "9": "other",
}


def parse_tz_date(datestring: str) -> Optional[datetime]:
    if not datestring:
        return None

    parsed = _parse_tz_date(datestring, raise_if_failed=True)
    if not parsed.tzinfo:
        raise ValueError(f'_parse_tz_date should only return tz aware datetimes: {parsed}')
    return parsed


def try_parse_tz_date(datestring) -> Optional[datetime]:

    if not datestring:
        return None

    return _parse_tz_date(datestring, raise_if_failed=False)


# vaccinations has a strange date format ..  equivalent to
# datetime.now().astimezone().strftime('%Y%m%dT%H%M%S%z').replace('+', '')[:17]
_tz_date_re = re.compile(r'^\d{8}T?(?:[0-2]\d(?:[0-5]\d(?:[0-5]\d(?:(?:(?:0\d)|(?:1[0-2]))(?:[0-5]\d)?)?)?)?)?$')


def _parse_tz_date(datestring, raise_if_failed: bool = False) -> Optional[datetime]:

    try:

        if _tz_date_re.match(datestring):

            datestring = datestring.replace('T', '').ljust(18, '0')
            datestring = f"{datestring[:8]}T{datestring[8:14]}+{datestring[14:]}"

        return isoparse(datestring)

    except ValueError:
        if raise_if_failed:
            raise
        return None


def create_fhir_message_from_row(
    row: Row, updates_enabled: bool, send_adverse_reactions: bool, mailbox_from: str, title: Optional[str] = None
) -> MessageBundle:

    fhir_patient_gp = Organization(
        {"site_code": row.PERSON_GP_CODE, "site_name": row.PERSON_GP_NAME}
    )
    fhir_patient_gp.validate()

    fhir_patient_gp_for_header = HeaderOrganization({"site_code": row.PERSON_GP_CODE})
    fhir_patient_gp_for_header.validate()

    fhir_location = None
    if row.SITE_CODE_TYPE_URI == "http://snomed.info/sct":
        fhir_vaccination_service_provider = Organization(
            {
                "site_code": "N2N9I",
                "site_name": "COVID19 VACCINE RESOLUTION SERVICEDESK",
                "site_code_type_uri": "https://fhir.nhs.uk/Id/ods-organization-code",
            }
        )
        country_lookup = {cn["snomed_code"]: cn["name"] for cn in dsp.ref_data.Countries}
        fhir_location = Location(
            {
                "site_code": row.SITE_CODE,
                "site_name": country_lookup.get(row.SITE_CODE),
                "site_code_type_uri": row.SITE_CODE_TYPE_URI,
            }
        )
    else:
        fhir_vaccination_service_provider = Organization(
            {
                "site_code": row.SITE_CODE ,
                "site_name": row.SITE_NAME,
                "site_code_type_uri": row.SITE_CODE_TYPE_URI,
            }
        )

    if row.VACCINE_MANUFACTURER:
        fhir_vaccine_manufacturer = Organization(
            {"site_name": row.VACCINE_MANUFACTURER}
        )
        fhir_vaccine_manufacturer.validate()
    else:
        fhir_vaccine_manufacturer = None

    fhir_patient = Patient(
        {
            # In the input, NHS_NUMBER is what was received from the supplier
            # PERSON_ID is the one matched from mps
            "nhs_number": row.PERSON_ID,
            "person_forename": row.PERSON_FORENAME,
            "person_surname": row.PERSON_SURNAME,
            "person_gender": GENDERS[row.PERSON_GENDER_CODE],
            "person_dob": datetime.strptime(row.PERSON_DOB, "%Y%m%d"),
            "person_postcode": row.PERSON_POSTCODE,
            "nhs_number_status_code": row.NHS_NUMBER_STATUS_INDICATOR_CODE,
            "nhs_number_status_display": row.NHS_NUMBER_STATUS_INDICATOR_DESCRIPTION,
            "general_practitioner": fhir_patient_gp,
        }
    )
    fhir_patient.validate()

    fhir_performing_professional = Practitioner(
        {
            "performing_professional_body_reg_uri": row.PERFORMING_PROFESSIONAL_BODY_REG_URI,
            "performing_professional_body_reg_code": row.PERFORMING_PROFESSIONAL_BODY_REG_CODE,
            "performing_professional_forename": row.PERFORMING_PROFESSIONAL_FORENAME,
            "performing_professional_surname": row.PERFORMING_PROFESSIONAL_SURNAME,
        }
    )
    fhir_performing_professional.validate()

    row_datetime = parse_tz_date(row.DATE_AND_TIME)

    fhir_encounter = Encounter(
        {
            "encounter_date": row_datetime,
            "subject": fhir_patient,
            "practitioner": fhir_performing_professional,
            "service_provider": fhir_vaccination_service_provider,
        }
    )
    fhir_encounter.validate()

    immunization_fields = {
        "record_date": datetime.strptime(row.RECORDED_DATE, "%Y%m%d") if row.RECORDED_DATE is not None else None,
        "unique_id_uri": row.UNIQUE_ID_URI,
        "unique_id": row.UNIQUE_ID,
        "vaccination_procedure_code": row.VACCINATION_PROCEDURE_CODE,
        "vaccination_procedure_term": row.VACCINATION_PROCEDURE_TERM,
        "vaccination_situation_code": row.VACCINATION_SITUATION_CODE,
        "vaccination_situation_term": row.VACCINATION_SITUATION_TERM,
        "action_flag": row.ACTION_FLAG,
        "not_given": row.NOT_GIVEN == "TRUE",
        "vaccine_product_code": row.VACCINE_PRODUCT_CODE,
        "vaccine_product_term": row.VACCINE_PRODUCT_TERM,
        "batch_number": row.BATCH_NUMBER,
        "expiry_date": datetime.strptime(row.EXPIRY_DATE, "%Y%m%d") if row.EXPIRY_DATE is not None else None,
        "date_and_time": row_datetime,
        "primary_source": row.PRIMARY_SOURCE == "TRUE",
        "vaccine_manufacturer": row.VACCINE_MANUFACTURER,
        "site_of_vaccination_code": row.SITE_OF_VACCINATION_CODE,
        "site_of_vaccination_term": row.SITE_OF_VACCINATION_TERM,
        "route_of_vaccination_code": row.ROUTE_OF_VACCINATION_CODE,
        "route_of_vaccination_term": row.ROUTE_OF_VACCINATION_TERM,
        "indication_code": row.INDICATION_CODE,
        "indication_term": row.INDICATION_TERM,
        "reason_not_given_code": row.REASON_NOT_GIVEN_CODE,
        "reason_not_given_term": row.REASON_NOT_GIVEN_TERM,
        "dose_unit_code": row.DOSE_UNIT_CODE,
        "dose_unit_term": row.DOSE_UNIT_TERM,
        "dose_amount": row.DOSE_AMOUNT,
        "dose_sequence": row.DOSE_SEQUENCE,
        "practitioner": fhir_performing_professional,
        "manufacturer": fhir_vaccine_manufacturer,
        "patient": fhir_patient,
        "encounter": fhir_encounter,
    }

    if fhir_location is not None:
        immunization_fields["location"] = fhir_location

    fhir_immunization = Immunization(
        immunization_fields
    )
    fhir_immunization.validate()

    fhir_nhsd = HeaderOrganization({"site_code": "X26"})
    fhir_nhsd.validate()

    composition = {
        "current_date": date.today(),
        "care_setting_type_code": row.CARE_SETTING_TYPE_CODE,
        "care_setting_type_description": row.CARE_SETTING_TYPE_DESCRIPTION,
        "author": fhir_performing_professional,
        "custodian": fhir_patient_gp,
        "subject": fhir_patient,
        "immunization": fhir_immunization,
        "encounter": fhir_encounter,
        "treatment_consent": row.CONSENT_FOR_TREATMENT_DESCRIPTION,
    }

    adverse_reactions_entries = []

    if updates_enabled:

        composition["relatesTo"] = row.PREV_COMPOSITION_ID
        composition["delete"] = row.ACTION_FLAG == "delete"

        fhir_allergy_list = get_allergies(
            row, send_adverse_reactions, fhir_patient, fhir_performing_professional, fhir_encounter
        )
        if fhir_allergy_list is not None:
            composition["allergies"] = fhir_allergy_list
            adverse_reactions_entries = [fhir_allergy_list] + fhir_allergy_list.allergies

    if title:
        composition["title"] = title

    fhir_composition = ImmunizationComposition(composition)
    fhir_composition.validate()

    entries = [
        fhir_composition,
        fhir_patient,
        fhir_performing_professional,
        fhir_immunization,
        fhir_patient_gp,
        fhir_vaccine_manufacturer,
        fhir_encounter,
        fhir_vaccination_service_provider,
    ]

    if fhir_location is not None:
        entries.append(fhir_location)

    if adverse_reactions_entries:
        entries.extend(adverse_reactions_entries)

    fhir_document_bundle = DocumentBundle(
        {"entries": [e for e in entries if e is not None]}
    )

    fhir_message_header = MessageHeader(
        {
            "sender": fhir_nhsd,
            "receiver": fhir_patient_gp_for_header,
            "source": mailbox_from,
            "message": fhir_document_bundle,
        }
    )

    fhir_message = MessageBundle(
        {
            "messageHeader": fhir_message_header,
            "organisations": [fhir_nhsd, fhir_patient_gp_for_header],
            "document": fhir_document_bundle,
        }
    )

    return fhir_message


def get_allergies(
    row: Row, send_adverse_reactions: bool,
    fhir_patient: Patient,
    fhir_performing_professional: Practitioner,
    fhir_encounter: Encounter
) -> Optional[AllergyList]:

    if not send_adverse_reactions or not row.VAX_STATE.ADVERSE_REACTIONS:
        return None

    fhir_allergies = [
        AllergyIntolerance(
            {
                "unique_id_uri": reaction.UNIQUE_ID_URI,
                "unique_id": reaction.UNIQUE_ID,
                "clinical_status": "active",
                "verification_status": reaction.VERIFICATION_STATUS,
                "reaction_type": reaction.TYPE_OF_REACTION,
                "reaction_code": reaction.REACTION_CODING_CODE,
                "reaction_display": reaction.REACTION_CODING_DISPLAY,
                "reaction_description": reaction.REACTION_DESCRIPTION,
                "reaction_severity": reaction.REACTION_SEVERITY,
                "allergy_code": reaction.CAUSATIVE_AGENT_CODING_CODE,
                "allergy_display": reaction.CAUSATIVE_AGENT_CODING_DISPLAY,
                "patient": fhir_patient,
                "asserted_date": datetime.strptime(
                    (reaction.RECORDED_DATE or row.RECORDED_DATE), "%Y%m%d"
                ),
                "onset": parse_tz_date(reaction.ONSET),
            }
        )
        for reaction in row.VAX_STATE.ADVERSE_REACTIONS
    ]

    fhir_allergy_list = AllergyList(
        {
            "subject": fhir_patient,
            "source": fhir_performing_professional,
            "encounter": fhir_encounter,
            "allergies": fhir_allergies,
        }
    )

    return fhir_allergy_list
