from datetime import datetime, timedelta, timezone, time
from uuid import uuid4
from xml.etree.ElementTree import ElementTree

import re

import pytz
from lxml import etree
from io import BytesIO

import pytest
from pyspark import Row

from dsp.datasets.models.covax.fhir import *
from dsp.datasets.models.covax.fhir_message_from_row import GENDERS, get_allergies, parse_tz_date


def test_base():
    new = Base()
    assert new.id


def allergy_base_record(**updates):

    result = {
        "unique_id_uri": uuid4().hex,
        "unique_id": uuid4().hex,
        "clinical_status": "active",
        "verification_status": "confirmed",
        "reaction_type": "not the good type",
        "reaction_code": "EEK",
        "reaction_display": "that stings!",
        "reaction_description": "nasty itching",
        "reaction_severity": "really nasty",
        "allergy_code": "BEES",
        "allergy_display": "Bees with frickin laser beams",
        "patient": patient_base_record(**updates.get('patient', {})),
        "asserted_date": datetime.now(),
        "onset": datetime.now() + timedelta(days=-1)
    }

    result.update(updates)

    return AllergyIntolerance(result)


def practice_base_record(**updates):
    result = {"site_code": 'AGP', "site_name": 'A Practice'}
    result.update(updates)
    return Organization(result)


def practitioner_base_record(**updates):
    result = {
        "performing_professional_body_reg_uri": "http://thinguri",
        "performing_professional_body_reg_code": "ACOD1",
        "performing_professional_forename": "Dr Thing",
        "performing_professional_surname": "THING",
    }
    result.update(updates)
    return Practitioner(result)


def patient_base_record(**updates):

    result = {
        # In the input, NHS_NUMBER is what was received from the supplier
        # PERSON_ID is the one matched from mps
        "nhs_number": '0000000000',
        "person_forename": 'Qin Shi',
        "person_surname": 'Huang',
        "person_gender": GENDERS["1"],
        "person_dob": datetime(221, 2, 18),
        "person_postcode": 'CH',
        "nhs_number_status_code": '01',
        "nhs_number_status_display": 'ok right',
        "general_practitioner": practice_base_record(**updates.get('general_practitioner', {})),
    }

    result.update(updates)

    return Patient(result)


def vaccination_service_provider_base_record(**updates):
    result = {"site_code": 'AVP', "site_name": 'Vaccination Dome 2021', 'site_code_type_uri': 'http://things'}
    result.update(updates)
    return Organization(result)


def encounter_base_record(**updates):
    result = {
        "encounter_date": datetime.now(),
        "subject": patient_base_record(**updates.get('subject', {})),
        "practitioner": practitioner_base_record(**updates.get('practitioner', {})),
        "service_provider": vaccination_service_provider_base_record(**updates.get('service_provider', {})),
    }
    result.update(updates)
    return Encounter(result)


def manufacturer_base_record(**updates):
    result = {
        "site_name": "Mr Manufacturer",
    }
    result.update(updates)
    return Organization(result)


def immunization_base_record(**updates):

    encounter = encounter_base_record(
        subject=patient_base_record(**updates.get('patient', {})),
        practitioner=practitioner_base_record(**updates.get('practitioner', {})),
    )

    row_datetime = datetime.now(timezone.utc)

    if 'date_and_time' in updates:
        date_and_time = updates.pop('date_and_time')
        row_datetime = parse_tz_date(date_and_time)

    result = {
        "record_date": datetime.combine(datetime.today(), time(0, 0, 0)),
        "unique_id_uri": "http://local-testing",
        "unique_id": uuid4().hex,
        "vaccination_procedure_code": "VAXC1",
        "vaccination_procedure_term": "VAX TERM1",
        "vaccination_situation_code": "VAX SITCH1",
        "vaccination_situation_term": "VAX SITCH TERM1",
        "action_flag": "new",
        "not_given": False,
        "vaccine_product_code": "VAX1",
        "vaccine_product_term": "VAX PROD TERM2",
        "batch_number": "1",
        "expiry_date": datetime(2099, 1, 23),
        "date_and_time": row_datetime,
        "primary_source": True,
        "vaccine_manufacturer": "VAXMAF1",
        "site_of_vaccination_code": "VAXSITE1",
        "site_of_vaccination_term": "VAX SITE TERM",
        "route_of_vaccination_code": "VAXRT1",
        "route_of_vaccination_term": "VAX ROUTE TERM",
        "indication_code": "VAXIN1",
        "indication_term": "VAX IND TERM",
        "reason_not_given_code": "",
        "reason_not_given_term": "",
        "dose_unit_code": "ml",
        "dose_unit_term": "millilitres",
        "dose_amount": "12.3",
        "dose_sequence": 1,
        "practitioner": encounter.practitioner,
        "manufacturer": manufacturer_base_record(**updates.get('manufacturer', {})),
        "patient": encounter.subject,
        "encounter": encounter,
    }

    result.update(updates)

    return Immunization(result)




def minimal_ar_row(**updates):

    result = dict(
        META=Row(
            DATASET_VERSION="1",
            EVENT_RECEIVED_TS=datetime.now(),
            RECORD_INDEX=1,
            RECORD_VERSION=1
        ),
        UNIQUE_ID=uuid4().hex,
        UNIQUE_ID_URI="http://local-testing",
        VERIFICATION_STATUS="confirmed",
        CAUSATIVE_AGENT_CODING_SYSTEM='slowmed',
        CAUSATIVE_AGENT_CODING_CODE='ASNOWMEDCODE',
        CAUSATIVE_AGENT_CODING_DISPLAY='Bad Stuff (tm)',
        REACTION_CODING_SYSTEM='',
        REACTION_CODING_CODE='',
        REACTION_CODING_DISPLAY='',
        REACTION_DESCRIPTION='',
        TYPE_OF_REACTION='',
        REACTION_SEVERITY='',
        EVIDENCE='',
        ONSET='20210113T09293300',
        ACTION_FLAG='new',
        VACCINATION_UNIQUE_ID_URI="http://local-testing",
        VACCINATION_UNIQUE_ID=uuid4().hex,
        RECORDED_DATE="20210113",
        PROCESSED_DATE=None
    )
    result.update(updates)

    return Row(**result)


def test_allergy_intolerance_text_desc():
    """https://developer.nhs.uk/apis/digitalmedicines-1.2.3-private-beta/explore_allergies_and_adverse_reactions.html"""

    record = allergy_base_record()

    with BytesIO() as f:
        et = ElementTree(record.text_summary())
        et.write(f)
        text = f.getvalue().decode()

        assert text == (
                '<table width="100%"><tbody>' +
                f"<tr><th>Causative agent</th><td>{record.allergy_display}</td></tr>" +
                f"<tr><th>Description of reaction</th><td>{record.reaction_description}</td></tr>" +
                f"<tr><th>SNOMED Code of reaction</th><td>{record.reaction_code} ({record.reaction_display})</td></tr>" +
                f"<tr><th>Severity</th><td>{record.reaction_severity}</td></tr>" +
                f"<tr><th>Certainty</th><td>{record.verification_status}</td></tr>" +
                f"<tr><th>Date recorded</th><td>{record.asserted_date.strftime('%-d %b %Y')}</td></tr>" +
                '</tbody></table>'
        )


@pytest.mark.parametrize("verification_status, clinical_status", [
    ('confirmed', "active"),
    ('entered-in-error', None)
])
def test_allergy_intolerance_clinical_status(verification_status, clinical_status):
    """https://developer.nhs.uk/apis/digitalmedicines-1.2.3-private-beta/explore_allergies_and_adverse_reactions.html"""

    record = allergy_base_record(verification_status=verification_status)

    with BytesIO() as f:
        et = ElementTree(record.to_fhir_xml())
        et.write(f)
        fhir = etree.fromstring(f.getvalue().decode())

    ecs = fhir.find('clinicalStatus')

    if not clinical_status:
        assert ecs is None

    if clinical_status:
        assert ecs is not None
        assert 'value' in ecs.attrib
        assert ecs.attrib['value'] == clinical_status


@pytest.mark.parametrize("adverse_reaction, expected_onset", [
    (minimal_ar_row(ONSET=None), None),
    (minimal_ar_row(ONSET='20210117T11293303'), datetime(2021, 1, 17, 11, 29, 33, tzinfo=timezone(timedelta(seconds=10800)))),
    (minimal_ar_row(ONSET='20210117T112933'), datetime(2021, 1, 17, 11, 29, 33).astimezone(timezone.utc))
])
def test_adverse_reaction_onset(adverse_reaction, expected_onset):

    encounter = encounter_base_record()
    patient = encounter.subject
    practitioner = encounter.practitioner

    allergy_list = get_allergies(
        Row(VAX_STATE=Row(ADVERSE_REACTIONS=[adverse_reaction])), True, patient, practitioner, encounter
    )

    allergies = allergy_list.allergies

    assert allergies[0].onset == expected_onset


@pytest.mark.parametrize("date_and_time, expected", [
    ("20210117T112933.123+03:00", "17-Jan-2021 08:29 (GMT)"),
    ("20210117T11293303", "17-Jan-2021 08:29 (GMT)"),
    ("20210823T170102", "23-Aug-2021 18:01 (BST)"),
    ("20210823", "23-Aug-2021 01:00 (BST)"),
    ("20211223", "23-Dec-2021 00:00 (GMT)"),
])
def test_immunization_date_render(date_and_time, expected):

    record = immunization_base_record(date_and_time=date_and_time)

    with BytesIO() as f:
        et = ElementTree(record.text_summary())
        et.write(f)
        text = f.getvalue().decode()
        found = re.search('<th>Date Time</th><td>.*?</td>', text)
        assert found
        assert found.group() == f"<th>Date Time</th><td>{expected}</td>"


@pytest.mark.parametrize("date_and_time, expected", [
    ("20210117T112933.123+03:00", "17-Jan-2021 08:29 (GMT)"),
    ("20210117T11293303", "17-Jan-2021 08:29 (GMT)"),
    ("20210823T170102", "23-Aug-2021 18:01 (BST)"),
    ("20210823", "23-Aug-2021 01:00 (BST)"),
    ("20211223", "23-Dec-2021 00:00 (GMT)"),
])
def test_parse_tz_and_format(date_and_time, expected):

    parsed = parse_tz_date(date_and_time)

    formatted = parsed.astimezone(pytz.timezone("Europe/London")).strftime("%d-%b-%Y %H:%M (%Z)")

    assert formatted == expected
