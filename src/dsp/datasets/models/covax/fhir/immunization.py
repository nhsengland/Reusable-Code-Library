import pytz
from datetime import datetime, timezone, time
from uuid import uuid4
from xml.etree.ElementTree import Element, SubElement

from schematics.types import (
    StringType,
    DateType,
    DateTimeType,
    DecimalType,
    ModelType,
    BooleanType,
    IntType,
)


from dsp.datasets.models.covax.fhir.base import Base
from dsp.datasets.models.covax.fhir.location import Location
from dsp.datasets.models.covax.fhir.patient import Patient
from dsp.datasets.models.covax.fhir.encounter import Encounter

from dsp.datasets.models.covax.fhir.practitioner import Practitioner
from dsp.datasets.models.covax.fhir.organization import Organization
from dsp.datasets.models.covax.fhir.utils import create_coding, create_snomed_code_container


class Immunization(Base):
    """Implements CareConnect-Immunization-1 FHIR element
    documented here: https://fhir.nhs.uk/STU3/StructureDefinition/CareConnect-ITK-DM-Immunization-1
    For v1.2.3 of Digital Medicines specification
    """

    record_date = DateType()
    unique_id_uri = StringType(default="https://tools.ietf.org/html/rfc4122")
    unique_id = StringType(default=lambda: str(uuid4()))
    vaccination_procedure_code = StringType()
    vaccination_procedure_term = StringType()
    vaccination_situation_code = StringType()
    vaccination_situation_term = StringType()
    action_flag = StringType()
    not_given = BooleanType()
    vaccine_product_code = StringType()
    vaccine_product_term = StringType()
    batch_number = StringType()
    expiry_date = DateType()
    date_and_time = DateTimeType()
    primary_source = BooleanType()
    vaccine_manufacturer = StringType()
    site_of_vaccination_code = StringType()
    site_of_vaccination_term = StringType()
    route_of_vaccination_code = StringType()
    route_of_vaccination_term = StringType()
    indication_code = StringType()
    indication_term = StringType()
    reason_not_given_code = StringType()
    reason_not_given_term = StringType()
    dose_unit_code = StringType()
    dose_unit_term = StringType()
    dose_amount = DecimalType()
    dose_sequence = IntType()

    practitioner = ModelType(Practitioner)
    manufacturer = ModelType(Organization)
    patient = ModelType(Patient)
    location = ModelType(Location)
    encounter = ModelType(Encounter)

    def to_fhir_xml(self):
        root = Element("Immunization")
        SubElement(root, "id", value=str(self.id))

        meta = SubElement(root, "meta")
        SubElement(
            meta,
            "profile",
            value="https://fhir.nhs.uk/STU3/StructureDefinition/CareConnect-ITK-DM-Immunization-1",
        )

        if self.record_date is not None:
            record_date_extension = SubElement(
                root,
                "extension",
                url="https://fhir.hl7.org.uk/STU3/StructureDefinition/Extension-CareConnect-DateRecorded-1",
            )
            record_date_as_datetime = datetime.combine(
                self.record_date, time()
            )
            SubElement(
                record_date_extension,
                "valueDateTime",
                value=record_date_as_datetime.astimezone(timezone.utc).isoformat(
                    timespec="seconds"
                ),
            )

        vaccination_procedure_extension = SubElement(
            root,
            "extension",
            url="https://fhir.hl7.org.uk/STU3/StructureDefinition/Extension-CareConnect-VaccinationProcedure-1",
        )
        if not self.not_given:
            # Base case - vaccination was given
            create_snomed_code_container(
                vaccination_procedure_extension,
                code=self.vaccination_procedure_code,
                description=self.vaccination_procedure_term,
                container="valueCodeableConcept",
            )
        else:
            create_snomed_code_container(
                vaccination_procedure_extension,
                code=self.vaccination_situation_code,
                description=self.vaccination_situation_term,
                container="valueCodeableConcept",
            )

        if self.unique_id:
            identifier = SubElement(root, "identifier")
            SubElement(identifier, "system", value=self.unique_id_uri)
            SubElement(identifier, "value", value=self.unique_id)

        SubElement(root, "status", value="completed")
        SubElement(root, "notGiven", value="true" if self.not_given else "false")

        create_snomed_code_container(
            root,
            code=self.vaccine_product_code,
            description=self.vaccine_product_term,
            container="vaccineCode",
        )

        patient = SubElement(root, "patient")
        patient.append(self.patient.xml_reference)
        SubElement(patient, "display", value=self.patient.admin_name)

        encounter = SubElement(root, "encounter")
        encounter.append(self.encounter.xml_reference)

        SubElement(
            root,
            "date",
            value=self.date_and_time.astimezone(timezone.utc).isoformat(
                timespec="seconds"
            ),
        )
        SubElement(
            root, "primarySource", value="true" if self.primary_source else "false"
        )

        if self.location:
            location = SubElement(root, "location")
            location.append(self.location.xml_reference)

        if self.vaccine_manufacturer:
            manufacturer = SubElement(root, "manufacturer")
            manufacturer.append(self.manufacturer.xml_reference)
            SubElement(manufacturer, "display", value=self.vaccine_manufacturer)

        if not self.not_given:
            if self.batch_number is not None:
                SubElement(root, "lotNumber", value=self.batch_number)
            if self.expiry_date is not None:
                SubElement(root, "expirationDate", value=self.expiry_date.isoformat())

            create_snomed_code_container(
                root,
                code=self.site_of_vaccination_code,
                description=self.site_of_vaccination_term,
                container="site",
            )
            create_snomed_code_container(
                root,
                code=self.route_of_vaccination_code,
                description=self.route_of_vaccination_term,
                container="route",
            )

            if self.dose_amount and self.dose_unit_term and self.dose_unit_code:
                dose_quantity = SubElement(root, "doseQuantity")
                SubElement(dose_quantity, "value", value=str(self.dose_amount))
                SubElement(dose_quantity, "unit", value=self.dose_unit_term)
                SubElement(dose_quantity, "system", value="http://snomed.info/sct")
                SubElement(dose_quantity, "code", value=self.dose_unit_code)

        practitioner = SubElement(root, "practitioner")
        practitioner_role = SubElement(practitioner, "role")
        create_coding(
            practitioner_role,
            "http://hl7.org/fhir/v2/0443",
            "AP",
            "Administering Provider",
        )

        practitioner_actor = SubElement(practitioner, "actor")
        practitioner_actor.append(self.practitioner.xml_reference)
        # SubElement(practitioner_actor, "display", value=self.practitioner.admin_name)

        if not self.not_given:
            if self.indication_code:
                create_snomed_code_container(
                    root,
                    code=self.indication_code,
                    description=self.indication_term,
                    container="explanation",
                    subcontainer="reason",
                )
        else:
            create_snomed_code_container(
                root,
                code=self.reason_not_given_code,
                description=self.reason_not_given_term,
                container="explanation",
                subcontainer="reasonNotGiven",
            )

        # Removed after clinical review 30/11
        # all of this info is contained in snomed codes
        #
        # protocol = SubElement(root, "vaccinationProtocol")
        # SubElement(protocol, "doseSequence", value=str(self.dose_sequence))
        # #     <!-- FHIR Mandatory section - the coding below needs to be confirmed as clinically correct. Taken from NHS Digital release of SNOMED CT codes for COVID-19 - disease labels section -->
        # create_snomed_code_container(
        #     protocol,
        #     code="1240751000000100",
        #     description="Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 (disorder)",
        #     container="targetDisease",
        # )
        # #     <!-- Another FHIR Mandatory section - the coding below is an indication that the vaccination give counts towards immunity -->
        # dose_status = SubElement(protocol, "doseStatus")
        # create_coding(
        #     dose_status,
        #     "http://hl7.org/fhir/vaccination-protocol-dose-status",
        #     "count",
        #     "Counts",
        # )

        return root

    def text_summary(self):
        root = Element("text")
        SubElement(root, "status", value="additional")
        div = SubElement(root, "div", xmlns="http://www.w3.org/1999/xhtml")
        table = SubElement(div, "table", width="100%")
        body = SubElement(table, "tbody")

        row = SubElement(body, "tr")
        SubElement(row, "th").text = "Vaccine product"
        SubElement(row, "td").text = self.vaccine_product_term
        SubElement(row, "td").text = self.vaccine_product_code

        if self.vaccine_manufacturer:
            row = SubElement(body, "tr")
            SubElement(row, "th").text = "Manufacturer"
            SubElement(row, "td").text = self.vaccine_manufacturer

        if self.batch_number:
            row = SubElement(body, "tr")
            SubElement(row, "th").text = "Batch number"
            SubElement(row, "td").text = self.batch_number

        if self.expiry_date:
            row = SubElement(body, "tr")
            SubElement(row, "th").text = "Expiry date"
            SubElement(row, "td").text = self.expiry_date.strftime("%d %b %Y")

        table = SubElement(div, "table", width="100%")
        body = SubElement(table, "tbody")

        row = SubElement(body, "tr")
        SubElement(row, "th").text = "Vaccine procedure"
        SubElement(row, "td").text = self.vaccination_procedure_term
        SubElement(row, "td").text = self.vaccination_procedure_code

        if not self.not_given:
            if self.site_of_vaccination_code:
                row = SubElement(body, "tr")
                SubElement(row, "th").text = "Site"
                SubElement(row, "td").text = self.site_of_vaccination_term
                SubElement(row, "td").text = self.site_of_vaccination_code

            row = SubElement(body, "tr")
            SubElement(row, "th").text = "Route"
            SubElement(row, "td").text = self.route_of_vaccination_term
            SubElement(row, "td").text = self.route_of_vaccination_code

            if self.indication_term:
                row = SubElement(body, "tr")
                SubElement(row, "th").text = "Indication"
                SubElement(row, "td").text = self.indication_term
                SubElement(row, "td").text = self.indication_code
            if self.dose_amount and self.dose_unit_term and self.dose_unit_code:
                row = SubElement(body, "tr")
                SubElement(row, "th").text = "Dose"
                SubElement(row, "td").text = f"{self.dose_amount} {self.dose_unit_term}"
                SubElement(row, "td").text = self.dose_unit_code
        else:
            row = SubElement(body, "tr")
            SubElement(row, "th").text = "Reason not given"
            SubElement(row, "td").text = self.reason_not_given_term
            SubElement(row, "td").text = self.reason_not_given_code

        row = SubElement(body, "tr")
        SubElement(row, "th").text = "Date Time"
        SubElement(row, "td").text = (
            self.date_and_time.astimezone(
                pytz.timezone("Europe/London")
            ).strftime("%d-%b-%Y %H:%M (%Z)")
        )

        row = SubElement(body, "tr")
        SubElement(row, "th").text = "Administered by"
        SubElement(row, "td").text = self.practitioner.full_name

        row = SubElement(body, "tr")
        SubElement(
            row, "th"
        ).text = f"{self.practitioner.performing_professional_body_reg_type} identifier"
        SubElement(
            row, "td"
        ).text = self.practitioner.performing_professional_body_reg_code

        if self.location:
            row = SubElement(body, "tr")
            SubElement(row, "th").text = "Location"
            SubElement(row, "td").text = self.location.site_name
            SubElement(row, "td").text = self.location.site_code

        return root