from xml.etree.ElementTree import Element, SubElement

from schematics.types import StringType, DateType, ModelType

from dsp.datasets.models.covax.fhir.base import Base
from dsp.datasets.models.covax.fhir.organization import Organization
from dsp.datasets.models.covax.fhir.utils import create_coding


class Patient(Base):
    """Implements CareConnect-Patient-1 FHIR element
    documented here: https://fhir.hl7.org.uk/STU3/StructureDefinition/CareConnect-Patient-1
    For v1.2.3 of Digital Medicines specification
    """

    nhs_number = StringType()
    person_forename = StringType()
    person_surname = StringType()
    person_gender = StringType(choices=["male", "female", "unknown", "other"])
    person_dob = DateType()
    person_postcode = StringType()
    nhs_number_status_code = StringType()
    nhs_number_status_display = StringType()

    general_practitioner = ModelType(Organization)

    def to_fhir_xml(self):
        root = Element("Patient", xmlns="http://hl7.org/fhir")
        SubElement(root, "id", value=str(self.id))

        meta = SubElement(root, "meta")
        SubElement(
            meta,
            "profile",
            value="https://fhir.hl7.org.uk/STU3/StructureDefinition/CareConnect-Patient-1",
        )

        identifier = SubElement(root, "identifier")
        extension = SubElement(
            identifier,
            "extension",
            url="https://fhir.hl7.org.uk/STU3/StructureDefinition/Extension-CareConnect-NHSNumberVerificationStatus-1",
        )
        valueCodeableConcept = SubElement(extension, "valueCodeableConcept")
        create_coding(
            valueCodeableConcept,
            "https://fhir.hl7.org.uk/STU3/CodeSystem/CareConnect-NHSNumberVerificationStatus-1",
            str(self.nhs_number_status_code),
            self.nhs_number_status_display,
        )
        SubElement(identifier, "system", value="https://fhir.nhs.uk/Id/nhs-number")
        SubElement(identifier, "value", value=self.nhs_number)

        if self.person_forename or self.person_surname:
            name = SubElement(root, "name")
            SubElement(name, "use", value="official")
            if self.person_surname:
                SubElement(name, "family", value=self.person_surname)
            if self.person_forename:
                for name_piece in self.person_forename.split(" "):
                    SubElement(name, "given", value=name_piece)

        if self.person_gender:
            SubElement(root, "gender", value=self.person_gender)
        if self.person_dob:
            SubElement(root, "birthDate", value=self.person_dob.isoformat())

        if self.person_postcode:
            address = SubElement(root, "address")
            SubElement(address, "use", value="home")
            SubElement(address, "postalCode", value=self.person_postcode)

        gp = SubElement(root, "generalPractitioner")
        gp.append(self.general_practitioner.xml_reference)

        return root

    @property
    def full_name(self):
        if self.person_surname and self.person_forename:
            return f"{self.person_surname.title()}, {self.person_forename.title()}"
        elif self.person_surname and not self.person_forename:
            return self.person_surname.title()
        elif not self.person_surname and self.person_forename:
            return self.person_forename.title()
        else:
            return ""

    @property
    def admin_name(self):
        if self.person_surname and self.person_forename:
            return f"{self.person_surname.upper()}, {self.person_forename.title()}"
        elif self.person_surname and not self.person_forename:
            return self.person_surname.upper()
        elif not self.person_surname and self.person_forename:
            return self.person_forename.title()
        else:
            return ""

    def text_summary(self):
        root = Element("text")
        SubElement(root, "status", value="additional")
        div = SubElement(root, "div", xmlns="http://www.w3.org/1999/xhtml")
        table = SubElement(div, "table", width="100%")
        body = SubElement(table, "tbody")

        if self.person_forename or self.person_surname:
            row = SubElement(body, "tr")
            SubElement(row, "th").text = f"Patient name"
            td = SubElement(row, "td")
            if self.person_forename:
                SubElement(td, "p").text = f"Given Name: {self.person_forename}"
            if self.person_surname:
                SubElement(td, "p").text = f"Family Name: {self.person_surname}"

        if self.person_dob:
            row = SubElement(body, "tr")
            SubElement(row, "th").text = f"Date of birth"
            SubElement(row, "td").text = self.person_dob.strftime("%d %B %Y")

        if self.person_gender:
            row = SubElement(body, "tr")
            SubElement(row, "th").text = "Sex"
            SubElement(row, "td").text = self.person_gender.title()

        if self.nhs_number:
            row = SubElement(body, "tr")
            SubElement(row, "th").text = "NHS Number"
            SubElement(row, "td").text = self.nhs_number

        if self.person_postcode:
            row = SubElement(body, "tr")
            SubElement(row, "th").text = "Patient address"
            td = SubElement(row, "td")
            SubElement(td, "p").text = self.person_postcode

        return root
