from uuid import uuid4
from xml.etree.ElementTree import Element, SubElement

from schematics.types import StringType, DateType, ModelType, BooleanType


from dsp.datasets.models.covax.fhir.base import Base
from dsp.datasets.models.covax.fhir.patient import Patient
from dsp.datasets.models.covax.fhir.encounter import Encounter
from dsp.datasets.models.covax.fhir.practitioner import Practitioner
from dsp.datasets.models.covax.fhir.organization import Organization
from dsp.datasets.models.covax.fhir.utils import create_snomed_code_container
from dsp.datasets.models.covax.fhir.allergy_list import AllergyList
from dsp.datasets.models.covax.fhir.immunization import Immunization


class ImmunizationComposition(Base):
    """Implements CareConnect-Immunization-1 FHIR element
    documented here: https://fhir.nhs.uk/STU3/StructureDefinition/CareConnect-ITK-DM-Immunization-Composition-1
    For v1.2.3 of Digital Medicines specification
    """

    current_date = DateType()
    care_setting_type_code = StringType()
    care_setting_type_description = StringType()

    author = ModelType(Practitioner)
    title = StringType(default="COVID-19 Vaccination Record")
    delete = BooleanType(default=False)
    # Patients gp
    allergies = ModelType(AllergyList)
    custodian = ModelType(Organization)
    subject = ModelType(Patient)
    immunization = ModelType(Immunization)
    encounter = ModelType(Encounter)

    identifier = StringType(default=lambda: str(uuid4()))
    identifier_system = StringType(default="https://tools.ietf.org/html/rfc4122")

    treatment_consent = StringType(default="No consent information was provided.")
    info_consent = StringType()

    relatesTo = StringType()

    def to_fhir_xml(self):
        root = Element("Composition")
        SubElement(root, "id", value=str(self.id))

        meta = SubElement(root, "meta")
        SubElement(
            meta,
            "profile",
            value="https://fhir.nhs.uk/STU3/StructureDefinition/CareConnect-ITK-DM-Immunization-Composition-1",
        )

        if self.care_setting_type_code:
            care_setting_extension = SubElement(
                root,
                "extension",
                url="https://fhir.nhs.uk/STU3/StructureDefinition/Extension-CareSettingType-1",
            )
            create_snomed_code_container(
                care_setting_extension,
                code=self.care_setting_type_code,
                description=self.care_setting_type_description,
                container="valueCodeableConcept",
            )

        identifier = SubElement(root, "identifier")
        SubElement(identifier, "system", value=self.identifier_system)
        SubElement(identifier, "value", value=str(self.identifier))

        status = "entered-in-error" if self.delete else "final"
        SubElement(root, "status", value=status)
        create_snomed_code_container(
            root,
            code="41000179103",
            description="Immunization record",
            container="type",
        )

        subject = SubElement(root, "subject")
        subject.append(self.subject.xml_reference)

        encounter = SubElement(root, "encounter")
        encounter.append(self.encounter.xml_reference)

        SubElement(root, "date", value=self.current_date.isoformat())

        author = SubElement(root, "author")
        author.append(self.author.xml_reference)

        SubElement(root, "title", value=self.title)
        if self.relatesTo is not None:
            rt = SubElement(root, "relatesTo")
            SubElement(rt, "code", value="replaces")
            SubElement(
                SubElement(rt, "targetIdentifier"), "value", value=self.relatesTo
            )

        custodian = SubElement(root, "custodian")
        custodian.append(self.custodian.xml_reference)

        #
        # BEGIN HTML SECTION
        #
        # Allergies and Adverse Reactions
        if self.allergies is not None:
            allergies_section = create_snomed_code_container(
                root,
                code="886921000000105",
                description="Allergies and adverse reactions",
                title="Allergies and adverse reactions",
                container="section",
                subcontainer="code",
            )
            allergies_section.append(self.allergies.text_summary())
            allergies_section_entry = SubElement(allergies_section, "entry")
            allergies_section_entry.append(self.allergies.xml_reference)

        # Consent
        consent_section = create_snomed_code_container(
            root,
            code="61861000000100",
            description="Consent",
            title="Consent",
            container="section",
            subcontainer="code",
        )
        consent_section.append(self.consent_summary())

        # Patient's GP
        gp_section = create_snomed_code_container(
            root,
            code="886711000000101",
            description="GP practice",
            title="GP practice",
            container="section",
            subcontainer="code",
        )
        gp_section.append(self.custodian.text_summary())
        gp_section_entry = SubElement(gp_section, "entry")
        gp_section_entry.append(self.custodian.xml_reference)

        # Vaccinations
        vaccinations_section = create_snomed_code_container(
            root,
            code="1102181000000102",
            description="Immunisations",
            title="Vaccinations",
            container="section",
            subcontainer="code",
        )
        vaccinations_section.append(self.immunization.text_summary())
        vaccinations_section_entry = SubElement(vaccinations_section, "entry")
        vaccinations_section_entry.append(self.immunization.xml_reference)

        # Patient demographics
        demographics_section = create_snomed_code_container(
            root,
            code="886731000000109",
            description="Patient demographics",
            title="Patient demographics",
            container="section",
            subcontainer="code",
        )
        demographics_section.append(self.subject.text_summary())
        demographics_section_entry = SubElement(demographics_section, "entry")
        demographics_section_entry.append(self.subject.xml_reference)
        #
        # END HTML
        #

        return root

    def consent_summary(self):
        root = Element("text")
        SubElement(root, "status", value="additional")
        div = SubElement(root, "div", xmlns="http://www.w3.org/1999/xhtml")
        table = SubElement(div, "table", width="100%")
        body = SubElement(table, "tbody")

        row = SubElement(body, "tr")
        SubElement(row, "th").text = "Consent for treatment record"
        SubElement(row, "td").text = self.treatment_consent

        if self.info_consent:
            row = SubElement(body, "tr")
            SubElement(row, "th").text = "Consent for information sharing"
            SubElement(row, "td").text = self.info_consent

        return root
