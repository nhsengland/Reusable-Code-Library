from xml.etree.ElementTree import Element, SubElement

from schematics.types import StringType, ModelType, DateType, DateTimeType

from dsp.datasets.models.covax.fhir.base import Base
from dsp.datasets.models.covax.fhir.patient import Patient
from dsp.datasets.models.covax.fhir.utils import create_snomed_code_container


def tr(head, body):
    root = Element("tr")
    SubElement(root, "th").text = head
    SubElement(root, "td").text = body
    return root


class AllergyIntolerance(Base):
    """Implements CareConnect-ITK-AllergyIntolerance-1 FHIR element
    documented here: https://fhir.nhs.uk/STU3/StructureDefinition/CareConnect-ITK-AllergyIntolerance-1
    For v1.2.3 of Digital Medicines specification
    """

    unique_id_uri = StringType()
    unique_id = StringType()

    clinical_status = StringType(choices=["active", "inactive", "resolved"])
    verification_status = StringType(
        required=True, choices=["unconfirmed", "confirmed", "entered-in-error"]
    )

    reaction_type = StringType(choices=["allergy", "intolerance"])
    category = StringType(choices=["food", "medication", "environment", "biologic"])

    allergy_code = StringType(required=True)
    allergy_display = StringType()

    patient = ModelType(Patient)

    asserted_date = DateType(required=True)

    reaction_code = StringType()
    reaction_display = StringType()
    reaction_description = StringType()
    reaction_severity = StringType()
    onset = DateTimeType()

    def to_fhir_xml(self):
        root = Element("AllergyIntolerance")
        SubElement(root, "id", value=str(self.id))

        meta = SubElement(root, "meta")
        SubElement(
            meta,
            "profile",
            value="https://fhir.nhs.uk/STU3/StructureDefinition/CareConnect-ITK-AllergyIntolerance-1",
        )

        identifier = SubElement(root, "identifier")
        SubElement(identifier, "system", value=self.unique_id_uri)
        SubElement(identifier, "value", value=self.unique_id)

        if self.clinical_status and self.verification_status != "entered-in-error":
            SubElement(root, "clinicalStatus", value=self.clinical_status)

        SubElement(root, "verificationStatus", value=self.verification_status)
        if self.reaction_type:
            SubElement(root, "type", value=self.reaction_type)
        if self.category:
            SubElement(root, "category", value=self.category)

        create_snomed_code_container(
            root,
            code=self.allergy_code,
            description=self.allergy_display,
            container="code",
        )

        patient = SubElement(root, "patient")
        patient.append(self.patient.xml_reference)
        SubElement(patient, "display", value=self.patient.admin_name)

        SubElement(root, "assertedDate", value=self.asserted_date.isoformat())

        if self.reaction_code:
            reaction = SubElement(root, "reaction")
            create_snomed_code_container(
                reaction,
                code=self.reaction_code,
                description=self.reaction_display,
                container="manifestation",
            )
            if self.reaction_description:
                SubElement(reaction, "description", value=self.reaction_description)
            if self.onset:
                SubElement(reaction, "onset", value=self.onset.isoformat())
            if self.reaction_severity:
                SubElement(reaction, "severity", value=self.reaction_severity)

        return root

    def text_summary(self):
        table = Element("table", width="100%")
        body = SubElement(table, "tbody")

        if self.allergy_display:
            body.append(tr("Causative agent", self.allergy_display))

        if self.reaction_code:
            body.append(tr("Description of reaction", self.reaction_description))
            body.append(tr("SNOMED Code of reaction", f"{self.reaction_code} ({self.reaction_display})"))

        if self.reaction_severity:
            body.append(tr("Severity", self.reaction_severity))

        if self.verification_status:
            body.append(tr("Certainty", self.verification_status))

        if self.asserted_date:
            body.append(
                tr("Date recorded", self.asserted_date.strftime("%-d %b %Y"))
            )

        return table
