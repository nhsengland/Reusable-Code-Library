from xml.etree.ElementTree import Element, SubElement

from schematics.types import (
    ModelType,
    ListType,
    DateType,
)

from dsp.datasets.models.covax.fhir.base import Base
from dsp.datasets.models.covax.fhir.patient import Patient
from dsp.datasets.models.covax.fhir.practitioner import Practitioner
from dsp.datasets.models.covax.fhir.encounter import Encounter
from dsp.datasets.models.covax.fhir.allergy_intolerance import AllergyIntolerance
from dsp.datasets.models.covax.fhir.utils import create_snomed_code_container


class AllergyList(Base):
    """Implements CareConnect-ITK-Allergy-List-1 FHIR element
    documented here: https://fhir.nhs.uk/STU3/StructureDefinition/CareConnect-ITK-Allergy-List-1
    For v1.2.3 of Digital Medicines specification
    """

    subject = ModelType(Patient)
    source = ModelType(Practitioner)
    encounter = ModelType(Encounter)

    allergies = ListType(ModelType(AllergyIntolerance), required=True, min_size=1)

    def to_fhir_xml(self):
        root = Element("List")
        SubElement(root, "id", value=str(self.id))

        meta = SubElement(root, "meta")
        SubElement(
            meta,
            "profile",
            value="https://fhir.nhs.uk/STU3/StructureDefinition/CareConnect-ITK-Allergy-List-1",
        )

        # In accordance with example, this is just the UUID of the section
        identifier = SubElement(root, "identifier")
        SubElement(identifier, "system", value="https://tools.ietf.org/html/rfc4122")
        SubElement(identifier, "value", value=str(self.id))

        SubElement(root, "status", value="current")
        SubElement(root, "mode", value="snapshot")

        create_snomed_code_container(
            root,
            code="886921000000105",
            description="Allergies and adverse reactions",
            container="code",
        )

        subject = SubElement(root, "subject")
        subject.append(self.subject.xml_reference)
        SubElement(subject, "display", value=self.subject.admin_name)

        encounter = SubElement(root, "encounter")
        encounter.append(self.encounter.xml_reference)

        source = SubElement(root, "source")
        source.append(self.source.xml_reference)
        SubElement(source, "display", value=self.source.admin_name)

        for allergy in self.allergies:
            entry = SubElement(root, "entry")
            SubElement(entry, "item").append(allergy.xml_reference)

        return root

    def text_summary(self):
        root = Element("text")
        SubElement(root, "status", value="additional")
        div = SubElement(root, "div", xmlns="http://www.w3.org/1999/xhtml")

        for allergy in self.allergies:
            div.append(allergy.text_summary())
        return root
