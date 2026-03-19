from uuid import uuid4
from xml.etree.ElementTree import Element, SubElement

from schematics.types import PolyModelType, StringType, ListType

from dsp.datasets.models.covax.fhir.base import Base
from dsp.datasets.models.covax.fhir.patient import Patient
from dsp.datasets.models.covax.fhir.practitioner import Practitioner
from dsp.datasets.models.covax.fhir.encounter import Encounter
from dsp.datasets.models.covax.fhir.organization import Organization
from dsp.datasets.models.covax.fhir.immunization import Immunization
from dsp.datasets.models.covax.fhir.location import Location
from dsp.datasets.models.covax.fhir.immunization_composition import ImmunizationComposition
from dsp.datasets.models.covax.fhir.allergy_list import AllergyList
from dsp.datasets.models.covax.fhir.allergy_intolerance import AllergyIntolerance


class DocumentBundle(Base):
    """Implements ITK-Document-Bundle-1 FHIR element
    documented here: https://fhir.nhs.uk/STU3/StructureDefinition/ITK-Document-Bundle-1
    For v1.2.3 of Digital Medicines specification
    """

    identifier = StringType(default=lambda: str(uuid4()))
    identifier_system = StringType(default="https://tools.ietf.org/html/rfc4122")
    entries = ListType(
        PolyModelType(
            [
                ImmunizationComposition,
                Immunization,
                Organization,
                Practitioner,
                Patient,
                Location,
                Encounter,
                AllergyIntolerance,
                AllergyList
            ]
        )
    )  # All valid resource models go here

    def to_fhir_xml(self):
        root = Element("Bundle", xmlns="http://hl7.org/fhir")
        SubElement(root, "id", value=str(self.id))
        meta = SubElement(root, "meta")
        SubElement(
            meta,
            "profile",
            value="https://fhir.nhs.uk/STU3/StructureDefinition/ITK-Document-Bundle-1",
        )
        identifier = SubElement(root, "identifier")
        SubElement(identifier, "system", value=self.identifier_system)
        SubElement(identifier, "value", value=self.identifier)
        SubElement(root, "type", value="document")
        for entry in self.entries:
            element = SubElement(root, "entry")
            SubElement(element, "fullUrl", value=f"urn:uuid:{entry.id}")
            resource = SubElement(element, "resource")
            resource.append(entry.to_fhir_xml())
        return root
