from xml.etree.ElementTree import Element, SubElement

from schematics.types import StringType
from dsp.datasets.models.covax.fhir.base import Base


class Location(Base):
    """Implements CareConnect-Location-1 FHIR element
    documented here: https://fhir.hl7.org.uk/STU3/StructureDefinition/CareConnect-Location-1
    For v1.2.3 of Digital Medicines specification
    """

    site_code = StringType()
    site_name = StringType()
    site_code_type_uri = StringType()

    def to_fhir_xml(self):
        root = Element("Location")
        SubElement(root, "id", value=str(self.id))

        meta = SubElement(root, "meta")
        SubElement(
            meta,
            "profile",
            value="https://fhir.hl7.org.uk/STU3/StructureDefinition/CareConnect-Location-1",
        )

        identifier = SubElement(root, "identifier")
        SubElement(identifier, "system", value=self.site_code_type_uri)
        SubElement(identifier, "value", value=self.site_code)

        if self.site_name:
            SubElement(root, "name", value=self.site_name)

        return root
