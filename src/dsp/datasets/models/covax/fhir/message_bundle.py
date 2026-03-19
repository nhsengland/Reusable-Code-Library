from uuid import uuid4
from xml.etree.ElementTree import Element, SubElement

from schematics.types import StringType, ModelType, ListType


from dsp.datasets.models.covax.fhir.base import Base
from dsp.datasets.models.covax.fhir.message_header import MessageHeader

from dsp.datasets.models.covax.fhir.organization import HeaderOrganization
from dsp.datasets.models.covax.fhir.document_bundle import DocumentBundle


class MessageBundle(Base):
    """Implements ITK-Message-Bundle-1 FHIR element
    documented here: https://fhir.nhs.uk/STU3/StructureDefinition/ITK-Message-Bundle-1
    For v1.2.3 of Digital Medicines specification
    """

    identifier = StringType(default=lambda: str(uuid4()))
    identifier_system = StringType(default="https://tools.ietf.org/html/rfc4122")
    messageHeader = ModelType(MessageHeader, required=True)
    organisations = ListType(ModelType(HeaderOrganization))
    document = ModelType(DocumentBundle, required=True)

    def to_fhir_xml(self):
        root = Element("Bundle", xmlns="http://hl7.org/fhir")
        SubElement(root, "id", value=str(self.id))
        meta = SubElement(root, "meta")
        SubElement(
            meta,
            "profile",
            value="https://fhir.nhs.uk/STU3/StructureDefinition/ITK-Message-Bundle-1",
        )
        identifier = SubElement(root, "identifier")
        SubElement(identifier, "system", value=self.identifier_system)
        SubElement(identifier, "value", value=self.identifier)
        SubElement(root, "type", value="message")
        for entry in [self.messageHeader, *self.organisations, self.document]:
            element = SubElement(root, "entry")
            SubElement(element, "fullUrl", value=f"urn:uuid:{entry.id}")
            resource = SubElement(element, "resource")
            resource.append(entry.to_fhir_xml())
        return root
