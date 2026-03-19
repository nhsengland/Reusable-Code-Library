from datetime import datetime, timezone
from xml.etree.ElementTree import Element, SubElement

from schematics.types import ModelType, StringType, BooleanType

from dsp.datasets.models.covax.fhir.base import Base
from dsp.datasets.models.covax.fhir.document_bundle import DocumentBundle
from dsp.datasets.models.covax.fhir.organization import HeaderOrganization


message_def_lookup = {"FA": "For Action", "FI": "For Information"}

event_lookup = {
    "ITK003D": "ITK eDischarge",
    "ITK004D": "ITK Mental Health eDischarge",
    "ITK005D": "ITK Emergency Care eDischarge",
    "ITK006D": "ITK Outpatient Letter",
    "ITK007C": "ITK GP Connect Send Document",
    "ITK008M": "ITK Response",
    "ITK009D": "ITK Digital Medicine Immunization Document",
    "ITK010D": "ITK Digital Medicine Emergency Supply Document",
    "ITK011M": "ITK Events Management Service",
    "ITK012M": "ITK National Pathology",
}


class MessageHeader(Base):
    """Implements ITK-MessageHeader-2 FHIR element
    documented here: https://fhir.nhs.uk/STU3/StructureDefinition/ITK-MessageHeader-2
    For v1.2.3 of Digital Medicines specification
    """

    receiver = ModelType(HeaderOrganization)
    sender = ModelType(HeaderOrganization)
    # Our MESH mailbox id
    source = StringType(required=True)
    business_ack = BooleanType(default=True)
    infrastructure_ack = BooleanType(default=True)
    recipient_type = StringType(choices=list(message_def_lookup.keys()), default="FA")
    event_type = StringType(choices=list(event_lookup.keys()), default="ITK009D")
    message_definition = StringType(
        default="https://fhir.nhs.uk/STU3/MessageDefinition/ITK-DM-Immunization-MessageDefinition-1"
    )
    sender_reference = StringType(default="None")
    local_extension = StringType(default="None")
    message = ModelType(DocumentBundle)

    def to_fhir_xml(self):
        root = Element("MessageHeader")
        SubElement(root, "id", value=str(self.id))
        meta = SubElement(root, "meta")
        SubElement(
            meta,
            "profile",
            value="https://fhir.nhs.uk/STU3/StructureDefinition/ITK-MessageHeader-2",
        )
        extension = SubElement(
            root,
            "extension",
            url="https://fhir.nhs.uk/STU3/StructureDefinition/Extension-ITK-MessageHandling-2",
        )

        ext = SubElement(extension, "extension", url="BusAckRequested")
        SubElement(ext, "valueBoolean", value="true" if self.business_ack else "false")

        ext = SubElement(extension, "extension", url="InfAckRequested")
        SubElement(
            ext, "valueBoolean", value="true" if self.infrastructure_ack else "false"
        )

        ext = SubElement(extension, "extension", url="RecipientType")
        val = SubElement(ext, "valueCoding")
        SubElement(
            val,
            "system",
            value="https://fhir.nhs.uk/STU3/CodeSystem/ITK-RecipientType-1",
        )
        SubElement(val, "code", value=self.recipient_type)
        SubElement(val, "display", value=message_def_lookup[self.recipient_type])

        ext = SubElement(extension, "extension", url="MessageDefinition")
        val = SubElement(ext, "valueReference")
        SubElement(val, "reference", value=self.message_definition)

        ext = SubElement(extension, "extension", url="SenderReference")
        SubElement(ext, "valueString", value=self.sender_reference)

        ext = SubElement(extension, "extension", url="LocalExtension")
        SubElement(ext, "valueString", value=self.local_extension)

        event = SubElement(root, "event")
        SubElement(
            event,
            "system",
            value="https://fhir.nhs.uk/STU3/CodeSystem/ITK-MessageEvent-2",
        )
        SubElement(event, "code", value=self.event_type)
        SubElement(event, "display", value=event_lookup[self.event_type])

        receiver = SubElement(root, "receiver")
        receiver.append(self.receiver.xml_reference)

        sender = SubElement(root, "sender")
        sender.append(self.sender.xml_reference)

        SubElement(
            root,
            "timestamp",
            value=datetime.now(timezone.utc).isoformat(
                timespec="seconds"
            ),
        )

        source = SubElement(root, "source")
        SubElement(source, "endpoint", value=self.source)

        focus = SubElement(root, "focus")
        focus.append(self.message.xml_reference)

        return root
