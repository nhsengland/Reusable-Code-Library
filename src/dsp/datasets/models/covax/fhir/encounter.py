from datetime import timezone
from xml.etree.ElementTree import Element, SubElement

from schematics.types import StringType, DateTimeType, ModelType

from dsp.datasets.models.covax.fhir.base import Base
from dsp.datasets.models.covax.fhir.patient import Patient
from dsp.datasets.models.covax.fhir.practitioner import Practitioner
from dsp.datasets.models.covax.fhir.organization import Organization
from dsp.datasets.models.covax.fhir.utils import create_coding


class Encounter(Base):
    """Implements CareConnect-Patient-1 FHIR element
    documented here: https://fhir.nhs.uk/STU3/StructureDefinition/CareConnect-ITK-Encounter-1
    For v1.2.3 of Digital Medicines specification
    """

    encounter_date = DateTimeType()

    status = StringType(default="finished")

    subject = ModelType(Patient)
    practitioner = ModelType(Practitioner)
    # location = ModelType(Location)
    # the vaccine provider (same as Location)
    service_provider = ModelType(Organization)

    def to_fhir_xml(self):
        root = Element("Encounter")
        SubElement(root, "id", value=str(self.id))

        meta = SubElement(root, "meta")
        SubElement(
            meta,
            "profile",
            value="https://fhir.nhs.uk/STU3/StructureDefinition/CareConnect-ITK-Encounter-1",
        )

        SubElement(root, "status", value=self.status)

        subject = SubElement(root, "subject")
        subject.append(self.subject.xml_reference)

        participant = SubElement(root, "participant")
        participant_type = SubElement(participant, "type")
        create_coding(
            participant_type,
            "http://hl7.org/fhir/v3/ParticipationType",
            "PPRF",
            "primary performer",
        )

        participant_individual = SubElement(participant, "individual")
        participant_individual.append(self.practitioner.xml_reference)
        # SubElement(
        #     participant_individual, "display", value=self.practitioner.admin_name
        # )

        period = SubElement(root, "period")
        SubElement(
            period,
            "start",
            value=self.encounter_date.astimezone(timezone.utc).isoformat(timespec="seconds")
        )

        # Based on feedback from TPP we are sending service provider rather than location
        # location = SubElement(root, "location")
        # location_2 = SubElement(location, "location")
        # location_2.append(self.location.xml_reference)

        if self.service_provider:
            provider = SubElement(root, "serviceProvider")
            provider.append(self.service_provider.xml_reference)

        return root
