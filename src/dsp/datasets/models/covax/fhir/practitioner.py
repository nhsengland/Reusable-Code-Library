from xml.etree.ElementTree import Element, SubElement

from schematics.types import StringType

from dsp.datasets.models.covax.fhir.base import Base


class Practitioner(Base):
    """Implements CareConnect-Organization-1 FHIR element
    documented here: https://fhir.hl7.org.uk/STU3/StructureDefinition/CareConnect-Practitioner-1
    For v1.2.3 of Digital Medicines specification
    """

    performing_professional_body_reg_uri = StringType()
    performing_professional_body_reg_code = StringType()
    performing_professional_forename = StringType(default="")
    performing_professional_surname = StringType(default="")

    def to_fhir_xml(self):
        root = Element("Practitioner")
        SubElement(root, "id", value=str(self.id))

        meta = SubElement(root, "meta")
        SubElement(
            meta,
            "profile",
            value="https://fhir.hl7.org.uk/STU3/StructureDefinition/CareConnect-Practitioner-1",
        )

        if self.performing_professional_body_reg_code and self.performing_professional_body_reg_uri:
            identifier = SubElement(root, "identifier")
            SubElement(
                identifier, "system", value=self.performing_professional_body_reg_uri
            )
            SubElement(
                identifier, "value", value=self.performing_professional_body_reg_code
            )

        if (
                self.performing_professional_surname
                or self.performing_professional_forename
        ):
            name = SubElement(root, "name")
            SubElement(name, "use", value="official")
            if self.performing_professional_surname:
                SubElement(name, "family", value=self.performing_professional_surname)
            if self.performing_professional_forename:
                for name_piece in self.performing_professional_forename.split(" "):
                    SubElement(name, "given", value=name_piece)

        return root

    @property
    def full_name(self) -> str:
        if (
                not self.performing_professional_surname
                or not self.performing_professional_forename
        ):
            return ""
        return f"{self.performing_professional_forename.title()} {self.performing_professional_surname.title()}"

    @property
    def admin_name(self) -> str:
        if (
                not self.performing_professional_surname
                or not self.performing_professional_forename
        ):
            return ""
        return f"{self.performing_professional_surname.upper()}, {self.performing_professional_forename.title()}"

    @property
    def performing_professional_body_reg_type(self) -> str:
        reg_lookup = {
            "https://fhir.hl7.org.uk/Id/nmc-number": "NMC",
            "https://fhir.hl7.org.uk/Id/gmc-number": "GMC",
            "https://fhir.hl7.org.uk/Id/gphc-number": "GPHC",
        }
        try:
            return reg_lookup[self.performing_professional_body_reg_uri]
        except KeyError:
            # This only goes into the HTML, so we don't care enough about it to
            # error if we get a body we haven't seen before
            return "Performing professional professional body"
