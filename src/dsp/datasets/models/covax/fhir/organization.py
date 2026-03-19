from xml.etree.ElementTree import Element, SubElement

from schematics.types import StringType

from dsp.datasets.models.covax.fhir.base import Base


code_lookup = {
    "org": "https://fhir.nhs.uk/Id/ods-organization-code",
    "site": "https://fhir.nhs.uk/Id/ods-site-code",
}


class Organization(Base):
    """Implements CareConnect-Organization-1 FHIR element
    documented here: https://fhir.hl7.org.uk/STU3/StructureDefinition/CareConnect-Organization-1
    For v1.2.3 of Digital Medicines specification
    """

    site_code = StringType()
    site_name = StringType()
    ods_code_type = StringType(choices=["org", "site"], default="org")
    site_code_type_uri = StringType()

    def to_fhir_xml(self):
        assert self.site_code or self.site_name
        root = Element("Organization")
        SubElement(root, "id", value=str(self.id))

        meta = SubElement(root, "meta")
        SubElement(
            meta,
            "profile",
            value="https://fhir.hl7.org.uk/STU3/StructureDefinition/CareConnect-Organization-1",
        )

        if self.site_code:
            identifier = SubElement(root, "identifier")
            SubElement(
                identifier,
                "system",
                value=self.site_code_type_uri
                if self.site_code_type_uri
                else code_lookup[self.ods_code_type],
            )
            SubElement(identifier, "value", value=self.site_code)

        if self.site_name:
            SubElement(root, "name", value=self.site_name)

        return root

    def text_summary(self, org_type="GP practice"):
        root = Element("text")
        SubElement(root, "status", value="additional")
        div = SubElement(root, "div", xmlns="http://www.w3.org/1999/xhtml")
        table = SubElement(div, "table", width="100%")
        body = SubElement(table, "tbody")

        if self.site_code:
            row = SubElement(body, "tr")
            SubElement(row, "th").text = f"{org_type} identifier"
            td = SubElement(row, "td")
            if org_type == "org":
                SubElement(td, "p").text = f"ODS Organization Code: {self.site_code}"
            else:
                SubElement(td, "p").text = f"ODS Site Code: {self.site_code}"

        if self.site_name:
            row = SubElement(body, "tr")
            SubElement(row, "th").text = f"{org_type} details"
            td = SubElement(row, "td")
            SubElement(td, "p").text = f"{org_type} name: {self.site_name}"

        return root


class HeaderOrganization(Base):
    """Implements CareConnect-ITK-Header-Organization-1 FHIR element
    documented here: https://fhir.nhs.uk/STU3/StructureDefinition/CareConnect-ITK-Header-Organization-1
    For v1.2.3 of Digital Medicines specification
    """

    site_code = StringType()
    ods_code_type = StringType(choices=["org", "site"], default="org")

    def to_fhir_xml(self):
        assert self.site_code or self.site_name
        root = Element("Organization")
        SubElement(root, "id", value=str(self.id))

        meta = SubElement(root, "meta")
        SubElement(
            meta,
            "profile",
            value="https://fhir.nhs.uk/STU3/StructureDefinition/CareConnect-ITK-Header-Organization-1",
        )

        identifier = SubElement(root, "identifier")
        SubElement(identifier, "system", value=code_lookup[self.ods_code_type])
        SubElement(identifier, "value", value=self.site_code)

        return root
