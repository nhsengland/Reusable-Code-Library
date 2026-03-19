from typing import Dict
from uuid import uuid4
from xml.etree.ElementTree import Element

from schematics.models import Model
from schematics.types import UUIDType


class Base(Model):
    """Base class for DigiMeds objects
    """

    def __getstate__(self):
        return self.to_native()

    def __setstate__(self, kwargs: Dict[str, any]):
        self.__init__(kwargs)

    id = UUIDType(default=uuid4)

    @property
    def xml_reference(self) -> Element:
        return Element("reference", value=f"urn:uuid:{self.id}")

    def to_fhir_xml(self) -> Element:
        raise NotImplemented("This method must be implemented in all descendents")
