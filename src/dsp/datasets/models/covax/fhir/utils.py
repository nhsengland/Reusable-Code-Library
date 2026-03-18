from xml.etree.ElementTree import SubElement


def create_coding(element, system_value, code_value, display_value=None):
    element_coding = SubElement(element, "coding")
    SubElement(element_coding, "system", value=system_value)
    SubElement(element_coding, "code", value=code_value)
    if display_value is not None:
        SubElement(element_coding, "display", value=display_value)
    return element_coding


def create_snomed_code_container(
    parent, code=None, description=None, container=None, subcontainer=None, **kwargs
):
    if code is not None:
        c = SubElement(parent, container) if container else parent
        for (tag, value) in kwargs.items():
            SubElement(c, tag, value=value)
        sc = SubElement(c, subcontainer) if subcontainer else c
        create_coding(sc, "http://snomed.info/sct", code, description)
        return c
