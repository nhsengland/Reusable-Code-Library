from collections import defaultdict, OrderedDict
from functools import partial
from io import BytesIO, StringIO

from lxml import etree as ET


def _etree_to_dict(t, retain_namespaces: bool): #function for interracting with xml using etree to dict 
    children = list(t)

    def tag_name(tag):
        return tag if retain_namespaces else ET.QName(tag).localname

    this_tag = tag_name(t.tag)

    d = OrderedDict({this_tag: {} if t.attrib or children else None})

    if t.attrib:
        d[this_tag].update(('@' + tag_name(k), v)
                           for k, v in t.attrib.items())

    children = list(t)
    if children:
        dd = defaultdict(list)
        _etree_to_dict_fn = partial(_etree_to_dict, retain_namespaces=retain_namespaces)
        for dc in map(_etree_to_dict_fn, children):
            for k, v in dc.items():
                dd[k].append(v)
        d[this_tag].update({tag_name(k): v[0] if len(v) == 1 else v
                            for k, v in dd.items()})

    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
                d[this_tag]['#text'] = text
        else:
            d[this_tag] = text

    return d


def xml_to_dict(xml_data: str, retain_namespaces=True):
    """
    There are 3rd party libraries that do this, such as xmltodict, but measuring against a 70MB file,
    this produced the same output in the half the time.
    Access elements as keys, attributes as keys prefixed by @ and text nodes under the #text key or the value.
    """
    return _etree_to_dict(ET.parse(StringIO(xml_data)).getroot(), retain_namespaces)


def xml_bytes_to_dict(xml_data: bytes, retain_namespaces=True):
    """
    There are 3rd party libraries that do this, such as xmltodict, but measuring against a 70MB file,
    this produced the same output in the half the time.
    Access elements as keys, attributes as keys prefixed by @ and text nodes under the #text key or the value.
    """
    return _etree_to_dict(ET.parse(BytesIO(xml_data)).getroot(), retain_namespaces)
