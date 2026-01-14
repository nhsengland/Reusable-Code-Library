import pytest
from lxml.etree import XMLSyntaxError

from src.nhs_reusable_code_library.resuable_codes.xml_helpers import xml_to_dict, xml_bytes_to_dict


def test_xml_to_dict_root_only():
    xml = '<xml/>'
    d = xml_to_dict(xml)
    assert d == {"xml": None}


def test_xml_to_dict_root_only_no_encoding():
    xml = '<?xml version="1.0" standalone="yes"?> <xml/>'
    d = xml_to_dict(xml)
    assert d == {"xml": None}


def test_xml_to_dict_root_only_bytes_no_encoding():
    xml = '<?xml version="1.0" standalone="yes"?><xml/>'.encode('UTF-8')
    d = xml_bytes_to_dict(xml)
    assert d == {"xml": None}


def test_xml_to_dict_with_prolog_encoding():
    xml = '<?xml version="1.0" encoding="utf-8" standalone="yes"?><xml/>'.encode('UTF-8')
    d = xml_bytes_to_dict(xml)
    assert d == {"xml": None}


def test_xml_to_dict_with_prolog_invalid_encoding():
    xml = '<?xml version="1.0" encoding="utf-16" standalone="yes"?><xml/>'.encode('UTF-8')

    with pytest.raises(XMLSyntaxError) as excinfo:
        _ = xml_bytes_to_dict(xml)

    assert str(excinfo.value).startswith("Document labelled UTF-16 but has UTF-8 content")


def test_xml_to_dict_root_attr():
    xml = '<xml a="5"/>'
    d = xml_to_dict(xml)
    assert d == {"xml": {"@a": "5"}}


def test_xml_to_dict_root_attrs():
    xml = '<xml a="5" b="4"/>'
    d = xml_to_dict(xml)
    assert d == {"xml": {"@a": "5", "@b": "4"}}


def test_xml_to_dict_with_ns():
    xml = '<xml xmlns="my-schema-uri" a="5" b="4"><data1>sally</data1><data2 c="3">bob</data2></xml>'
    d = xml_to_dict(xml)
    assert d == {
        "{my-schema-uri}xml": {
            "@a": "5",
            "@b": "4",
            "{my-schema-uri}data1": "sally",
            "{my-schema-uri}data2": {
                "@c": "3",
                "#text": "bob"
            }
        }
    }


def test_xml_to_dict_with_ns_discarded():
    xml = '<xml xmlns="my-schema-uri" a="5" b="4"><data1>sally</data1><data2 c="3">bob</data2></xml>'
    d = xml_to_dict(xml, retain_namespaces=False)
    assert d == {
        "xml": {
            "@a": "5",
            "@b": "4",
            "data1": "sally",
            "data2": {
                "@c": "3",
                "#text": "bob"
            }
        }
    }


def test_xml_to_dict_large_example_keep_ns():
    xml = """<GPES-E-Q-QR-Message xmlns="http://www.ic.nhs.uk/GPES-I/Schema"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.ic.nhs.uk/GPES-I/Schema/GPES-E-Q-QR.xsd" GPES-I-Version="6.0" QR-Message-Version="0.2">
    <Query-Results QR-UUID="276C5A44-DCE2-4F9C-B1B2-DDB4374A115C" Scheduled-Query-Instance-ID="A00151-202003310000-R" 
    RTP-UUID="9E0E33F3-004A-48D3-BD3A-0BEF65BC7591" Practice-ID="T00001" 
    GPET-Q-SQI-Authentication-Signature="3B023A7E-96AC-4254-BFFA-C162C0CC349B" GPET-E-SQI-Authentication-Signature="49104CE1-5AB8-4432-9310-91A98B317B26" 
    Issue-Date-Time="2020-03-25T10:04:45" 
    Query-Name="CVDPREVENT 2019-20" 
    Query-Description="This specification supports Patient Level Extract CVDPREVENT for the 2019-20 requirement. The query extracts data for 3 outputs; 3 patient level extracts.">
        <Query-Results-Manifest>
            <GPET-E-Executable-Version>1.0</GPET-E-Executable-Version>
            <Payload-Identifiers>
                <GP-System-Identifier>001.001.483.000</GP-System-Identifier>
                <Terminology-Identifiers>
                    <CodeSchemeID>2.16.840.1.113883.2.1.3.2.4.15</CodeSchemeID>
                    <Terminology-Name>SNOMED-CT</Terminology-Name>
                    <Terminology-Release/>
                </Terminology-Identifiers>
            </Payload-Identifiers>
            <RTP-Translation-Statistics>
                <RTP-Translation-Start-Date-Time>2020-03-25T10:04:45</RTP-Translation-Start-Date-Time>
                <RTP-Translation-End-Date-Time>2020-03-25T10:04:45</RTP-Translation-End-Date-Time>
            </RTP-Translation-Statistics>
            <Query-Execution-Statistics>
                <Query-Execution-Start-Date-Time>2020-03-25T10:04:45</Query-Execution-Start-Date-Time>
                <Query-Execution-End-Date-Time>2020-03-25T10:04:45</Query-Execution-End-Date-Time>
            </Query-Execution-Statistics>
        </Query-Results-Manifest>
        <Query-Results-Records>
            <Aggregate-Record RID="1" AID="957080F1-4EB8-4547-A3C6-267E2A53BD1A" Description="CVDPREVENT 2019-20 Audit Period Start Date (Fixed Date 1900-01-01)">2020-03-31</Aggregate-Record>
            <Aggregate-Record RID="2" AID="41ED40A4-0AFB-4C65-8E5B-94B31132A37F" Description="CVDPREVENT 2019-20 Audit Period End Date (Fixed Date 2020-03-31)">2020-05-31</Aggregate-Record>
            <Patient-Level-Record RID="7">
                <Patient-Table QID="1" LID="1" DATE-OF-BIRTH="1965-05-17" NHS-NUMBER="9979000007" POSTCODE="SR1 3BG" PRACTICE="A34561" SEX="2"/>
                <Journals-Table QID="2" LID="1" DATE="1990-04-20" CODE="107691000000105" VALUE1-CONDITION="" VALUE2-CONDITION="" VALUE1-PRESCRIPTION="" VALUE2-PRESCRIPTION=""/>
                <Journals-Table QID="2" LID="2" DATE="2019-02-15" CODE="162863004" VALUE1-CONDITION="27.9" VALUE2-CONDITION="" VALUE1-PRESCRIPTION="" VALUE2-PRESCRIPTION=""/>
                <Journals-Table QID="2" LID="3" DATE="2019-02-15" CODE="162755006" VALUE1-CONDITION="156" VALUE2-CONDITION="" VALUE1-PRESCRIPTION="" VALUE2-PRESCRIPTION=""/>
            </Patient-Level-Record>
            <Patient-Level-Record RID="8">
                <Patient-Table QID="1" LID="1" DATE-OF-BIRTH="1944-09-13" NHS-NUMBER="9979000023" POSTCODE="DE1 7BN" PRACTICE="A34561" SEX="2"/>
                <Journals-Table QID="2" LID="1" DATE="1990-04-20" CODE="976791000000107" VALUE1-CONDITION="" VALUE2-CONDITION="" VALUE1-PRESCRIPTION="" VALUE2-PRESCRIPTION=""/>
                <Journals-Table QID="2" LID="2" DATE="2019-08-15" CODE="162864005" VALUE1-CONDITION="30" VALUE2-CONDITION="" VALUE1-PRESCRIPTION="" VALUE2-PRESCRIPTION=""/>
            </Patient-Level-Record>
        </Query-Results-Records>
    </Query-Results>
</GPES-E-Q-QR-Message>
    """
    d = xml_to_dict(xml)
    assert d == {
        "{http://www.ic.nhs.uk/GPES-I/Schema}GPES-E-Q-QR-Message": {
            "@{http://www.w3.org/2001/XMLSchema-instance}schemaLocation": "http://www.ic.nhs.uk/GPES-I/Schema/GPES-E-Q-QR.xsd",
            "@GPES-I-Version": "6.0",
            "@QR-Message-Version": "0.2",
            "{http://www.ic.nhs.uk/GPES-I/Schema}Query-Results": {
                "@QR-UUID": "276C5A44-DCE2-4F9C-B1B2-DDB4374A115C",
                "@Scheduled-Query-Instance-ID": "A00151-202003310000-R",
                "@RTP-UUID": "9E0E33F3-004A-48D3-BD3A-0BEF65BC7591",
                "@Practice-ID": "T00001",
                "@GPET-Q-SQI-Authentication-Signature": "3B023A7E-96AC-4254-BFFA-C162C0CC349B",
                "@GPET-E-SQI-Authentication-Signature": "49104CE1-5AB8-4432-9310-91A98B317B26",
                "@Issue-Date-Time": "2020-03-25T10:04:45",
                "@Query-Name": "CVDPREVENT 2019-20",
                "@Query-Description": "This specification supports Patient Level Extract CVDPREVENT for the 2019-20 requirement. The query extracts data for 3 outputs; 3 patient level extracts.",
                "{http://www.ic.nhs.uk/GPES-I/Schema}Query-Results-Manifest": {
                    "{http://www.ic.nhs.uk/GPES-I/Schema}GPET-E-Executable-Version": "1.0",
                    "{http://www.ic.nhs.uk/GPES-I/Schema}Payload-Identifiers": {
                        "{http://www.ic.nhs.uk/GPES-I/Schema}GP-System-Identifier": "001.001.483.000",
                        "{http://www.ic.nhs.uk/GPES-I/Schema}Terminology-Identifiers": {
                            "{http://www.ic.nhs.uk/GPES-I/Schema}CodeSchemeID": "2.16.840.1.113883.2.1.3.2.4.15",
                            "{http://www.ic.nhs.uk/GPES-I/Schema}Terminology-Name": "SNOMED-CT",
                            "{http://www.ic.nhs.uk/GPES-I/Schema}Terminology-Release": None
                        }
                    },
                    "{http://www.ic.nhs.uk/GPES-I/Schema}RTP-Translation-Statistics": {
                        "{http://www.ic.nhs.uk/GPES-I/Schema}RTP-Translation-Start-Date-Time": "2020-03-25T10:04:45",
                        "{http://www.ic.nhs.uk/GPES-I/Schema}RTP-Translation-End-Date-Time": "2020-03-25T10:04:45"
                    },
                    "{http://www.ic.nhs.uk/GPES-I/Schema}Query-Execution-Statistics": {
                        "{http://www.ic.nhs.uk/GPES-I/Schema}Query-Execution-Start-Date-Time": "2020-03-25T10:04:45",
                        "{http://www.ic.nhs.uk/GPES-I/Schema}Query-Execution-End-Date-Time": "2020-03-25T10:04:45"
                    }
                },
                "{http://www.ic.nhs.uk/GPES-I/Schema}Query-Results-Records": {
                    "{http://www.ic.nhs.uk/GPES-I/Schema}Aggregate-Record": [
                        {
                            "@RID": "1",
                            "@AID": "957080F1-4EB8-4547-A3C6-267E2A53BD1A",
                            "@Description": "CVDPREVENT 2019-20 Audit Period Start Date (Fixed Date 1900-01-01)",
                            "#text": "2020-03-31"
                        },
                        {
                            "@RID": "2",
                            "@AID": "41ED40A4-0AFB-4C65-8E5B-94B31132A37F",
                            "@Description": "CVDPREVENT 2019-20 Audit Period End Date (Fixed Date 2020-03-31)",
                            "#text": "2020-05-31"
                        }
                    ],
                    "{http://www.ic.nhs.uk/GPES-I/Schema}Patient-Level-Record": [
                        {
                            "@RID": "7",
                            "{http://www.ic.nhs.uk/GPES-I/Schema}Patient-Table": {
                                "@QID": "1",
                                "@LID": "1",
                                "@DATE-OF-BIRTH": "1965-05-17",
                                "@NHS-NUMBER": "9979000007",
                                "@POSTCODE": "SR1 3BG",
                                "@PRACTICE": "A34561",
                                "@SEX": "2"
                            },
                            "{http://www.ic.nhs.uk/GPES-I/Schema}Journals-Table": [
                                {
                                    "@QID": "2",
                                    "@LID": "1",
                                    "@DATE": "1990-04-20",
                                    "@CODE": "107691000000105",
                                    "@VALUE1-CONDITION": "",
                                    "@VALUE2-CONDITION": "",
                                    "@VALUE1-PRESCRIPTION": "",
                                    "@VALUE2-PRESCRIPTION": ""
                                },
                                {
                                    "@QID": "2",
                                    "@LID": "2",
                                    "@DATE": "2019-02-15",
                                    "@CODE": "162863004",
                                    "@VALUE1-CONDITION": "27.9",
                                    "@VALUE2-CONDITION": "",
                                    "@VALUE1-PRESCRIPTION": "",
                                    "@VALUE2-PRESCRIPTION": ""
                                },
                                {
                                    "@QID": "2",
                                    "@LID": "3",
                                    "@DATE": "2019-02-15",
                                    "@CODE": "162755006",
                                    "@VALUE1-CONDITION": "156",
                                    "@VALUE2-CONDITION": "",
                                    "@VALUE1-PRESCRIPTION": "",
                                    "@VALUE2-PRESCRIPTION": ""
                                }
                            ]
                        },
                        {
                            "@RID": "8",
                            "{http://www.ic.nhs.uk/GPES-I/Schema}Patient-Table": {
                                "@QID": "1",
                                "@LID": "1",
                                "@DATE-OF-BIRTH": "1944-09-13",
                                "@NHS-NUMBER": "9979000023",
                                "@POSTCODE": "DE1 7BN",
                                "@PRACTICE": "A34561",
                                "@SEX": "2"
                            },
                            "{http://www.ic.nhs.uk/GPES-I/Schema}Journals-Table": [
                                {
                                    "@QID": "2",
                                    "@LID": "1",
                                    "@DATE": "1990-04-20",
                                    "@CODE": "976791000000107",
                                    "@VALUE1-CONDITION": "",
                                    "@VALUE2-CONDITION": "",
                                    "@VALUE1-PRESCRIPTION": "",
                                    "@VALUE2-PRESCRIPTION": ""
                                },
                                {
                                    "@QID": "2",
                                    "@LID": "2",
                                    "@DATE": "2019-08-15",
                                    "@CODE": "162864005",
                                    "@VALUE1-CONDITION": "30",
                                    "@VALUE2-CONDITION": "",
                                    "@VALUE1-PRESCRIPTION": "",
                                    "@VALUE2-PRESCRIPTION": ""
                                }
                            ]
                        }
                    ]
                }
            }
        }
    }


def test_xml_to_dict_large_example_remove_ns():
    xml = """<GPES-E-Q-QR-Message xmlns="http://www.ic.nhs.uk/GPES-I/Schema"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.ic.nhs.uk/GPES-I/Schema/GPES-E-Q-QR.xsd" GPES-I-Version="6.0" QR-Message-Version="0.2">
    <Query-Results QR-UUID="276C5A44-DCE2-4F9C-B1B2-DDB4374A115C" Scheduled-Query-Instance-ID="A00151-202003310000-R" 
    RTP-UUID="9E0E33F3-004A-48D3-BD3A-0BEF65BC7591" Practice-ID="T00001" 
    GPET-Q-SQI-Authentication-Signature="3B023A7E-96AC-4254-BFFA-C162C0CC349B" GPET-E-SQI-Authentication-Signature="49104CE1-5AB8-4432-9310-91A98B317B26" 
    Issue-Date-Time="2020-03-25T10:04:45" 
    Query-Name="CVDPREVENT 2019-20" 
    Query-Description="This specification supports Patient Level Extract CVDPREVENT for the 2019-20 requirement. The query extracts data for 3 outputs; 3 patient level extracts.">
        <Query-Results-Manifest>
            <GPET-E-Executable-Version>1.0</GPET-E-Executable-Version>
            <Payload-Identifiers>
                <GP-System-Identifier>001.001.483.000</GP-System-Identifier>
                <Terminology-Identifiers>
                    <CodeSchemeID>2.16.840.1.113883.2.1.3.2.4.15</CodeSchemeID>
                    <Terminology-Name>SNOMED-CT</Terminology-Name>
                    <Terminology-Release/>
                </Terminology-Identifiers>
            </Payload-Identifiers>
            <RTP-Translation-Statistics>
                <RTP-Translation-Start-Date-Time>2020-03-25T10:04:45</RTP-Translation-Start-Date-Time>
                <RTP-Translation-End-Date-Time>2020-03-25T10:04:45</RTP-Translation-End-Date-Time>
            </RTP-Translation-Statistics>
            <Query-Execution-Statistics>
                <Query-Execution-Start-Date-Time>2020-03-25T10:04:45</Query-Execution-Start-Date-Time>
                <Query-Execution-End-Date-Time>2020-03-25T10:04:45</Query-Execution-End-Date-Time>
            </Query-Execution-Statistics>
        </Query-Results-Manifest>
        <Query-Results-Records>
            <Aggregate-Record RID="1" AID="957080F1-4EB8-4547-A3C6-267E2A53BD1A" Description="CVDPREVENT 2019-20 Audit Period Start Date (Fixed Date 1900-01-01)">2020-03-31</Aggregate-Record>
            <Aggregate-Record RID="2" AID="41ED40A4-0AFB-4C65-8E5B-94B31132A37F" Description="CVDPREVENT 2019-20 Audit Period End Date (Fixed Date 2020-03-31)">2020-05-31</Aggregate-Record>
            <Patient-Level-Record RID="7">
                <Patient-Table QID="1" LID="1" DATE-OF-BIRTH="1965-05-17" NHS-NUMBER="9979000007" POSTCODE="SR1 3BG" PRACTICE="A34561" SEX="2"/>
                <Journals-Table QID="2" LID="1" DATE="1990-04-20" CODE="107691000000105" VALUE1-CONDITION="" VALUE2-CONDITION="" VALUE1-PRESCRIPTION="" VALUE2-PRESCRIPTION=""/>
                <Journals-Table QID="2" LID="2" DATE="2019-02-15" CODE="162863004" VALUE1-CONDITION="27.9" VALUE2-CONDITION="" VALUE1-PRESCRIPTION="" VALUE2-PRESCRIPTION=""/>
                <Journals-Table QID="2" LID="3" DATE="2019-02-15" CODE="162755006" VALUE1-CONDITION="156" VALUE2-CONDITION="" VALUE1-PRESCRIPTION="" VALUE2-PRESCRIPTION=""/>
            </Patient-Level-Record>
            <Patient-Level-Record RID="8">
                <Patient-Table QID="1" LID="1" DATE-OF-BIRTH="1944-09-13" NHS-NUMBER="9979000023" POSTCODE="DE1 7BN" PRACTICE="A34561" SEX="2"/>
                <Journals-Table QID="2" LID="1" DATE="1990-04-20" CODE="976791000000107" VALUE1-CONDITION="" VALUE2-CONDITION="" VALUE1-PRESCRIPTION="" VALUE2-PRESCRIPTION=""/>
                <Journals-Table QID="2" LID="2" DATE="2019-08-15" CODE="162864005" VALUE1-CONDITION="30" VALUE2-CONDITION="" VALUE1-PRESCRIPTION="" VALUE2-PRESCRIPTION=""/>
            </Patient-Level-Record>
        </Query-Results-Records>
    </Query-Results>
</GPES-E-Q-QR-Message>
    """
    d = xml_to_dict(xml, False)
    assert d == {
        "GPES-E-Q-QR-Message": {
            "@schemaLocation": "http://www.ic.nhs.uk/GPES-I/Schema/GPES-E-Q-QR.xsd",
            "@GPES-I-Version": "6.0",
            "@QR-Message-Version": "0.2",
            "Query-Results": {
                "@QR-UUID": "276C5A44-DCE2-4F9C-B1B2-DDB4374A115C",
                "@Scheduled-Query-Instance-ID": "A00151-202003310000-R",
                "@RTP-UUID": "9E0E33F3-004A-48D3-BD3A-0BEF65BC7591",
                "@Practice-ID": "T00001",
                "@GPET-Q-SQI-Authentication-Signature": "3B023A7E-96AC-4254-BFFA-C162C0CC349B",
                "@GPET-E-SQI-Authentication-Signature": "49104CE1-5AB8-4432-9310-91A98B317B26",
                "@Issue-Date-Time": "2020-03-25T10:04:45",
                "@Query-Name": "CVDPREVENT 2019-20",
                "@Query-Description": "This specification supports Patient Level Extract CVDPREVENT for the 2019-20 requirement. The query extracts data for 3 outputs; 3 patient level extracts.",
                "Query-Results-Manifest": {
                    "GPET-E-Executable-Version": "1.0",
                    "Payload-Identifiers": {
                        "GP-System-Identifier": "001.001.483.000",
                        "Terminology-Identifiers": {
                            "CodeSchemeID": "2.16.840.1.113883.2.1.3.2.4.15",
                            "Terminology-Name": "SNOMED-CT",
                            "Terminology-Release": None
                        }
                    },
                    "RTP-Translation-Statistics": {
                        "RTP-Translation-Start-Date-Time": "2020-03-25T10:04:45",
                        "RTP-Translation-End-Date-Time": "2020-03-25T10:04:45"
                    },
                    "Query-Execution-Statistics": {
                        "Query-Execution-Start-Date-Time": "2020-03-25T10:04:45",
                        "Query-Execution-End-Date-Time": "2020-03-25T10:04:45"
                    }
                },
                "Query-Results-Records": {
                    "Aggregate-Record": [
                        {
                            "@RID": "1",
                            "@AID": "957080F1-4EB8-4547-A3C6-267E2A53BD1A",
                            "@Description": "CVDPREVENT 2019-20 Audit Period Start Date (Fixed Date 1900-01-01)",
                            "#text": "2020-03-31"
                        },
                        {
                            "@RID": "2",
                            "@AID": "41ED40A4-0AFB-4C65-8E5B-94B31132A37F",
                            "@Description": "CVDPREVENT 2019-20 Audit Period End Date (Fixed Date 2020-03-31)",
                            "#text": "2020-05-31"
                        }
                    ],
                    "Patient-Level-Record": [
                        {
                            "@RID": "7",
                            "Patient-Table": {
                                "@QID": "1",
                                "@LID": "1",
                                "@DATE-OF-BIRTH": "1965-05-17",
                                "@NHS-NUMBER": "9979000007",
                                "@POSTCODE": "SR1 3BG",
                                "@PRACTICE": "A34561",
                                "@SEX": "2"
                            },
                            "Journals-Table": [
                                {
                                    "@QID": "2",
                                    "@LID": "1",
                                    "@DATE": "1990-04-20",
                                    "@CODE": "107691000000105",
                                    "@VALUE1-CONDITION": "",
                                    "@VALUE2-CONDITION": "",
                                    "@VALUE1-PRESCRIPTION": "",
                                    "@VALUE2-PRESCRIPTION": ""
                                },
                                {
                                    "@QID": "2",
                                    "@LID": "2",
                                    "@DATE": "2019-02-15",
                                    "@CODE": "162863004",
                                    "@VALUE1-CONDITION": "27.9",
                                    "@VALUE2-CONDITION": "",
                                    "@VALUE1-PRESCRIPTION": "",
                                    "@VALUE2-PRESCRIPTION": ""
                                },
                                {
                                    "@QID": "2",
                                    "@LID": "3",
                                    "@DATE": "2019-02-15",
                                    "@CODE": "162755006",
                                    "@VALUE1-CONDITION": "156",
                                    "@VALUE2-CONDITION": "",
                                    "@VALUE1-PRESCRIPTION": "",
                                    "@VALUE2-PRESCRIPTION": ""
                                }
                            ]
                        },
                        {
                            "@RID": "8",
                            "Patient-Table": {
                                "@QID": "1",
                                "@LID": "1",
                                "@DATE-OF-BIRTH": "1944-09-13",
                                "@NHS-NUMBER": "9979000023",
                                "@POSTCODE": "DE1 7BN",
                                "@PRACTICE": "A34561",
                                "@SEX": "2"
                            },
                            "Journals-Table": [
                                {
                                    "@QID": "2",
                                    "@LID": "1",
                                    "@DATE": "1990-04-20",
                                    "@CODE": "976791000000107",
                                    "@VALUE1-CONDITION": "",
                                    "@VALUE2-CONDITION": "",
                                    "@VALUE1-PRESCRIPTION": "",
                                    "@VALUE2-PRESCRIPTION": ""
                                },
                                {
                                    "@QID": "2",
                                    "@LID": "2",
                                    "@DATE": "2019-08-15",
                                    "@CODE": "162864005",
                                    "@VALUE1-CONDITION": "30",
                                    "@VALUE2-CONDITION": "",
                                    "@VALUE1-PRESCRIPTION": "",
                                    "@VALUE2-PRESCRIPTION": ""
                                }
                            ]
                        }
                    ]
                }
            }
        }
    }
