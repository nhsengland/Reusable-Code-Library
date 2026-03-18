from lxml.etree import XML  # nosec - never called with attacker-controlled data
from dsp.model.ods_record_codes import ODSRelType
from dsp.model.ods_record import ODSRecord, ODSRecordPaths


def test_was_nhs_org_when_opened_and_closed_on_the_same_day():
    json = '''{
        "from": 20140401, "org_code": "RAT02",
        "roles": [
            {"to": 20140401, "from": 20140401, "prim": true, "type": "RO198"},
            {"to": 20140401, "from": 20140401, "prim": false, "type": "RO31"}
        ], "rels": [
            {"to": 20140401, "type": "RE6", "role": "RO197", "org_code": "RAT", "from": 20140401}
        ], "to": 20140401, "succs": [], "postcode": "IG11 9LX",
        "name": "B&D ACCESS & ASSESSMENT 2 - BARKING COMMUNITY HOSPITAL"
    }'''
    parsed = ODSRecord.from_json(json)  # type: ODSRecord

    assert parsed is not None

    assert parsed.nhs_organisation_at(20170501) is True
    

def test_non_nhs_org_resolution_multi_role():
    json = '''{
"from" : 20140401,
"roles" : [
  {
     "type" : "RO177",
     "from" : 20140401
  },
  {
     "type" : "RO87",
     "from" : 20140415
  },
  {
     "from" : 20140415,
     "type" : "RO80"
  }
],
"postcode" : "BA1 9AE",
"rels" : [
  {
     "type" : "RE4",
     "role" : "RO98",
     "from" : 20140401,
     "org_code" : "11E"
  }
],
"org_code" : "Y04512",
"succs" : [],
"name" : "BANES DOCTORS URGENT CARE (PAULTON)"
}
'''
    parsed = ODSRecord.from_json(json)  # type: ODSRecord

    assert parsed is not None

    assert parsed.nhs_organisation_at(20140501) is True


def test_evaluate_relationship_effective_end_date():
    json = '''{
"org_code" : "V00887",
"roles" : [],
"postcode" : "MK178PL",
"rels" : [
         {
             "type": "RE5",
             "org_code" : "Q38",
             "role": "RO210",
             "from" : 20060401
         },
         {
             "type" : "RE4",
             "to" : 20130331,
             "org_code" : "5CQ",
             "from" : 20060401
         }
     ],
"from" : 20060401
}
'''
    parsed = ODSRecord.from_json(json)  # type: ODSRecord

    assert parsed is not None

    assert parsed.rel_org_at_of_type(point_in_time=20170101, rel_type=ODSRelType.IS_COMMISSIONED_BY) is None
    assert parsed.rel_org_at_of_type(point_in_time=20170101
                                     , rel_type=ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF) is not None
    assert parsed.rel_org_at_of_type(point_in_time=20170101
                                     , rel_type=ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF
                                     , role_ids='RO210') is not None
    assert parsed.rel_org_at_of_type(point_in_time=20170101
                                     , rel_type=ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF
                                     , role_ids='FAKE') is None


def test_evaluate_relationship_effective_end_date2():
    json = '''{
"org_code" : "V00887",
"roles" : [],
"postcode" : "MK178PL",
"rels" : [
         {
             "type": "RE5",
             "org_code" : "Q38",
             "role": "RO210",
             "from" : 20060401
         },
         {
             "type" : "RE4",
             "to" : 20130331,
             "org_code" : "5CQ",
             "from" : 20060401
         }
     ],
"from" : 20060401
}
'''
    parsed = ODSRecord.from_json(json)  # type: ODSRecord

    assert parsed is not None

    assert parsed.rel_org_at_of_type(point_in_time=20170101, rel_type=ODSRelType.IS_COMMISSIONED_BY) is None
    assert parsed.rel_org_at_of_type(point_in_time=20170101
                                     , rel_type=ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF) is not None
    assert parsed.rel_org_at_of_type(point_in_time=20170101
                                     , rel_type=ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF, role_ids='RO210') is not None
    assert parsed.rel_org_at_of_type(point_in_time=20170101
                                     , rel_type=ODSRelType.IS_LOCATED_IN_THE_GEOGRAPHY_OF, role_ids='FAKE') is None


def test_parse_etree_simple():
    xml = """<Organisation orgRecordClass="RC2">
  <Name>GHATTAORA AS</Name>
  <Date>
    <Type value="Operational"/>
    <Start value="2004-04-01"/>
  </Date>
  <OrgId assigningAuthorityName="HSCIC" extension="C84714001" root="2.16.840.1.113883.2.1.3.2.4.18.48"/>
  <Status value="Active"/>
  <LastChangeDate value="2015-04-15"/>
  <GeoLoc>
    <Location>
      <AddrLn1>SUNRISE MEDICAL PRACTICE</AddrLn1>
      <AddrLn2>STUDENT CENTRE, NOTTINGHAM TRENT UN</AddrLn2>
      <AddrLn3>CLIFTON CAMPUS, CLIFTON LANE</AddrLn3>
      <Town>NOTTINGHAM</Town>
      <County>NOTTINGHAMSHIRE</County>
      <PostCode>NG11 8NS</PostCode>
      <Country>ENGLAND</Country>
    </Location>
  </GeoLoc>
  <Contacts>
    <Contact type="tel" value="01158483100"/>
  </Contacts>
  <Roles>
    <Role id="RO96" primaryRole="true" uniqueRoleId="1177">
      <Date>
        <Type value="Operational"/>
        <Start value="2004-04-01"/>
      </Date>
      <Status value="Active"/>
    </Role>
  </Roles>
  <Rels>
    <Rel id="RE6" uniqueRelId="18082">
      <Date>
        <Type value="Operational"/>
        <Start value="2004-04-01"/>
      </Date>
      <Status value="Active"/>
      <Target>
        <OrgId assigningAuthorityName="HSCIC" extension="C84714" root="2.16.840.1.113883.2.1.3.2.4.18.48"/>
        <PrimaryRoleId id="RO177" uniqueRoleId="59739"/>
      </Target>
    </Rel>
  </Rels>
</Organisation>"""

    tree = XML(xml)

    parsed = ODSRecord.from_etree(tree)

    assert parsed is not None

    assert parsed.org_code() is not None
    assert parsed.org_code() == 'C84714001'

    assert parsed.postcode() is not None
    assert parsed.postcode() == 'NG11 8NS'

    roles = parsed.roles()

    assert len(roles) == 1

    assert roles[0][ODSRecordPaths.EFFECTIVE_FROM] == 20040401
    assert roles[0].get(ODSRecordPaths.EFFECTIVE_TO, None) is None
    assert roles[0][ODSRecordPaths.TYPE] == 'RO96'

    rels = parsed.rels()

    assert len(rels) == 1

    assert rels[0][ODSRecordPaths.EFFECTIVE_FROM] == 20040401
    assert roles[0].get(ODSRecordPaths.EFFECTIVE_TO, None) is None
    assert rels[0][ODSRecordPaths.TYPE] == 'RE6'
    assert rels[0][ODSRecordPaths.ROLE] == 'RO177'
    assert rels[0][ODSRecordPaths.ORG_CODE] == 'C84714'


def test_parse_etree_simple_inactive():

    xml = """<Organisation orgRecordClass="RC2">
  <Name>DERBYSHIRE LANE SURGERY</Name>
  <Date>
    <Type value="Operational"/>
    <Start value="2004-04-01"/>
    <End value="2014-06-30"/>
  </Date>
  <OrgId assigningAuthorityName="HSCIC" extension="C88032001" root="2.16.840.1.113883.2.1.3.2.4.18.48"/>
  <Status value="Inactive"/>
  <LastChangeDate value="2014-07-30"/>
  <GeoLoc>
    <Location>
      <AddrLn1>213 DERBYSHIRE LANE</AddrLn1>
      <Town>SHEFFIELD</Town>
      <County>SOUTH YORKSHIRE</County>
      <PostCode>S8 8SA</PostCode>
      <Country>ENGLAND</Country>
    </Location>
  </GeoLoc>
  <Contacts>
    <Contact type="tel" value="01142550972"/>
  </Contacts>
  <Roles>
    <Role id="RO96" primaryRole="true" uniqueRoleId="1203">
      <Date>
        <Type value="Operational"/>
        <Start value="2004-04-01"/>
        <End value="2014-06-30"/>
      </Date>
      <Status value="Inactive"/>
    </Role>
  </Roles>
  <Rels>
    <Rel id="RE6" uniqueRelId="24415">
      <Date>
        <Type value="Operational"/>
        <Start value="2004-04-01"/>
        <End value="2014-06-30"/>
      </Date>
      <Status value="Inactive"/>
      <Target>
        <OrgId assigningAuthorityName="HSCIC" extension="C88032" root="2.16.840.1.113883.2.1.3.2.4.18.48"/>
        <PrimaryRoleId id="RO177" uniqueRoleId="92556"/>
      </Target>
    </Rel>
  </Rels>
</Organisation>"""

    tree = XML(xml)

    parsed = ODSRecord.from_etree(tree)

    assert parsed is not None

    rels = parsed.rels()

    assert len(rels) == 1

    assert parsed.active_at(20140630) is True
    assert parsed.active_at(20140701) is False


def test_parse_successors():

    xml = """<Organisation orgRecordClass="RC1">
  <Name>INDIGO 4</Name>
  <Date>
    <Type value="Operational"/>
    <Start value="2006-04-01"/>
    <End value="2015-06-30"/>
  </Date>
  <OrgId assigningAuthorityName="HSCIC" extension="YGM10" root="2.16.840.1.113883.2.1.3.2.4.18.48"/>
  <Status value="Inactive"/>
  <LastChangeDate value="2015-07-13"/>
  <GeoLoc>
    <Location>
      <AddrLn1>AIZLEWOODS MILL</AddrLn1>
      <AddrLn2>NURSERY STREET</AddrLn2>
      <Town>SHEFFIELD</Town>
      <County>SOUTH YORKSHIRE</County>
      <PostCode>S3 8GG</PostCode>
      <Country>ENGLAND</Country>
    </Location>
  </GeoLoc>
  <Roles>
    <Role id="RO92" primaryRole="true" uniqueRoleId="97897">
      <Date>
        <Type value="Operational"/>
        <Start value="2006-04-01"/>
        <End value="2015-07-01"/>
      </Date>
      <Status value="Inactive"/>
    </Role>
  </Roles>
  <Succs>
    <Succ uniqueSuccId="37311">
      <Date>
        <Type value="Legal"/>
        <Start value="2015-06-30"/>
      </Date>
      <Type>Successor</Type>
      <Target>
        <OrgId assigningAuthorityName="HSCIC" extension="YGM41" root="2.16.840.1.113883.2.1.3.2.4.18.48"/>
        <PrimaryRoleId id="RO92" uniqueRoleId="128462"/>
      </Target>
    </Succ>
    <Succ forwardSuccession="true" uniqueSuccId="36030">
      <Date>
        <Type value="Legal"/>
        <Start value="2012-02-01"/>
      </Date>
      <Type>Predecessor</Type>
      <Target>
        <OrgId assigningAuthorityName="HSCIC" extension="8GW80" root="2.16.840.1.113883.2.1.3.2.4.18.48"/>
        <PrimaryRoleId id="RO157" uniqueRoleId="91723"/>
      </Target>
    </Succ>
  </Succs>
</Organisation>"""

    tree = XML(xml)

    parsed = ODSRecord.from_etree(tree)

    assert parsed is not None

    assert parsed.org_code() == 'YGM10'

    succs = parsed.successions()

    assert len(succs) == 1
    assert succs[0][ODSRecordPaths.EFFECTIVE_FROM] == 20150630
    assert succs[0][ODSRecordPaths.ORG_CODE] == 'YGM41'
    # successors should be honoured after an org is closed
    assert parsed.superceded_by_at(20150701) == 'YGM41'
    assert parsed.superceded_by_at(20150630) is None
    assert parsed.superceded_by_at(20150629) is None
    assert parsed.active_at(20150630) is True
    assert parsed.active_at(20150701) is False
    assert parsed.roles()[0][ODSRecordPaths.PRIMARY] is True


def test_parse_with_legal_dates():

    xml = """<Organisation orgRecordClass="RC2">
  <Name>THE FLOWERS HEALTH CENTRE</Name>
  <Date>
    <Type value="Legal"/>
    <Start value="1980-01-01"/>
    <End value="2013-03-31"/>
  </Date>
  <Date>
    <Type value="Operational"/>
    <Start value="2012-04-02"/>
  </Date>
  <OrgId assigningAuthorityName="HSCIC" extension="5N424" root="2.16.840.1.113883.2.1.3.2.4.18.48"/>
  <Status value="Active"/>
  <LastChangeDate value="2013-03-27"/>
  <GeoLoc>
    <Location>
      <AddrLn1>87 WINCOBANK AVENUE</AddrLn1>
      <Town>SHEFFIELD</Town>
      <County>SOUTH YORKSHIRE</County>
      <PostCode>S5 6AZ</PostCode>
      <Country>ENGLAND</Country>
    </Location>
  </GeoLoc>
  <Roles>
    <Role id="RO180" primaryRole="true" uniqueRoleId="162151">
      <Date>
        <Type value="Legal"/>
        <Start value="2012-04-01"/>
        <End value="2013-03-31"/>
      </Date>
      <Date>
        <Type value="Operational"/>
        <Start value="2012-04-01"/>
      </Date>
      <Status value="Active"/>
    </Role>
     <Role id="RO180">
      <Date>
        <Type value="Operational"/>
        <Start value="2010-04-01"/>
        <End value="2011-03-31"/>
      </Date>
      <Status value="Active"/>
    </Role>
    <Role id="RO180">
      <Date>
        <Type value="Operational"/>
        <Start value="2012-04-01"/>
        <End value="2013-03-31"/>
      </Date>
      <Status value="Active"/>
    </Role>
  </Roles>
  <Rels>
    <Rel id="RE6" uniqueRelId="184316">
      <Date>
        <Type value="Legal"/>
        <Start value="2012-04-01"/>
        <End value="2013-03-31"/>
      </Date>
      <Date>
        <Type value="Operational"/>
        <Start value="2012-04-01"/>
      </Date>
      <Status value="Active"/>
      <Target>
        <OrgId assigningAuthorityName="HSCIC" extension="5N4" root="2.16.840.1.113883.2.1.3.2.4.18.48"/>
        <PrimaryRoleId id="RO179" uniqueRoleId="137166"/>
      </Target>
    </Rel>
  </Rels>
</Organisation>"""

    tree = XML(xml)

    parsed = ODSRecord.from_etree(tree)

    assert parsed is not None

    assert parsed.org_code() == '5N424'
    assert parsed.record_dict[ODSRecordPaths.EFFECTIVE_FROM] == 19800101
    assert parsed.record_dict[ODSRecordPaths.EFFECTIVE_TO] == 20130331
    assert parsed.nhs_organisation_at(20100331) is False
    assert parsed.nhs_organisation_at(20100401) is True
    assert parsed.nhs_organisation_at(20110331) is True
    assert parsed.nhs_organisation_at(20110401) is False
    assert parsed.nhs_organisation_at(20120331) is False
    assert parsed.nhs_organisation_at(20120401) is True
    assert parsed.nhs_organisation_at(20130401) is True
    assert parsed.active_at(20130330) is True
    assert parsed.active_at(20130331) is True
    assert parsed.active_at(20130401) is False
    assert parsed.was_closed_at(19700101) is False
    assert parsed.was_closed_at(20130401) is True
    # org rels are NOT preserved after the record is closed

    assert parsed.rel_org_at_of_type(point_in_time=20120402, rel_type='RE6', role_ids=['RO179', 'RO179']) == '5N4'

    assert parsed.rel_org_at_of_type(point_in_time=20120402, rel_type='RE6', role_ids='RO179') == '5N4'

    assert parsed.rel_org_at_of_type(point_in_time=20120402, rel_type='RE6') == '5N4'

    assert parsed.rel_org_at_of_type(point_in_time=99991231, rel_type='RE6') is None


def test_has_role_at():
    xml = """<Organisation orgRecordClass="RC1">
<Name>TRAFALGAR MEDICAL GROUP PRACTICE</Name>
<Date>
  <Type value="Operational" />
  <Start value="1974-04-01" />
</Date>
<OrgId root="2.16.840.1.113883.2.1.3.2.4.18.48" assigningAuthorityName="HSCIC" extension="J82028" />
<Status value="Active" />
<LastChangeDate value="2016-06-24" />
<GeoLoc>
  <Location>
    <AddrLn1>THE SURGERY</AddrLn1>
    <AddrLn2>25 OSBORNE ROAD</AddrLn2>
    <AddrLn3>PORTSMOUTH</AddrLn3>
    <Town>SOUTHSEA</Town>
    <County>HAMPSHIRE</County>
    <PostCode>PO5 3ND</PostCode>
    <Country>ENGLAND</Country>
  </Location>
</GeoLoc>
<Contacts>
  <Contact type="tel" value="02392 821371" />
</Contacts>
<Roles>
  <Role id="RO177" uniqueRoleId="17745" primaryRole="true">
    <Date>
      <Type value="Operational" />
      <Start value="1974-04-01" />
    </Date>
    <Status value="Active" />
  </Role>
  <Role id="RO76" uniqueRoleId="185634">
    <Date>
      <Type value="Operational" />
      <Start value="2014-04-15" />
    </Date>
    <Status value="Active" />
  </Role>
</Roles>
<Rels>
  <Rel id="RE4" uniqueRelId="261045">
    <Date>
      <Type value="Operational" />
      <Start value="2001-04-01" />
      <End value="2013-03-31" />
    </Date>
    <Status value="Inactive" />
    <Target>
      <OrgId root="2.16.840.1.113883.2.1.3.2.4.18.48" assigningAuthorityName="HSCIC" extension="5FE" />
      <PrimaryRoleId id="RO179" uniqueRoleId="77668" />
    </Target>
  </Rel>
  <Rel id="RE4" uniqueRelId="261046">
    <Date>
      <Type value="Operational" />
      <Start value="1999-04-01" />
      <End value="2001-03-31" />
    </Date>
    <Status value="Inactive" />
    <Target>
      <OrgId root="2.16.840.1.113883.2.1.3.2.4.18.48" assigningAuthorityName="HSCIC" extension="4RK71" />
      <PrimaryRoleId id="RO171" uniqueRoleId="124631" />
    </Target>
  </Rel>
  <Rel id="RE4" uniqueRelId="261047">
    <Date>
      <Type value="Operational" />
      <Start value="2013-04-01" />
    </Date>
    <Status value="Active" />
    <Target>
      <OrgId root="2.16.840.1.113883.2.1.3.2.4.18.48" assigningAuthorityName="HSCIC" extension="10R" />
      <PrimaryRoleId id="RO98" uniqueRoleId="161509" />
    </Target>
  </Rel>
</Rels>
</Organisation>
    """
    provider_role_id_list = ['RO76']
    tree = XML(xml)

    parsed = ODSRecord.from_etree(tree)

    print(parsed.roles())
    assert parsed.has_role_at(20140416, *provider_role_id_list) is True
    assert parsed.has_role(*provider_role_id_list) is True
    provider_role_id_list = ['RO177']
    tree = XML(xml)

    parsed = ODSRecord.from_etree(tree)

    print(parsed.roles())
    assert parsed.has_role_at(20120430, *provider_role_id_list) is True
    assert parsed.has_role(*provider_role_id_list) is True
