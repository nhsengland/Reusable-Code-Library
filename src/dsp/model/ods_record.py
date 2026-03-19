import sys
from collections.abc import Iterable
from typing import List, Union, Callable, Dict

import msgpack
from lxml.etree import XML  # nosec - ODS data is trusted

from dsp.model.base_model import BaseModel
from dsp.model.ods_record_codes import ODSOrgRole

_nhs_org_roles = [
    ODSOrgRole.CARE_TRUST, ODSOrgRole.CARE_TRUST_SITE, ODSOrgRole.DIRECTLY_MANAGED_UNIT_DMU,
    ODSOrgRole.DMU_SITE, ODSOrgRole.STRATEGIC_HEALTH_AUTHORITY_SITE,
    ODSOrgRole.WELSH_LOCAL_HEALTH_BOARD_SITE, ODSOrgRole.PRIMARY_CARE_TRUST,
    ODSOrgRole.PRIMARY_CARE_TRUST_SITE, ODSOrgRole.SPECIAL_HEALTH_AUTHORITY_SITE,
    ODSOrgRole.NHS_TRUST, ODSOrgRole.NHS_TRUST_SITE, ODSOrgRole.REGION_GEOGRAPHY_SITE,
    ODSOrgRole.COMMISSIONING_SUPPORT_UNIT, ODSOrgRole.COMMISSIONING_SUPPORT_UNIT_SITE,
    ODSOrgRole.DSCRO_SITE, ODSOrgRole.NHS_TRUST_DERIVED, ODSOrgRole.FOUNDATION_TRUST,
    ODSOrgRole.WALK_IN_CENTRE, ODSOrgRole.CLINICAL_COMMISSIONING_GROUP,
    ODSOrgRole.CLINICAL_COMMISSIONING_GROUP_SITE, ODSOrgRole.CARE_TRUST,
    ODSOrgRole.CARE_TRUST_SITE, ODSOrgRole.DIRECTLY_MANAGED_UNIT_DMU, ODSOrgRole.DMU_SITE,
    ODSOrgRole.NHS_TRUST, ODSOrgRole.NHS_TRUST_SITE, ODSOrgRole.NHS_TRUST_DERIVED,
    ODSOrgRole.FOUNDATION_TRUST
]


class _ODSRecordPaths:
    ORG_CODE = 'org_code'
    NAME = 'name'
    POSTCODE = 'postcode'
    HISTORICAL_POSTCODES = 'historical_postcodes'
    EFFECTIVE_FROM = 'from'
    EFFECTIVE_TO = 'to'
    RELS = 'rels'
    TYPE = 'type'
    STATUS = 'status'
    ROLE = 'role'
    ROLES = 'roles'
    SUCCESSIONS = 'succs'
    PRIMARY = 'prim'
    FROM_ORG_TYPE = 'from_org_type'
    TO_ORG_TYPE = 'to_org_type'


ODSRecordPaths = _ODSRecordPaths()
del _ODSRecordPaths


def _nexpath(node, path, default=None):
    return next(iter(node.xpath(path)), default)


class ODSRecord(BaseModel):

    def event_id(self):
        pass

    def pack(self):
        return msgpack.packb(self.record_dict)

    @classmethod
    def from_dict(cls, record_dict):
        return cls.build_record(record_dict)

    @classmethod
    def from_xml(cls, xml):
        """parse an organisation element tree

        Args:
            xml (str):
        Returns:
            ODSRecord: the parsed ODSRecord
        """
        etree = XML(xml)
        return cls.from_etree(etree)

    @classmethod
    def from_etree(cls, etree):
        """parse an organisation element tree

        Args:
            etree (ElementTree):
        Returns:
            ODSRecord: the parsed ODSRecord
        """
        postcode = etree.findtext('GeoLoc/Location/PostCode')
        name = etree.findtext('Name')
        org_code = _nexpath(etree, 'OrgId/@extension')
        # reverse the role and rel lists so most current information is first
        return ODSRecord(cls._temporal_dict(etree, {
            ODSRecordPaths.POSTCODE: postcode,
            ODSRecordPaths.NAME: name,
            ODSRecordPaths.ORG_CODE: org_code,
            ODSRecordPaths.RELS: list(reversed(list(cls._parse_rels(etree)))),
            ODSRecordPaths.ROLES: list(reversed(list(cls._parse_roles(etree)))),
            ODSRecordPaths.SUCCESSIONS: list(reversed(list(cls._parse_successions(etree)))),
        }))

    @classmethod
    def _parse_roles(cls, etree):

        for role in etree.xpath('Roles/Role'):
            role_type = role.get('id')
            primary = role.get('primaryRole') is not None

            yield cls._temporal_dict(role, {ODSRecordPaths.TYPE: role_type, ODSRecordPaths.PRIMARY: primary})

    @classmethod
    def _parse_successions(cls, etree):

        for succession in etree.xpath('Succs/Succ'):

            succ_type = succession.findtext('./Type')
            if succ_type != 'Successor':
                continue

            successor_org = _nexpath(succession, 'Target/OrgId/@extension')

            yield cls._temporal_dict(succession, {ODSRecordPaths.ORG_CODE: successor_org})

    @classmethod
    def _parse_rels(cls, etree):

        for rel in etree.xpath('Rels/Rel'):
            rel_type = rel.get('id')
            org_id = _nexpath(rel, 'Target/OrgId/@extension')
            role_id = _nexpath(rel, 'Target/PrimaryRoleId/@id')

            yield cls._temporal_dict(rel, {
                ODSRecordPaths.TYPE: rel_type, ODSRecordPaths.ORG_CODE: org_id, ODSRecordPaths.ROLE: role_id
            })

    @staticmethod
    def _temporal_dict(elm, other_args):

        temporal_dict = {}
        elm_dates = elm.findall('Date')
        effective_from = None
        effective_to = None

        for rel_date in elm_dates:
            date_type = _nexpath(rel_date, 'Type/@value')
            if effective_from is None or date_type == 'Legal':
                effective_from = int(_nexpath(rel_date, 'Start/@value').replace('-', ''))

            to = _nexpath(rel_date, 'End/@value')
            if to is not None and (effective_to is None or date_type == 'Legal'):
                effective_to = int(to.replace('-', ''))

        temporal_dict[ODSRecordPaths.EFFECTIVE_FROM] = effective_from
        if effective_to:
            temporal_dict[ODSRecordPaths.EFFECTIVE_TO] = effective_to

        for key, val in other_args.items():
            temporal_dict[key] = val

        return temporal_dict

    @classmethod
    def build_record(cls, record_dict) -> 'ODSRecord':
        return ODSRecord(record_dict)

    def name(self):
        return self.record_dict[ODSRecordPaths.NAME]

    def org_code(self):
        return self.record_dict[ODSRecordPaths.ORG_CODE]

    def generated_identifier(self):
        return self.org_code()

    def postcode(self):
        return self.record_dict.get(ODSRecordPaths.POSTCODE, None)

    def historical_postcodes(self):
        return self.record_dict.get(ODSRecordPaths.HISTORICAL_POSTCODES, {})

    def roles(self):
        return self.record_dict.get(ODSRecordPaths.ROLES, [])

    def rels(self):
        return self.record_dict.get(ODSRecordPaths.RELS, [])

    def successions(self):
        return self.record_dict.get(ODSRecordPaths.SUCCESSIONS, [])

    @staticmethod
    def _effective_at(temporal_dict, point_in_time):
        """

        Args:
            temporal_dict (dict): temporal item dict
            point_in_time (int): int representation e.g. 20160924005959 of point in time to evaluate system dates

        Returns:
            bool: True if the point in time date was within the range.

        """
        effective_from = temporal_dict.get(ODSRecordPaths.EFFECTIVE_FROM, None)
        if effective_from is None:
            raise ValueError('ODS Temporal field should have an effective from date')

        return effective_from <= point_in_time <= temporal_dict.get(ODSRecordPaths.EFFECTIVE_TO, sys.maxsize)

    def active_at(self, point_in_time):
        """ Test if the organisation was effective / existed at the point in time
        Args:
            point_in_time (int/str/date/datetime): point in time to evaluate

        Returns:
            bool: true if the organisation was active
        """
        return self._effective_at(self.record_dict, self.get_point_in_time(point_in_time))

    def was_closed_at(self, point_in_time):

        point_in_time = self.get_point_in_time(point_in_time)

        temporal_dict = self.record_dict
        effective_from = temporal_dict.get(ODSRecordPaths.EFFECTIVE_FROM, None)
        if effective_from is None:
            raise ValueError('ODS Temporal field should have an effective from date')

        return effective_from <= temporal_dict.get(ODSRecordPaths.EFFECTIVE_TO, sys.maxsize) < point_in_time

    def rels_at_of_type(self, point_in_time: int, rel_type: str, predicate: Callable[[Dict, ], bool],
                        from_org_type: str = None, role_ids: Union[List[str], None] = None) -> Iterable:
        """returns the related OrgId / org_code of that was effective at the given point in time and is of rel_type
            if rel_type is None then the first matching rel will be returned

        Args:
            point_in_time (int/str/date/datetime): point in time to evaluate the rels
            rel_type (str): rel type filter
            predicate (Callable[dict, bool]): predicate to filter records
            from_org_type (str): from_org_type_filter
            role_ids (list[str]) list of allowed primary role ids for further
        Returns:
            Iterable[dict]: parent org code

        """

        effective_at = self.get_point_in_time(point_in_time)

        # don't resolve relationships for closed organisations
        if self.was_closed_at(effective_at):
            return

        for rel_dict in self.rels():

            if rel_type != rel_dict.get(ODSRecordPaths.TYPE):
                continue

            if from_org_type and from_org_type != rel_dict.get(ODSRecordPaths.FROM_ORG_TYPE):
                continue

            if role_ids and rel_dict[ODSRecordPaths.ROLE] not in role_ids:
                continue

            if not predicate(rel_dict):
                continue

            yield rel_dict  # rel_dict[ODSRecordPaths.ORG_CODE]

    def rel_org_at_of_type(self, point_in_time: int, rel_type: str, from_org_type: str = None,
                           role_ids: Union[List[str], None] = None):
        """returns the related OrgId / org_code of that was effective at the given point in time and is of rel_type


        Args:
            point_in_time (int/str/date/datetime): point in time to evaluate the rels
            rel_type (str): rel type
            from_org_type (str): optional from org type filter
            role_ids (list[str]) list of allowed primary role ids for further
        Returns:
            str: parent org code

        """

        effective_at = self.get_point_in_time(point_in_time)

        org_codes = set(d.get(ODSRecordPaths.ORG_CODE) for d in self.rels_at_of_type(
            point_in_time=point_in_time,
            rel_type=rel_type,
            predicate=lambda rel_dict: self._effective_at(rel_dict, effective_at),
            from_org_type=from_org_type,
            role_ids=role_ids
        ))

        if len(org_codes) > 1:
            raise ValueError(
                'org {} found multiple differnt rels of type {} at {} (try and restrict by role or from_org_type'.format(
                    self.org_code(), rel_type, point_in_time)
            )

        return next(iter(org_codes), None)

    def latest_rel_org_at_of_type(self, point_in_time: int, rel_type: str, from_org_type: str = None,
                                  role_ids: Union[List[str], None] = None):
        """returns the related OrgId / org_code of that was effective at the given point in time and is of rel_type
            or the most recently effective one if there is no current effective rel

        Args:
            point_in_time (int/str/date/datetime): point in time to evaluate the rels
            rel_type (str): rel type
            from_org_type (str): optional from org type filter
            role_ids (list[str]) list of allowed primary role ids for further
        Returns:
            str: parent org code

        """

        effective_at = self.get_point_in_time(point_in_time)

        rels_at_of_type = list(self.rels_at_of_type(
            point_in_time=point_in_time,
            rel_type=rel_type,
            predicate=lambda rd: rd[ODSRecordPaths.EFFECTIVE_FROM] <= effective_at,
            from_org_type=from_org_type,
            role_ids=role_ids
        ))

        current_orgs = set()
        most_recent_closed_date = 0  # set to 0 instead of None to avoid type error
        most_recent_closed_org = None

        for rel_dict in rels_at_of_type:

            if self._effective_at(rel_dict, effective_at):
                current_orgs.add(rel_dict.get(ODSRecordPaths.ORG_CODE))
                continue

            to_date = rel_dict.get(ODSRecordPaths.EFFECTIVE_TO, sys.maxsize)
            if most_recent_closed_date > to_date:
                continue

            most_recent_closed_date = to_date
            most_recent_closed_org = rel_dict[ODSRecordPaths.ORG_CODE]

        if len(current_orgs) > 1:
            raise ValueError(
                'org {} found multiple rels of type {} at {} (try and restrict by role or from_org_type'.format(
                    self.org_code(), rel_type, point_in_time))

        if current_orgs:
            return current_orgs.pop()

        return most_recent_closed_org

    def superceded_by_at(self, point_in_time, from_org_type: str = None):
        """if the ods record is superceeded then return the superceeding org at the point in time

        Args:
            point_in_time (int/str/date/datetime): point in time to evaluate the rels
            from_org_type (str): org type from filter
        Returns:
            str: parent org code
        """

        effective_at = self.get_point_in_time(point_in_time)
        was_closed = self.was_closed_at(effective_at)

        if not was_closed:
            return None

        possible_successors = [
            successor for successor in self.successions() if self._effective_at(successor, effective_at) and (
                    from_org_type is None or successor.get(ODSRecordPaths.FROM_ORG_TYPE) == from_org_type
            )
        ]

        return possible_successors[0][ODSRecordPaths.ORG_CODE] if len(possible_successors) == 1 else None

    def nhs_organisation_at(self, point_in_time):
        """check if the org was an NHS org at the given point in time

        Args:
            point_in_time (int/str/date/datetime): point in time to evaluate the roles

        Returns:
            bool: False if the org has the non nhs role at the point in time
        """

        effective_at = self.get_point_in_time(point_in_time)
        was_closed = self.was_closed_at(effective_at)

        for role in self.roles():
            if role[ODSRecordPaths.TYPE] not in _nhs_org_roles:
                continue

            # honour nhs org for perpetuity
            if was_closed and self.record_dict[ODSRecordPaths.EFFECTIVE_TO] \
                    <= role.get(ODSRecordPaths.EFFECTIVE_TO, sys.maxsize):
                return True

            if self._effective_at(role, effective_at):
                return True

        return False

    def has_role(self, *role_types):
        """
        Checks if org has the role
        Args:
            *role_types: role types to check

        Returns:
            True if org has one of the role types
        """
        for role in self.roles():
            if role['type'] in role_types:
                return True

        return False

    def has_role_at(self, point_in_time, *role_types):
        """check which role the org had at the given point in time

        Args:
            point_in_time (int/str/date/datetime): point in time to evaluate
            role_types (list/str): list of allowed role types

        Returns:
            bool: True if the org has an effective role of this in the requested types
        """
        effective_at = self.get_point_in_time(point_in_time)

        for role in self.roles():
            if role['type'] not in role_types:
                continue

            if self._effective_at(role, effective_at):
                return True

        return False
