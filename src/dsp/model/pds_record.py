import json
import sys
from datetime import datetime, date
from typing import List, MutableMapping, Optional, Mapping, Union

from dsp.model.base_model import BaseModel
from dsp.shared.common import strtobool

DEFAULT_MAX_END_DATE = 99991231


class PDSDataImportException(Exception):
    pass


class _PDSRecordPaths:
    NHS_NUMBER = 'nhs_number'
    ACTIVITY_DATE = 'date'
    SERIAL_CHANGE_NUMBER = 'scn'
    EFFECTIVE_FROM = 'from'
    EFFECTIVE_TO = 'to'
    EMAIL_ADDRESS = 'emailAddress'
    MOBILE_PHONE = 'mobilePhone'
    TELEPHONE = 'telephone'
    MPS_ID_DETAILS = 'mpsid_details'
    MPS_ID = 'mpsID'
    LOCAL_PATIENT_ID = 'localPatientID'
    NAME_HISTORY = 'name_history'
    NAME = 'name'
    VAL = 'val'
    ADDRESS_HISTORY = 'address_history'
    ADDR = 'addr'
    LINE = 'line'
    POSTALCODE = 'postalCode'
    POSTCODE = 'postCode'
    GP_CODES = 'gp_codes'
    CONFIDENTIALITY = 'confidentiality'
    REPLACED_BY = 'replaced_by'
    DOB = 'dob'
    GENDER = 'gender'
    GENDER_HISTORY = 'gender_history'
    DOD = 'dod'
    DEATH_STATUS = 'death_status'
    SENSITIVE = 'sensitive'
    GIVEN_NAME = 'givenName'
    OTHER_GIVEN_NAMES = 'other_given_names'
    FAMILY_NAME = 'familyName'
    MIGRANT_DATA = 'migrant_data'
    LINES = 'lines'
    GIVEN_NAMES = 'givenNames'

    # migrant data record paths
    VISA_FROM = 'visa_from'
    VISA_TO = 'visa_to'
    VISA_STATUS = 'visa_status'
    HOME_OFFICE_REF_NO = 'home_office_ref_no'
    BRP_NO = 'brp_no'
    NATIONALITY = 'nationality'


PDSRecordPaths = _PDSRecordPaths()
del _PDSRecordPaths


class PDSRecord(BaseModel):

    def __init__(self, record_dict: Mapping):
        """Initialise record

        Args:
            record_dict(Mapping): dictionary representing the PDS record's data
        Returns:
            PDSRecord: Initialised record
        """
        super(PDSRecord, self).__init__(record_dict)

    @classmethod
    def build_record(cls, record_dict: Mapping):
        return PDSRecord(record_dict)

    @classmethod
    def from_mesh_line(cls, activity_date: str, record_serial_change_number: int, nhs_number: str, record: str,
                       mandatory_field_check=True) -> 'PDSRecord':
        """Parse mesh line to PDS Record

        Args:
            activity_date: the date at which this version was changed
            record_serial_change_number: the incremting change number for this version of the record
            nhs_number: the nhs number for whom this record exists
            record: the string version of the dict representing the pds record
            mandatory_field_check (bool): Raise exception if mandatory fields are missing

        Returns:
            parsed, validated and cleansed record
        """
        record_dict = json.loads(record)

        if not nhs_number:
            raise PDSDataImportException('key: PDS Record missing NHS Number')

        if not record_serial_change_number:
            raise PDSDataImportException('key: PDS Record missing SerialChangeNumber')

        record_dict[PDSRecordPaths.ACTIVITY_DATE] = activity_date
        record_dict[PDSRecordPaths.SERIAL_CHANGE_NUMBER] = int(record_serial_change_number)
        record_dict[PDSRecordPaths.NHS_NUMBER] = nhs_number

        def vali_bool(bool_thing) -> Optional[bool]:

            if bool_thing is None:
                return None

            return strtobool(str(bool_thing))

        def vali_date(date_thing) -> Optional[int]:

            date_thing = str(date_thing)

            if not date_thing.isdigit() or len(date_thing) < 4:
                return None

            # we have seen 5 digit dates representing YYYYM in the file  e.g. 20151  ... try as 20150101
            if len(date_thing) == 5:
                # app_logger.warn(lambda: dict(msg='attempting to transform 5 digit date ', item=date_thing))
                date_thing = date_thing[:4] + '0' + date_thing[4]

            # we have seen 6 digit dates representing YYYYMM in the file  e.g. 201512  ... try as 20151201
            if len(date_thing) < 8:
                # app_logger.warn(lambda: dict(msg='attempting to transform short date ', item=date_thing))
                date_thing = date_thing[:6] + '0101'

            date_thing = int(date_thing[:8])

            # realistic range
            if date_thing < 18000101 or date_thing > 30001231:
                raise PDSDataImportException('key: {} - PDS date item unrealistic "{}"'
                                             .format(nhs_number, date_thing))

            return date_thing

        def vali_dict(temporal_dict, member, validate, raise_if_missing=False):

            if not temporal_dict.get(PDSRecordPaths.SERIAL_CHANGE_NUMBER, None):
                raise PDSDataImportException('key: {} - PDS Temporal item missing scn'
                                             .format(nhs_number, member))

            if member not in temporal_dict:
                if raise_if_missing:
                    raise PDSDataImportException('key: {} - PDS Temporal item missing {}'
                                                 .format(nhs_number, member))
                return

            value = temporal_dict[member]

            # blank value
            if not value:
                if raise_if_missing:
                    raise PDSDataImportException('key: {} - PDS Temporal item invalid {}'
                                                 .format(nhs_number, member))
                del temporal_dict[member]
                return

            cleaned = validate(value) if validate else value

            # string dates
            if not cleaned:
                raise PDSDataImportException('key: {} - PDS Temporal item invalid {}, item: "{}"'
                                             .format(nhs_number, member, value))

            temporal_dict[member] = cleaned

        def check_list(member: str, raise_if_empty: bool = False):
            member_list = record_dict.get(member, [])  # type: List[MutableMapping]
            if member_list:
                for item in member_list:
                    vali_dict(item, PDSRecordPaths.EFFECTIVE_FROM, vali_date, True)
                    vali_dict(item, PDSRecordPaths.EFFECTIVE_TO, vali_date)
            elif raise_if_empty:
                raise PDSDataImportException('key: {} - PDS Record missing an entry in {}'.format(nhs_number, member))

            member_list.sort(key=lambda x: (
                x[PDSRecordPaths.SERIAL_CHANGE_NUMBER],
                x[PDSRecordPaths.EFFECTIVE_FROM],
                x.get(PDSRecordPaths.EFFECTIVE_TO, sys.maxsize)
            ), reverse=True)

        def check_migrant_list(member):
            member_list = record_dict.get(member, [])  # type: List[MutableMapping]
            if member_list:
                for item in member_list:
                    vali_dict(item, PDSRecordPaths.HOME_OFFICE_REF_NO, None, True)
                    vali_dict(item, PDSRecordPaths.VISA_STATUS, None)
                    vali_dict(item, PDSRecordPaths.VISA_FROM, vali_date)
                    vali_dict(item, PDSRecordPaths.VISA_TO, vali_date)

                check_list(member)

        def check_mpsid_details(member):
            details = record_dict.get(member, [])  # type: List[MutableMapping]
            if details:
                for detail in details:
                    if PDSRecordPaths.MPS_ID not in detail:
                        raise PDSDataImportException(
                            'key: {} - PDS Record MPS ID detail missing {}'.format(nhs_number, PDSRecordPaths.MPS_ID))

        def check_member(member, validate=None, raise_if_missing=False):
            if member not in record_dict:
                if raise_if_missing:
                    raise PDSDataImportException('key: {} - PDS Record missing {}'.format(nhs_number, member))
                return

            value = record_dict[member]

            if not value:
                if raise_if_missing:
                    raise PDSDataImportException('key: {} - PDS Record invalid {}, item: "{}"'
                                                 .format(nhs_number, member, value))
                del record_dict[member]
                return

            cleaned = validate(value) if validate else value

            # string dates
            if not cleaned and raise_if_missing and member == "dob":
                raise PDSDataImportException('key: {} - PDS Record invalid {}, item: "{}"'.format(nhs_number, member,
                                                                                                  value))

            record_dict[member] = cleaned

        check_list(PDSRecordPaths.GP_CODES)
        check_list(PDSRecordPaths.CONFIDENTIALITY)
        check_list(PDSRecordPaths.GENDER_HISTORY, raise_if_empty=True)
        check_list(PDSRecordPaths.NAME_HISTORY)
        check_list(PDSRecordPaths.ADDRESS_HISTORY)

        check_mpsid_details(PDSRecordPaths.MPS_ID_DETAILS)

        check_migrant_list(PDSRecordPaths.MIGRANT_DATA)

        check_member(PDSRecordPaths.DOB, vali_date, raise_if_missing=mandatory_field_check)
        check_member(PDSRecordPaths.DOD, vali_date)
        check_member(PDSRecordPaths.DEATH_STATUS)
        check_member(PDSRecordPaths.SENSITIVE, vali_bool)

        return PDSRecord(record_dict)

    def nhs_number(self) -> str:
        """Gets the NHS Number supplied with the PDS record

        Returns:
            NHS Number supplied in MESH feed
        """
        return self.record_dict.get(PDSRecordPaths.NHS_NUMBER, None)

    def replaced_by_nhs_number(self) -> str:
        """Gets the NHS Number in the replacedBy field

        Returns:
            NHS Number replacedBy field
        """

        return self.record_dict.get(PDSRecordPaths.REPLACED_BY, None)

    def activity_date(self):
        """Gets the PDS Activity Date

        Returns:
            datetime: datetime of PDS Activity
        """
        return self.record_dict[PDSRecordPaths.ACTIVITY_DATE]

    def sequence_number(self):
        """Gets the PDS Activity Sequence Number

        Returns:
            int: sequence number
        """
        return self.record_dict.get(PDSRecordPaths.SERIAL_CHANGE_NUMBER, 0)

    def migrant_data(self):
        """Get list of PDS migrant data records containing visa_status, home_office_ref_no etc.

        Returns:
            list[dict]: list of migrant data records
        """
        return self.record_dict.get(PDSRecordPaths.MIGRANT_DATA, [])

    def date_of_birth(self) -> Optional[int]:
        """Get the PDS Date of Birth

        Returns:
            int: date of birth as tag date
        """
        return self.record_dict.get(PDSRecordPaths.DOB, None)

    def gender_history(self) -> List:
        """Gets full list of pds record gender_history

        Returns:
            list[dict]: list of pds record temporal gender_history
        """
        return self.record_dict.get(PDSRecordPaths.GENDER_HISTORY, [])

    def gender_at(self, point_in_time: Union[int, str, date, datetime]) -> Optional[str]:
        effective_at = self.get_point_in_time(point_in_time)
        if self.invalid_at(effective_at):
            return None

        for gender_dict in self.gender_history():
            if self._effective_at(gender_dict, effective_at):
                return gender_dict.get(PDSRecordPaths.GENDER, None)
        return None

    def date_of_death(self) -> Optional[int]:
        return self.record_dict.get(PDSRecordPaths.DOD, None)

    def death_status(self) -> str:
        return self.record_dict.get(PDSRecordPaths.DEATH_STATUS, None)

    def name_history(self) -> List:
        return self.record_dict.get(PDSRecordPaths.NAME_HISTORY, [])

    def family_name_at(self, point_in_time) -> Optional[str]:
        effective_at = self.get_point_in_time(point_in_time)
        if self.invalid_at(effective_at):
            return None

        for history in self.name_history():
            if self._effective_at(history, effective_at):
                return history.get(PDSRecordPaths.FAMILY_NAME, None)
        return None

    def given_names_at(self, point_in_time) -> Optional[List[str]]:
        effective_at = self.get_point_in_time(point_in_time)
        if self.invalid_at(effective_at):
            return None

        name_history = self.name_history()

        for history in name_history:
            if self._effective_at(history, effective_at):
                if PDSRecordPaths.GIVEN_NAME in name_history[0]:
                    return [item[PDSRecordPaths.NAME] for item in history.get(PDSRecordPaths.GIVEN_NAME, None)]
                elif PDSRecordPaths.GIVEN_NAMES in name_history[0]:
                    return history.get(PDSRecordPaths.GIVEN_NAMES, None)

        return None

    def email_address(self) -> str:
        return self.record_dict.get(PDSRecordPaths.EMAIL_ADDRESS)

    def telephone(self) -> str:
        return self.record_dict.get(PDSRecordPaths.TELEPHONE)

    def mobile_phone(self) -> str:
        return self.record_dict.get(PDSRecordPaths.MOBILE_PHONE)

    def address_history(self) -> Optional[List[Mapping]]:
        return self.record_dict.get(PDSRecordPaths.ADDRESS_HISTORY, [])

    def mps_id_details(self) -> Optional[List]:
        return self.record_dict.get(PDSRecordPaths.MPS_ID_DETAILS, [])

    def visa_status_at(self, point_in_time) -> Optional[str]:
        effective_at = self.get_point_in_time(point_in_time)
        if self.invalid_at(effective_at):
            return None

        for data in self.migrant_data():
            if self._effective_at(data, effective_at):
                return data.get(PDSRecordPaths.VISA_STATUS, None)
        return None

    def postcode_at(self, point_in_time) -> Optional[str]:
        effective_at = self.get_point_in_time(point_in_time)
        if self.invalid_at(effective_at):
            return None

        for address in self.address_history():
            if self._effective_at(address, effective_at):
                if address.get(PDSRecordPaths.POSTALCODE, None):
                    return address.get(PDSRecordPaths.POSTALCODE, None)
                return address.get(PDSRecordPaths.POSTCODE, None)
        return None

    def address_at(self, point_in_time) -> Optional[List[str]]:
        effective_at = self.get_point_in_time(point_in_time)
        if self.invalid_at(effective_at):
            return None

        address_history = self.address_history()

        for address in address_history:
            if self._effective_at(address, effective_at):
                if PDSRecordPaths.ADDR in address_history[0]:
                    return [item[PDSRecordPaths.LINE] for item in address.get(PDSRecordPaths.ADDR, None)]
                elif PDSRecordPaths.LINES in address_history[0]:
                    address_lines = address.get(PDSRecordPaths.LINES, None)
                    if address_lines:
                        while len(address_lines) < 5:
                            address_lines.append('')
                    return address_lines

        return None

    def confidentiality(self) -> List[Mapping]:
        """Gets full list of pds record confidentiality codes

        Returns:
            list[dict]: list of pds record temporal confidentiality codes
        """
        return self.record_dict.get(PDSRecordPaths.CONFIDENTIALITY, [])

    def confidentiality_at(self, point_in_time: Union[int, str, date, datetime]) -> Mapping:
        effective_at = self.get_point_in_time(point_in_time)

        for data in self.confidentiality():
            if self._effective_at(data, effective_at):
                return data.get(PDSRecordPaths.VAL, None)

    def _has_confidentiality_code_at(self, effective_at: int, *codes) -> bool:
        confidentiality = self.confidentiality_at(effective_at)

        if confidentiality and confidentiality in codes:
            return True

        return False

    def invalid_at(self, point_in_time: Union[int, str, date, datetime]) -> bool:
        """tell me the record was marked as invalid at the given point_in_time

        Args:
            point_in_time (object):  datetime/date/int to check for the confidentiality invalidation

        Returns:
            bool: if the record has been marked as invalid before point in time
        """

        effective_at = self.get_point_in_time(point_in_time)
        return self._has_confidentiality_code_at(effective_at, 'I')

    def sensitive_at(self, point_in_time: Union[int, str, date, datetime]) -> bool:
        """tell me the record was marked as sensitive at the given point_in_time

        Args:
            point_in_time (object):  datetime/date/int to check for the confidentiality invalidation

        Returns:
            bool: if the record has been marked as sensitive before point in time
        """

        effective_at = self.get_point_in_time(point_in_time)
        return self._has_confidentiality_code_at(effective_at, 'S', 'Y')

    def gp_codes(self) -> List[Mapping]:
        """Gets full list of pds record gp provider codes

        Returns:
            list[dict]: list of pds record temporal gp provider codes
        """
        return self.record_dict.get(PDSRecordPaths.GP_CODES, [])

    def gp_code_at(self, point_in_time: Union[int, str, date, datetime]):
        """tell me the patient gp provider code at a given point in time

        Args:
            point_in_time (object):  datetime/date/int to check for the gp provider code

        Returns:
            str: gp provider code at the point in time
        """
        effective_at = self.get_point_in_time(point_in_time)

        if self.invalid_at(effective_at):
            return None

        for gp_code_dict in self.gp_codes():
            if self._effective_at(gp_code_dict, effective_at):
                return gp_code_dict.get(PDSRecordPaths.VAL, None)
        return None

    @staticmethod
    def _effective_at(temporal_dict: Mapping, point_in_time: int):
        """

        Args:
            temporal_dict: temporal item dict
            point_in_time: int representation e.g. 20160924005959 of point in time to evaluate system dates

        Returns:
            bool: True if the point in time date was within the range.

        """
        sed_from = temporal_dict.get(PDSRecordPaths.EFFECTIVE_FROM, None)
        if sed_from is None:
            raise ValueError('PDS Temporal field should have an effective from date')

        distant_point_in_time = 99999999999999

        return sed_from <= point_in_time <= temporal_dict.get(PDSRecordPaths.EFFECTIVE_TO, distant_point_in_time)
