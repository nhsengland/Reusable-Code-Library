import sys
from datetime import date, datetime
from typing import Union, Optional

from dsp.model.base_model import BaseModel


class _SchoolRecordPaths:
    CODE = 'code'
    NAME = 'name'
    NATIONAL_GROUPING = 'national_grouping'
    HIGH_LEVEL_HEALTH_GEOGRAPHY = 'high_level_health_geography'
    ADDRESS_LINE_1 = 'address_line_1'
    ADDRESS_LINE_2 = 'address_line_2'
    ADDRESS_LINE_3 = 'address_line_3'
    ADDRESS_LINE_4 = 'address_line_4'
    ADDRESS_LINE_5 = 'address_line_5'
    POSTCODE = 'postcode'
    OPEN_DATE = 'open_date'
    CLOSE_DATE = 'close_date'
    LOCAL_AUTHORITY = 'local_authority'
    CONTACT_TELEPHONE_NUMBER = 'contact_telephone_number'
    CURRENT_CARE_ORGANISATION = 'current_care_organisation'
    TYPE_OF_ESTABLISHMENT = 'type_of_establishment'
    EFFECTIVE_FROM = 'dss_record_start_date'


SchoolRecordPaths = _SchoolRecordPaths()
del _SchoolRecordPaths


_school_value_types = [
    SchoolRecordPaths.NAME,
    SchoolRecordPaths.NATIONAL_GROUPING,
    SchoolRecordPaths.HIGH_LEVEL_HEALTH_GEOGRAPHY,
    SchoolRecordPaths.ADDRESS_LINE_1,
    SchoolRecordPaths.ADDRESS_LINE_2,
    SchoolRecordPaths.ADDRESS_LINE_3,
    SchoolRecordPaths.ADDRESS_LINE_4,
    SchoolRecordPaths.ADDRESS_LINE_5,
    SchoolRecordPaths.POSTCODE,
    SchoolRecordPaths.LOCAL_AUTHORITY,
    SchoolRecordPaths.CONTACT_TELEPHONE_NUMBER,
    SchoolRecordPaths.CURRENT_CARE_ORGANISATION,
    SchoolRecordPaths.TYPE_OF_ESTABLISHMENT,
    SchoolRecordPaths.CLOSE_DATE
]


class SchoolRecord(BaseModel):

    @classmethod
    def build_record(cls, record_dict: dict):
        return SchoolRecord(record_dict)

    @classmethod
    def from_dict(cls, record_dict: dict):
        return cls.build_record(record_dict)

    @classmethod
    def from_history_item(cls, code: str, history_row: dict):
        effective_from = history_row[SchoolRecordPaths.EFFECTIVE_FROM]

        record_dict = {
            value_type: [(effective_from, history_row[value_type])]
            if history_row[value_type] else [] for value_type in _school_value_types
        }

        record_dict.update({
            SchoolRecordPaths.CODE: code,
            SchoolRecordPaths.OPEN_DATE: history_row[SchoolRecordPaths.OPEN_DATE],
        })

        return cls.from_dict(record_dict)

    def add_history_item(self, history_row: dict):
        open_date = history_row[SchoolRecordPaths.OPEN_DATE]
        effective_from = history_row[SchoolRecordPaths.EFFECTIVE_FROM]

        if open_date and open_date < self.record_dict[SchoolRecordPaths.OPEN_DATE]:
            self.record_dict[SchoolRecordPaths.OPEN_DATE] = open_date


        for value_type in _school_value_types:
            new_value = history_row[value_type]
            item_history = self.record_dict[value_type]

            if not item_history:
                if not new_value:
                    continue
                item_history.append((effective_from, new_value))
                continue

            last_effective_from, last_value = item_history[0]
            if effective_from <= last_effective_from:
                raise ValueError(
                    'History played out of order for: {} - row {}'.format(
                        self.record_dict[SchoolRecordPaths.CODE],
                        history_row
                    )
                )

            if last_value == new_value:
                continue

            item_history.insert(0, (effective_from, new_value))

    def value_at(self, value_type: str, point_in_time: Union[str, int, date, datetime]) -> Optional[str]:
        """
        Loop through values in descending time order, when effective from falls within that time period,
        return the value

        Args:
            value_type: str
            point_in_time: Union[str, int, date, datetime]

        Returns:
            Optional string value if the value exists at point in time

        """

        at = self.get_point_in_time(point_in_time)
        for effective_from, value in self.record_dict.get(value_type, []):
            if effective_from <= at:
                return value
        return None

    def active_at(self, point_in_time: Union[str, int, date, datetime]) -> bool:
        open_date = self.record_dict.get(SchoolRecordPaths.OPEN_DATE, 0)
        point_in_time = self.get_point_in_time(point_in_time)
        close_date = self.value_at(SchoolRecordPaths.CLOSE_DATE, point_in_time)
        if not close_date:
            close_date = sys.maxsize
        return open_date <= point_in_time <= close_date
