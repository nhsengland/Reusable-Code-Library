import sys
from datetime import datetime, date
from typing import Tuple

from dsp.model.base_model import BaseModel
from testdata.ref_data import DEFAULT_MAX_END_DATE


class _ONSRecordPaths:
    POSTCODE = 'pcds'
    DATE_OF_INTRODUCTION = 'doint'
    DATE_OF_TERMINATION = 'doterm'
    EFFECTIVE_FROM = 'from'
    CCG = 'ccg'
    ICB = 'stp'
    SHA = 'oshlthau'
    COUNTRY = 'ctry'
    SHA_OLD = 'oshaprev'
    ED_COUNTY_CODE = 'psed'
    ED_DISTRICT_CODE = 'cened'
    ELECTORAL_WARD_98 = 'ward98'
    UNITARY_AUTHORITY = 'oslaua'
    LOWER_LAYER_SOA = 'lsoa11'
    MIDDLE_LAYER_SOA = 'msoa11'
    OS_EAST_1M = 'oseast1m'
    OS_NORTH_1M = 'osnrth1m'
    RESIDENCE_COUNTY = 'oscty'
    OS_WARD_2011 = 'osward'


ONSRecordPaths = _ONSRecordPaths()
del _ONSRecordPaths


_ons_value_types = [
    ONSRecordPaths.COUNTRY,
    ONSRecordPaths.CCG,
    ONSRecordPaths.SHA,
    ONSRecordPaths.SHA_OLD,
    ONSRecordPaths.ICB,
    ONSRecordPaths.ED_COUNTY_CODE,
    ONSRecordPaths.ED_DISTRICT_CODE,
    ONSRecordPaths.ELECTORAL_WARD_98,
    ONSRecordPaths.UNITARY_AUTHORITY,
    ONSRecordPaths.LOWER_LAYER_SOA,
    ONSRecordPaths.MIDDLE_LAYER_SOA,
    ONSRecordPaths.OS_EAST_1M,
    ONSRecordPaths.OS_NORTH_1M,
    ONSRecordPaths.RESIDENCE_COUNTY,
    ONSRecordPaths.OS_WARD_2011,
    # ONSRecordPaths.RURAL_URBAN_INDICATOR,
    # ONSRecordPaths.OUTPUT_AREA_CODE_2011,
    # ONSRecordPaths.CANCER_REGISTRY,
    # ONSRecordPaths.GOVERNMENT_OFFICE_REGION_OF_TREATMENT,
    # ONSRecordPaths.PARLIAMENTARY_CONSTITUENCY,
    # ONSRecordPaths.PCT,
    # ONSRecordPaths.OLD_PCT
]


_far_future_date = 80000101
_default_from_date = 19000101


class ONSRecord(BaseModel):

    def event_id(self):
        pass

    def generated_identifier(self):
        pass

    @classmethod
    def from_dict(cls, record_dict):
        return cls.build_record(record_dict)

    @classmethod
    def build_record(cls, record_dict):
        return ONSRecord(record_dict)

    @classmethod
    def from_history_item(cls, postcode, history_row):
        """ initialise a ONS record item from a history row

        Args:
            postcode (str): postcode including spaces
            history_row (dict): history row item
        Returns:
            ONSRecord: initialised record
        """

        effective_from = history_row[ONSRecordPaths.EFFECTIVE_FROM] or _default_from_date

        record_dict = {
            value_type: [(effective_from, history_row[value_type])]
            if history_row[value_type] else [] for value_type in _ons_value_types
        }

        record_dict.update({
            ONSRecordPaths.POSTCODE: postcode,
            ONSRecordPaths.DATE_OF_INTRODUCTION: history_row[ONSRecordPaths.DATE_OF_INTRODUCTION]
        })

        dot = history_row.get(ONSRecordPaths.DATE_OF_TERMINATION, None)
        if dot and dot < _far_future_date:
            record_dict[ONSRecordPaths.DATE_OF_TERMINATION] = dot

        return cls.from_dict(record_dict)

    @classmethod
    def from_history_items(cls, postcode, history_rows):
        """ initialise a ONS record item from list of history rows

        Args:
            postcode (str): postcode including spaces
            history_rows (list[dict]): history row items
        Returns:
            ONSRecord: initialised record
        """

        rec = None
        for row in history_rows:
            if not rec:
                rec = cls.from_history_item(postcode, row)
                continue
            rec.add_history_item(row)
        return rec

    def add_history_item(self, history_row):
        """ Add a historic item to an existing ONSRecord

        Args:
            history_row (dict): ons record history row data
        """

        doi = history_row[ONSRecordPaths.DATE_OF_INTRODUCTION]
        dot = history_row.get(ONSRecordPaths.DATE_OF_TERMINATION, None)

        # all earlier introduction and termination
        if doi and doi < self.record_dict[ONSRecordPaths.DATE_OF_INTRODUCTION]:
            self.record_dict[ONSRecordPaths.DATE_OF_INTRODUCTION] = doi

        if dot and dot < _far_future_date:
            self.record_dict[ONSRecordPaths.DATE_OF_TERMINATION] = dot
        elif not dot and self.record_dict.get(ONSRecordPaths.DATE_OF_TERMINATION, None):
            # This history item has no termination date so clear the existing one as we're back to being open-ended
            # Scenario - recycled postcodes - previously terminated postcodes now back in use
            del self.record_dict[ONSRecordPaths.DATE_OF_TERMINATION]

        effective_from = history_row[ONSRecordPaths.EFFECTIVE_FROM] or _default_from_date

        for value_type in _ons_value_types:
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
                        self.record_dict[ONSRecordPaths.POSTCODE],
                        history_row
                    )
                )

            if last_value == new_value:
                continue

            item_history.insert(0, (effective_from, new_value))

    def value_at(self, value_type, point_in_time):
        """ Get a the value type for a given value type at a given point in time

        Args:
            value_type (str): ONSRecord value type to look for
            point_in_time (int,date,str): point in time to evaluate

        Returns:
            str: the value at the given point in time
        """
        at = self.get_point_in_time(point_in_time)
        # todo: current sus does not respect the date of termination of a postcode
        # todo: need to investigate this further when we tackle the ons import
        # if not self.in_reference_data_at(at):
        #     return None

        for effective_from, value in self.record_dict.get(value_type, []):
            if effective_from <= at:
                return value
        return None

    def active_at(self, point_in_time) -> bool:
        # mm: default doint to 0 ..
        date_of_introduction = self.record_dict.get(ONSRecordPaths.DATE_OF_INTRODUCTION, 0) or 0
        date_of_termination = self.record_dict.get(ONSRecordPaths.DATE_OF_TERMINATION, sys.maxsize)
        point_in_time = self.get_point_in_time(point_in_time)
        return date_of_introduction <= point_in_time <= date_of_termination

    def get_sha_at(self, point_in_time):

        return self.value_at(ONSRecordPaths.SHA, point_in_time)

    def get_country_at(self, point_in_time):

        return self.value_at(ONSRecordPaths.COUNTRY, point_in_time)

    def get_ccg_code_at(self, point_in_time):

        return self.value_at(ONSRecordPaths.CCG, point_in_time)

    def get_icb_code_at(self, point_in_time):
        """
        Get the ICB (Integrated Care Board) at a given point in time

        Args:
            point_in_time (int/str/date/datetime): The point in time to evaluate

        Returns:
            str: The ICB value at the given point in time

        """

        return self.value_at(ONSRecordPaths.ICB, point_in_time)

    def get_unitary_authority_at(self, point_in_time):
        """
        Get the Unitary Authority value at a given point in time.

        Args:
            point_in_time (int/str/date/datetime): The point in time to evaluate

        Returns:
            str: The unitary authority value at the given point in time

        """

        return self.value_at(ONSRecordPaths.UNITARY_AUTHORITY, point_in_time)

    def get_lsoa_at(self, point_in_time):
        """
        Get the Lower Super Output Area value at a given point in time.

        Args:
            point_in_time (int/str/date/datetime): The point in time to evaluate

        Returns:
            str: The unitary authority value at the given point in time

        """

        return self.value_at(ONSRecordPaths.LOWER_LAYER_SOA, point_in_time)

    def get_msoa_at(self, point_in_time):
        """
        Get the Middle Super Output Area value at a given point in time.

        Args:
            point_in_time (int/str/date/datetime): The point in time to evaluate

        Returns:
            str: The unitary authority value at the given point in time

        """

        return self.value_at(ONSRecordPaths.MIDDLE_LAYER_SOA, point_in_time)

    def get_eastings_at(self, point_in_time):

        return self.value_at(ONSRecordPaths.OS_EAST_1M, point_in_time)

    def get_northings_at(self, point_in_time):

        return self.value_at(ONSRecordPaths.OS_NORTH_1M, point_in_time)

    def get_county_at(self, point_in_time):

        return self.value_at(ONSRecordPaths.RESIDENCE_COUNTY, point_in_time)

    def get_electoral_ward_at(self, point_in_time):

        return self.value_at(ONSRecordPaths.OS_WARD_2011, point_in_time)

    def get_pcds_at(self, point_in_time):

        if self.active_at(point_in_time):
            return self.record_dict.get(ONSRecordPaths.POSTCODE)

        return None
