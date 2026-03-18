from typing import Union

from dsp.model.base_model import BaseModel


class _IMDRecordPaths:
    IMD_YEAR = 'IMD_YEAR'
    LSOA_CODE_2011 = 'LSOA_CODE_2011'
    IMD = 'IMD'
    RANK_IMD = 'RANK_IMD'
    DECI_IMD = 'DECI_IMD'
    INCOME = 'INCOME'
    RANK_INCOME = 'RANK_INCOME'
    DECI_INCOME = 'DECI_INCOME'
    EMPLOYMENT = 'EMPLOYMENT'
    RANK_EMPLOYMENT = 'RANK_EMPLOYMENT'
    DECI_EMPLOYMENT = 'DECI_EMPLOYMENT'
    EDUCATION_SKILLS_TRAINING = 'EDUCATION_SKILLS_TRAINING'
    RANK_EDUCATION_SKILLS_TRAINING = 'RANK_EDUCATION_SKILLS_TRAINING'
    DECI_EDUCATION_SKILLS_TRAINING = 'DECI_EDUCATION_SKILLS_TRAINING'
    HEALTH_DEP_DISABILITY = 'HEALTH_DEP_DISABILITY'
    RANK_HEALTH_DEP_DISABILITY = 'RANK_HEALTH_DEP_DISABILITY'
    DECI_HEALTH_DEP_DISABILITY = 'DECI_HEALTH_DEP_DISABILITY'
    CRIME_DISORDER = 'CRIME_DISORDER'
    RANK_CRIME_DISORDER = 'RANK_CRIME_DISORDER'
    DECI_CRIME_DISORDER = 'DECI_CRIME_DISORDER'
    BARRIERS_HOUSING_SERVICES = 'BARRIERS_HOUSING_SERVICES'
    RANK_BARRIERS_HOUSING_SERVICES = 'RANK_BARRIERS_HOUSING_SERVICES'
    DECI_BARRIERS_HOUSING_SERVICES = 'DECI_BARRIERS_HOUSING_SERVICES'
    LIVING_ENVIRONMENT = 'LIVING_ENVIRONMENT'
    RANK_LIVING_ENVIRONMENT = 'RANK_LIVING_ENVIRONMENT'
    DECI_LIVING_ENVIRONMENT = 'DECI_LIVING_ENVIRONMENT'
    IDACI = 'IDACI'
    RANK_IDACI = 'RANK_IDACI'
    DECI_IDACI = 'DECI_IDACI'
    IDAOPI = 'IDAOPI'
    RANK_IDAOPI = 'RANK_IDAOPI'
    DECI_IDAOPI = 'DECI_IDAOPI'
    CHILDREN_AND_YP_SUBDOMAIN = 'CHILDREN_AND_YP_SUBDOMAIN'
    RANK_CHILDREN_AND_YP_SUBDOMAIN = 'RANK_CHILDREN_AND_YP_SUBDOMAIN'
    DECI_CHILDREN_AND_YP_SUBDOMAIN = 'DECI_CHILDREN_AND_YP_SUBDOMAIN'
    ADULT_SKILLS_SUBDOMAIN = 'ADULT_SKILLS_SUBDOMAIN'
    RANK_ADULT_SKILLS_SUBDOMAIN = 'RANK_ADULT_SKILLS_SUBDOMAIN'
    DECI_ADULT_SKILLS_SUBDOMAIN = 'DECI_ADULT_SKILLS_SUBDOMAIN'
    GEO_BARRIERS_SUBDOMAIN = 'GEO_BARRIERS_SUBDOMAIN'
    RANK_GEO_BARRIERS_SUBDOMAIN = 'RANK_GEO_BARRIERS_SUBDOMAIN'
    DECI_GEO_BARRIERS_SUBDOMAIN = 'DECI_GEO_BARRIERS_SUBDOMAIN'
    WIDER_BARRIERS_SUBDOMAIN = 'WIDER_BARRIERS_SUBDOMAIN'
    RANK_WIDER_BARRIERS_SUBDOMAIN = 'RANK_WIDER_BARRIERS_SUBDOMAIN'
    DECI_WIDER_BARRIERS_SUBDOMAIN = 'DECI_WIDER_BARRIERS_SUBDOMAIN'
    INDOORS_SUBDOMAIN = 'INDOORS_SUBDOMAIN'
    RANK_INDOORS_SUBDOMAIN = 'RANK_INDOORS_SUBDOMAIN'
    DECI_INDOORS_SUBDOMAIN = 'DECI_INDOORS_SUBDOMAIN'
    OUTDOORS_SUBDOMAIN = 'OUTDOORS_SUBDOMAIN'
    RANK_OUTDOORS_SUBDOMAIN = 'RANK_OUTDOORS_SUBDOMAIN'
    DECI_OUTDOORS_SUBDOMAIN = 'DECI_OUTDOORS_SUBDOMAIN'
    POPULATION_TOTAL = 'POPULATION_TOTAL'
    POPULATION_0_TO_15_2012 = 'POPULATION_0_TO_15_2012'
    POPULATION_16_TO_59_2012 = 'POPULATION_16_TO_59_2012'
    POPULATION_60_AND_OVER_2012 = 'POPULATION_60_AND_OVER_2012'
    POPULATION_WORKING_AGE_2012 = 'POPULATION_WORKING_AGE_2012'
    DSS_RECORD_START_DATE = 'DSS_RECORD_START_DATE'
    DSS_RECORD_END_DATE = 'DSS_RECORD_END_DATE'
    YEAR_LSOA = 'YEAR_LSOA'
    QUANTILE_TYPE = 'QUANTILE_TYPE'
    QUANTILE_DESC = 'QUANTILE_DESC'
    Quintile = 'Quintile'
    Quartile = 'Quartile'
    Decile = 'Decile'
    Tertile = 'Tertile'


IMDRecordPaths = _IMDRecordPaths()
del _IMDRecordPaths

_imd_value_types = [
    IMDRecordPaths.Quintile,
    IMDRecordPaths.Quartile,
    IMDRecordPaths.Decile,
    IMDRecordPaths.Tertile,
    # IMDRecordPaths.IMD,
    # IMDRecordPaths.RANK_IMD,
    # IMDRecordPaths.DECI_IMD,
    # IMDRecordPaths.INCOME,
    # IMDRecordPaths.RANK_INCOME,
    # IMDRecordPaths.DECI_INCOME,
    # IMDRecordPaths.EMPLOYMENT,
    # IMDRecordPaths.RANK_EMPLOYMENT,
    # IMDRecordPaths.DECI_EMPLOYMENT,
    # IMDRecordPaths.EDUCATION_SKILLS_TRAINING,
    # IMDRecordPaths.RANK_EDUCATION_SKILLS_TRAINING,
    # IMDRecordPaths.DECI_EDUCATION_SKILLS_TRAINING,
    # IMDRecordPaths.HEALTH_DEP_DISABILITY,
    # IMDRecordPaths.RANK_HEALTH_DEP_DISABILITY,
    # IMDRecordPaths.DECI_HEALTH_DEP_DISABILITY,
    # IMDRecordPaths.CRIME_DISORDER,
    # IMDRecordPaths.RANK_CRIME_DISORDER,
    # IMDRecordPaths.DECI_CRIME_DISORDER,
    # IMDRecordPaths.BARRIERS_HOUSING_SERVICES,
    # IMDRecordPaths.RANK_BARRIERS_HOUSING_SERVICES,
    # IMDRecordPaths.DECI_BARRIERS_HOUSING_SERVICES,
    # IMDRecordPaths.LIVING_ENVIRONMENT,
    # IMDRecordPaths.RANK_LIVING_ENVIRONMENT,
    # IMDRecordPaths.DECI_LIVING_ENVIRONMENT,
    # IMDRecordPaths.IDACI,
    # IMDRecordPaths.RANK_IDACI,
    # IMDRecordPaths.DECI_IDACI,
    # IMDRecordPaths.IDAOPI,
    # IMDRecordPaths.RANK_IDAOPI,
    # IMDRecordPaths.DECI_IDAOPI,
    # IMDRecordPaths.CHILDREN_AND_YP_SUBDOMAIN,
    # IMDRecordPaths.RANK_CHILDREN_AND_YP_SUBDOMAIN,
    # IMDRecordPaths.DECI_CHILDREN_AND_YP_SUBDOMAIN,
    # IMDRecordPaths.ADULT_SKILLS_SUBDOMAIN,
    # IMDRecordPaths.RANK_ADULT_SKILLS_SUBDOMAIN,
    # IMDRecordPaths.DECI_ADULT_SKILLS_SUBDOMAIN,
    # IMDRecordPaths.GEO_BARRIERS_SUBDOMAIN,
    # IMDRecordPaths.RANK_GEO_BARRIERS_SUBDOMAIN,
    # IMDRecordPaths.DECI_GEO_BARRIERS_SUBDOMAIN,
    # IMDRecordPaths.WIDER_BARRIERS_SUBDOMAIN,
    # IMDRecordPaths.RANK_WIDER_BARRIERS_SUBDOMAIN,
    # IMDRecordPaths.DECI_WIDER_BARRIERS_SUBDOMAIN,
    # IMDRecordPaths.INDOORS_SUBDOMAIN,
    # IMDRecordPaths.RANK_INDOORS_SUBDOMAIN,
    # IMDRecordPaths.DECI_INDOORS_SUBDOMAIN,
    # IMDRecordPaths.OUTDOORS_SUBDOMAIN,
    # IMDRecordPaths.RANK_OUTDOORS_SUBDOMAIN,
    # IMDRecordPaths.DECI_OUTDOORS_SUBDOMAIN,
    # IMDRecordPaths.POPULATION_TOTAL,
    # IMDRecordPaths.POPULATION_0_TO_15_2012,
    # IMDRecordPaths.POPULATION_16_TO_59_2012,
    # IMDRecordPaths.POPULATION_60_AND_OVER_2012,
    # IMDRecordPaths.POPULATION_WORKING_AGE_2012,
]

_far_future_date = 80000101
_default_from_date = 19000101


class IMDRecord(BaseModel):

    @staticmethod
    def format_key(lsoa: str, imd_year: Union[str, int], lsoa_year: Union[str, int]):
        return '{}:{}:{}'.format(lsoa, imd_year, lsoa_year)

    @classmethod
    def from_dict(cls, record_dict):
        return cls.build_record(record_dict)

    @classmethod
    def build_record(cls, record_dict):
        return IMDRecord(record_dict)

    @classmethod
    def from_history_item(cls, history_row):
        """ initialise a ONS record item from a history row

        Args:
            history_row (dict): history row item
        Returns:
            IMDRecord: initialised record
        """

        effective_from = history_row[IMDRecordPaths.DSS_RECORD_START_DATE] or _default_from_date

        record_dict = {
            value_type: [(effective_from, history_row[value_type])] if history_row.get(value_type) is not None else []
            for value_type in _imd_value_types
        }

        return cls.from_dict(record_dict)

    def add_history_item(self, history_row):
        """ Add a historic item to an existing ONSRecord

        Args:
            history_row (dict): imd record history row data
        """

        effective_from = history_row[IMDRecordPaths.DSS_RECORD_START_DATE] or _default_from_date

        for value_type in _imd_value_types:
            new_value = history_row.get(value_type)

            if new_value is None:
                continue

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
                        self.record_dict[IMDRecordPaths.LSOA_CODE_2011],
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
        pit = self.get_point_in_time(point_in_time)

        for effective_from, value in self.record_dict.get(value_type, []):
            if effective_from <= pit:
                return value
        return None
