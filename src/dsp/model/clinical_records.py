from datetime import date, datetime
from typing import Union

from dsp.validations.common import DateDelta
from dsp.model import base_model
from testdata.ref_data import DEFAULT_MAX_END_DATE


class ClinicalMappingTableModel(base_model.BaseModel):
    fields = [
        'snomedct_id',
        'snomedct_description',
        'nicip_id',
        'nicip_description',
        'modality_id',
        'modality',
        'sub_modality_id',
        'sub_modality',
        'region_id',
        'region',
        'sub_region_id',
        'sub_region',
        'system_id',
        'system',
        'sub_system_id',
        'sub_system',
        'sub_system_component_id',
        'sub_system_component',
        'morphology_id',
        'morphology',
        'fetal_id',
        'fetal',
        'early_diagnosis_of_cancer',
        'sub_early_diagnosis_of_cancer',
    ]

    @classmethod
    def build_record(cls, record_dict):
        return cls(record_dict)

    def __getattr__(self, item):
        if item in self.fields:
            return self.record_dict.get(item)
        raise AttributeError('Unknown field')

    def event_id(self):
        pass

    def generated_identifier(self):
        pass


class ClinicalRecordModel(base_model.BaseModel):
    fields = [
        'eff_from',
        'eff_to',
        'clinical_map',
    ]

    @classmethod
    def build_record(cls, record_dict):
        return cls(record_dict)

    def __getattr__(self, item):
        if item in self.fields:
            return self.record_dict.get(item)
        raise AttributeError('Unknown field')

    def event_id(self):
        pass

    def generated_identifier(self):
        pass

    def was_active_at(
            self, point_in_time: Union[str, int, date, datetime] = None, offset: DateDelta = None,
            format_str='%Y%m%d') -> bool:
        eff_from = self.eff_from
        eff_to = self.eff_to or DEFAULT_MAX_END_DATE
        if point_in_time and offset:
            eff_from = datetime.strptime(str(eff_from), format_str)
            eff_from = eff_from - offset
            eff_to = datetime.strptime(str(eff_to), format_str)
            eff_to = eff_to + offset \
                if eff_to != datetime.strptime(str(DEFAULT_MAX_END_DATE), format_str) \
                else datetime.strptime(str(DEFAULT_MAX_END_DATE), format_str)

            return eff_from <= point_in_time <= eff_to

        point_in_time = self.get_point_in_time(point_in_time)
        return eff_from <= point_in_time <= eff_to
