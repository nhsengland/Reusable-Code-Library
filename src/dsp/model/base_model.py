import json
from abc import abstractmethod
from datetime import date, datetime
from typing import Union, Optional

import msgpack

from dsp.common.json_helpers import ISODateEncoder
from testdata.ref_data import DEFAULT_MAX_END_DATE
from dsp.shared.content_types import NO_SEPARATOR_DATE_FORMAT


class BaseModelJSONEncoder(ISODateEncoder):

    def default(self, o):
        if isinstance(o, BaseModel):
            return o.record_dict

        return super(BaseModelJSONEncoder, self).default(o)


class BaseModel:
    """Base class for common functionality shared between all types in the Model."""

    def __init__(self, record_dict):
        self.record_dict = record_dict

    @classmethod
    @abstractmethod
    def build_record(cls, record_dict):
        """Constructs an instance of this class from a dictionary

        Args:
            record_dict(dict): the serialised record to be used to construct the instance

        Returns:
            BaseModel: the deserialised record
        """

    @classmethod
    def from_json(cls, serialised_record: str) -> 'BaseModel':
        """Constructs an instance of this class from the serialised data

        Args:
            serialised_record: the serialised record to be used to construct the instance

        Returns:
            BaseModel: the deserialised record
        """
        record_dict = json.loads(serialised_record)
        return cls.build_record(record_dict)

    @classmethod
    def from_msgpack(cls, serialised_record):
        """Constructs an instance of this class from the serialised data

        Args:
            serialised_record: the serialised record to be used to construct the instance

        Returns:
            BaseModel: the deserialised record
        """
        record_dict = msgpack.unpackb(serialised_record)
        return cls.build_record(record_dict)

    def to_json(self) -> str:
        """ Serialise this record to JSON

        Returns:
            str: json data
        """
        return json.dumps(self.project_for_log(), cls=BaseModelJSONEncoder, separators=(',', ':'))

    def project_for_log(self):
        """Default projection of the model object for log message.

        If an object that inherits this type is added to structured logging, the :py:func:`for_json` will be
        invoked to produce a suitable representation for logging. :py:class:`for_json` calls this function to
        project the object. Sub-types can override this method to customise the projection.

        If the instance object is called `example_record` the log line would look like::

            log_message(message_type='example_general_log_message',
                        record=example_record)

        If an alternative projection is required for a specific type of logging (e.g. within a particular sub-componet)
        alternative project methods should be implemented using the naming scheme `project_for_[subsystem]_log` and
        that method should be explicitly called when constructing the log message.

        To call an alternative project, assuming the instance object is called example_record and it's class has an
        alternative project method called `project_for_derivation_log`, the log line would look like::

            log_message(message_type='example_derivation_log_message',
                        record=example_record.project_for_derivation_log())

        Returns:
            Representation of the object suitable for structured logging (dict or str). :py:func:`json.dumps` must
            be able to encode the type so a `dict` or `str` is recommended.
        """
        projection = self.record_dict
        return projection

    @staticmethod
    def get_point_in_time(
        point_in_time: Optional[Union[int, str, date, datetime]]
    ) -> int:
        """ convert an str/int/date/datetime to an int representation of the date
            e.g. datetime('31 Jan 2017 17:59') becomes 20170131

        Args:
            point_in_time (int/str/date/datetime)

        Returns:
            int:  int representation of the point in time
        """
        if point_in_time is None:
            return DEFAULT_MAX_END_DATE  # get current information

        time_type = type(point_in_time)

        if time_type == int:
            if point_in_time <= DEFAULT_MAX_END_DATE:
                return point_in_time
            point_in_time = str(point_in_time)

        point_in_time_str = point_in_time
        if time_type in [datetime, date]:
            point_in_time_str = point_in_time.strftime(NO_SEPARATOR_DATE_FORMAT)

        point_in_time_str = point_in_time_str.replace('-', '')
        point_in_time_str = point_in_time_str[:8]

        return int(point_in_time_str)
