"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
import typing as t
from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from uuid import (
    UUID,
)

from ..types import (
    PYTHON_IMMUTABLE_TYPES,
)

if t.TYPE_CHECKING:
    from .fields import ModelField  # pragma: no cover
logger = logging.getLogger(__name__)

T = t.TypeVar("T")


class AvroDataEncoder:
    """Avro Data Encoder class."""

    def __init__(self, value: t.Any):
        self._value = value

    @classmethod
    def from_field(cls, field: ModelField) -> AvroDataEncoder:
        """Build a new instance from a ``ModelField``.

        :param field: The model field.
        :return: A new avro schema builder instance.
        """
        return cls(field.value)

    def build(self):
        """Build a avro data representation based on the content of the given field.

        :return: A `avro`-compatible data.
        """
        return self._to_avro_raw(self._value)

    def _to_avro_raw(self, value: t.Any) -> t.Any:
        if value is None:
            return None
        if type(value) in PYTHON_IMMUTABLE_TYPES:
            return value

        if type(value) is date:
            return self._date_to_avro_raw(value)

        if type(value) is time:
            return self._time_to_avro_raw(value)

        if type(value) is datetime:
            return self._datetime_to_avro_raw(value)

        if isinstance(value, UUID):
            return self._uuid_to_avro_raw(value)

        if isinstance(value, list):
            return [self._to_avro_raw(v) for v in value]

        if isinstance(value, dict):
            return {k: self._to_avro_raw(v) for k, v in value.items()}

        return value.avro_data

    @staticmethod
    def _date_to_avro_raw(value: date) -> int:
        return (value - date(1970, 1, 1)).days

    @staticmethod
    def _time_to_avro_raw(value: time) -> int:
        return (datetime.combine(date(1, 1, 1), value) - datetime(1, 1, 1)) // timedelta(microseconds=1)

    @staticmethod
    def _datetime_to_avro_raw(value: datetime) -> int:
        return (value - datetime(1970, 1, 1, tzinfo=value.tzinfo)) // timedelta(microseconds=1)

    @staticmethod
    def _uuid_to_avro_raw(value: UUID) -> str:
        return str(value)
