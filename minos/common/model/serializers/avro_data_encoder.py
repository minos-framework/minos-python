"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from decimal import (
    Decimal,
)
from typing import (
    TYPE_CHECKING,
    Any,
)
from uuid import (
    UUID,
)

from ...exceptions import (
    MinosMalformedAttributeException,
)

if TYPE_CHECKING:
    from ..fields import (
        Field,
    )

logger = logging.getLogger(__name__)


class AvroDataEncoder:
    """Avro Data Encoder class."""

    def __init__(self, value: Any):
        self.value = value

    @classmethod
    def from_field(cls, field: Field) -> AvroDataEncoder:
        """Build a new instance from a ``Field``.

        :param field: The model field.
        :return: A new avro schema builder instance.
        """
        return cls(field.value)

    def build(self) -> Any:
        """Build a avro data representation based on the content of the given field.

        :return: A `avro`-compatible data.
        """
        return self._to_avro_raw(self.value)

    def _to_avro_raw(self, value: Any) -> Any:
        if value is None:
            return None

        if isinstance(value, (str, int, bool, float, bytes)):
            return value

        if isinstance(value, memoryview):
            return value.tobytes()

        if isinstance(value, Decimal):
            return float(value)

        if isinstance(value, datetime):
            return self._datetime_to_avro_raw(value)

        if isinstance(value, date):
            return self._date_to_avro_raw(value)

        if isinstance(value, time):
            return self._time_to_avro_raw(value)

        if isinstance(value, UUID):
            return self._uuid_to_avro_raw(value)

        if isinstance(value, list):
            return [self._to_avro_raw(v) for v in value]

        if isinstance(value, dict):
            return {k: self._to_avro_raw(v) for k, v in value.items()}

        if hasattr(value, "avro_data"):
            return value.avro_data

        raise MinosMalformedAttributeException(f"Given type is not supported: {type(value)!r} ({value!r})")

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
