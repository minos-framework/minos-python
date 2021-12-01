from __future__ import (
    annotations,
)

import logging
from datetime import (
    date,
    datetime,
    time,
    timedelta,
    timezone,
)
from decimal import (
    Decimal,
)
from typing import (
    Any,
)
from uuid import (
    UUID,
)

from ...exceptions import (
    MinosMalformedAttributeException,
)
from ..types import (
    MissingSentinel,
)

logger = logging.getLogger(__name__)


class AvroDataEncoder:
    """Avro Data Encoder class."""

    def __init__(self, value: Any = None):
        self.value = value

    def build(self, value=MissingSentinel) -> Any:
        """Build a avro data representation based on the content of the given field.

        :return: A `avro`-compatible data.
        """
        if value is MissingSentinel:
            value = self.value
        return self._to_avro_raw(value)

    def _to_avro_raw(self, value: Any) -> Any:
        if value is None:
            return None

        from ..abc import (
            Model,
        )

        if isinstance(value, Model):
            return self._model_to_avro_raw(value)

        from ..abc import (
            Field,
        )

        if isinstance(value, Field):
            return self._field_to_avro_raw(value)

        if isinstance(value, (str, int, bool, float, bytes)):
            return value

        if isinstance(value, memoryview):
            return value.tobytes()

        if isinstance(value, Decimal):
            return float(value)

        if isinstance(value, datetime):
            return self._datetime_to_avro_raw(value)

        if isinstance(value, timedelta):
            return self._timedelta_to_avro_raw(value)

        if isinstance(value, date):
            return self._date_to_avro_raw(value)

        if isinstance(value, time):
            return self._time_to_avro_raw(value)

        if isinstance(value, UUID):
            return self._uuid_to_avro_raw(value)

        if isinstance(value, (list, set,)):
            return [self._to_avro_raw(v) for v in value]

        if isinstance(value, dict):
            return {k: self._to_avro_raw(v) for k, v in value.items()}

        raise MinosMalformedAttributeException(f"Given type is not supported: {type(value)!r} ({value!r})")

    def _model_to_avro_raw(self, model) -> Any:
        raw = {name: field.avro_data for name, field in model.fields.items()}
        return model.encode_data(self, raw)

    def _field_to_avro_raw(self, field) -> Any:
        return field.encode_data(self, field.value)

    @staticmethod
    def _date_to_avro_raw(value: date) -> int:
        return (value - date(1970, 1, 1)).days

    @staticmethod
    def _time_to_avro_raw(value: time) -> int:
        return (datetime.combine(date(1, 1, 1), value) - datetime(1, 1, 1)) // timedelta(microseconds=1)

    @staticmethod
    def _datetime_to_avro_raw(value: datetime) -> int:
        return (value.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)

    @staticmethod
    def _timedelta_to_avro_raw(value: timedelta) -> int:
        return value // timedelta(microseconds=1)

    @staticmethod
    def _uuid_to_avro_raw(value: UUID) -> str:
        return str(value)
