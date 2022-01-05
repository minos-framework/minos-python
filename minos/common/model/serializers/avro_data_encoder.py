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
    TYPE_CHECKING,
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

if TYPE_CHECKING:
    from ..abc import (
        Model,
    )
    from ..fields import (
        Field,
    )

logger = logging.getLogger(__name__)


class AvroDataEncoder:
    """Avro Data Encoder class."""

    def __init__(self, value: Any = None):
        self.value = value

    def build(self, value=MissingSentinel) -> Any:
        """Build an avro data representation based on the content of the given field.

        :param value: The value to be encoded.
        :return: A `avro`-compatible data.
        """
        if value is MissingSentinel:
            value = self.value
        return self._build(value)

    def _build(self, value: Any) -> Any:
        if value is None:
            return None

        from ..abc import (
            Model,
        )

        if isinstance(value, Model):
            return self._build_model(value)

        from ..abc import (
            Field,
        )

        if isinstance(value, Field):
            return self._build_field(value)

        if isinstance(value, (str, int, bool, float, bytes)):
            return value

        if isinstance(value, memoryview):
            return value.tobytes()

        if isinstance(value, Decimal):
            return float(value)

        if isinstance(value, datetime):
            return self._build_datetime(value)

        if isinstance(value, timedelta):
            return self._build_timedelta(value)

        if isinstance(value, date):
            return self._build_date(value)

        if isinstance(value, time):
            return self._build_time(value)

        if isinstance(value, UUID):
            return self._build_uuid(value)

        if isinstance(value, (list, set,)):
            return [self._build(v) for v in value]

        if isinstance(value, dict):
            return {k: self._build(v) for k, v in value.items()}

        raise MinosMalformedAttributeException(f"Given type is not supported: {type(value)!r} ({value!r})")

    def _build_model(self, model: Model) -> Any:
        raw = {name: self._build_field(field) for name, field in model.fields.items()}
        return model.encode_data(self, raw)

    def _build_field(self, field: Field) -> Any:
        return field.encode_data(self, field.value)

    @staticmethod
    def _build_date(value: date) -> int:
        return (value - date(1970, 1, 1)).days

    @staticmethod
    def _build_time(value: time) -> int:
        return (datetime.combine(date(1, 1, 1), value) - datetime(1, 1, 1)) // timedelta(microseconds=1)

    @staticmethod
    def _build_datetime(value: datetime) -> int:
        return (value.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)

    @staticmethod
    def _build_timedelta(value: timedelta) -> int:
        return value // timedelta(microseconds=1)

    @staticmethod
    def _build_uuid(value: UUID) -> str:
        return str(value)
