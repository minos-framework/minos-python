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

from .....exceptions import (
    MinosMalformedAttributeException,
)
from ....types import (
    MissingSentinel,
)
from ...abc import (
    DataEncoder,
)

if TYPE_CHECKING:
    from ....abc import (
        Model,
    )
    from ....fields import (
        Field,
    )

logger = logging.getLogger(__name__)


class AvroDataEncoder(DataEncoder):
    """Avro Data Encoder class."""

    def __init__(self, value: Any = None):
        self.value = value

    def build(self, value=MissingSentinel, **kwargs) -> Any:
        """Build an avro data representation based on the content of the given field.

        :param value: The value to be encoded.
        :return: A `avro`-compatible data.
        """
        if value is MissingSentinel:
            value = self.value
        return self._build(value, **kwargs)

    def _build(self, value: Any, **kwargs) -> Any:
        if value is None:
            return None

        from ....abc import (
            Model,
        )

        if isinstance(value, Model):
            return self._build_model(value, **kwargs)

        from ....abc import (
            Field,
        )

        if isinstance(value, Field):
            return self._build_field(value, **kwargs)

        if isinstance(value, (str, int, bool, float, bytes)):
            return value

        if isinstance(value, memoryview):
            return value.tobytes()

        if isinstance(value, Decimal):
            return float(value)

        if isinstance(value, datetime):
            return self._build_datetime(value, **kwargs)

        if isinstance(value, timedelta):
            return self._build_timedelta(value, **kwargs)

        if isinstance(value, date):
            return self._build_date(value, **kwargs)

        if isinstance(value, time):
            return self._build_time(value, **kwargs)

        if isinstance(value, UUID):
            return self._build_uuid(value, **kwargs)

        if isinstance(value, (list, set)):
            return [self._build(v, **kwargs) for v in value]

        if isinstance(value, dict):
            return {k: self._build(v, **kwargs) for k, v in value.items()}

        raise MinosMalformedAttributeException(f"Given type is not supported: {type(value)!r} ({value!r})")

    def _build_model(self, model: Model, **kwargs) -> Any:
        raw = {name: self._build_field(field, **kwargs) for name, field in model.fields.items()}

        if (ans := model.encode_data(self, raw, **kwargs)) is not MissingSentinel:
            return ans

        return self._build(raw, **kwargs)

    def _build_field(self, field: Field, **kwargs) -> Any:
        return self._build(field.value, **kwargs)

    @staticmethod
    def _build_date(value: date, **kwargs) -> int:
        return (value - date(1970, 1, 1)).days

    @staticmethod
    def _build_time(value: time, **kwargs) -> int:
        return (datetime.combine(date(1, 1, 1), value) - datetime(1, 1, 1)) // timedelta(microseconds=1)

    @staticmethod
    def _build_datetime(value: datetime, **kwargs) -> int:
        return (value.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)

    @staticmethod
    def _build_timedelta(value: timedelta, **kwargs) -> int:
        return value // timedelta(microseconds=1)

    @staticmethod
    def _build_uuid(value: UUID, **kwargs) -> str:
        return str(value)
