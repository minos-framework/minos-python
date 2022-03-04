from __future__ import (
    annotations,
)

from typing import (
    Any,
    TypeVar,
)

from minos.common import (
    DeclarativeModel,
    Model,
)

from .collections import (
    IncrementalSet,
)
from .exceptions import (
    ValueObjectException,
)


class ValueObject(DeclarativeModel):
    """Value Object class."""

    def __setitem__(self, key: str, value: Any) -> None:
        raise ValueObjectException("modification of an immutable value object not allowed")


T = TypeVar("T", bound=Model)


class ValueObjectSet(IncrementalSet[T]):
    """Value Object Set class."""

    data: set[T]
