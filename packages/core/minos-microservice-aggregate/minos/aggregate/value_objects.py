from __future__ import (
    annotations,
)

from typing import (
    Any,
    TypeVar,
)

from minos.aggregate.collections import (
    IncrementalSet,
)
from minos.aggregate.exceptions import (
    ValueObjectException,
)
from minos.common import (
    DeclarativeModel,
    Model,
)


class ValueObject(DeclarativeModel):
    """Value Object class."""

    def __setattr__(self, key: str, value: Any):
        if key.startswith("_"):
            super().__setattr__(key, value)
        else:
            raise ValueObjectException("modification of an immutable value object not allowed")


T = TypeVar("T", bound=Model)


class ValueObjectSet(IncrementalSet[T]):
    """Value Object Set class."""

    data: set[T]
