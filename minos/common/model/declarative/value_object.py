from typing import (
    Any,
)

from ...exceptions import (
    MinosImmutableClassException,
)
from . import (
    DeclarativeModel,
)


class ValueObject(DeclarativeModel):
    def __setattr__(self, key: str, value: Any):
        if key.startswith("_"):
            super().__setattr__(key, value)
        else:
            raise MinosImmutableClassException("modification of an immutable value object not allowed")
