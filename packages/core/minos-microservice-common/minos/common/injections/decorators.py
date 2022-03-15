from __future__ import (
    annotations,
)

from typing import (
    TypeVar,
)

T = TypeVar("T", bound=type)


class Injectable:
    """TODO"""

    def __init__(self, name: str):
        self._name = name

    def __call__(self, type_: T) -> T:
        for parent in type_.mro():
            if getattr(parent, "_injectable_name", None) == self._name:
                raise TypeError("TODO")

        type_._injectable_name = self._name
        return type_
