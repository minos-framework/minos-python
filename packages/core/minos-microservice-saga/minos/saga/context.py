from collections.abc import (
    MutableMapping,
)
from typing import (
    Any,
)

from minos.common import (
    BucketModel,
    Field,
)


class SagaContext(BucketModel, MutableMapping):
    """Saga Context class

    The purpose of this class is to keep an execution state.
    """

    def __init__(self, **kwargs):
        if "fields" not in kwargs:
            fields = {name: Field(name, Any, value) for name, value in kwargs.items()}
        else:
            fields = {name: Field(field.name, Any, field.value) for name, field in kwargs["fields"].items()}

        super().__init__(fields=fields)

    def __setitem__(self, key: str, value: Any) -> None:
        try:
            super().__setitem__(key, value)
        except KeyError:
            self._fields[key] = Field(key, Any, value)

    def __delitem__(self, item: str) -> None:
        try:
            del self._fields[item]
        except KeyError:
            raise KeyError(f"{type(self).__name__!r} does not contain the {item!r} field")

    def __setattr__(self, key: str, value: Any) -> None:
        try:
            super().__setattr__(key, value)
        except AttributeError:
            self[key] = value

    def __delattr__(self, item: str) -> None:
        if item.startswith("_"):
            super().__delattr__(item)
            return

        try:
            del self[item]
        except KeyError as exc:
            raise AttributeError(str(exc))
