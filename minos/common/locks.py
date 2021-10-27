from abc import (
    ABC,
)
from collections.abc import (
    Hashable,
)

from cached_property import (
    cached_property,
)


class Lock(ABC):
    """"Lock base class."""

    key: Hashable

    def __new__(cls, *args, **kwargs):
        if cls is Lock:
            raise TypeError(f"Abstract class {cls.__name__} cannot be instantiated")
        return super().__new__(cls)

    def __init__(self, key: Hashable, *args, **kwargs):
        if not isinstance(key, Hashable):
            raise ValueError(f"The key must be hashable. Obtained: {key!r} ({type(key)})")

        self.key = key

    @cached_property
    def hashed_key(self) -> int:
        """Get the hashed key.

        :return: An integer value.
        """
        if not isinstance(self.key, int):
            return hash(self.key)
        return self.key
