from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    Hashable,
)

from cached_property import (
    cached_property,
)

from .pools import (
    Pool,
)


class Lock(ABC):
    """Lock base class."""

    key: Hashable

    def __init__(self, key: Hashable, *args, **kwargs):
        if not isinstance(key, Hashable):
            raise ValueError(f"The key must be hashable. Obtained: {key!r} ({type(key)})")

        self.key = key

    async def __aenter__(self) -> Lock:
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()

    @abstractmethod
    async def acquire(self) -> None:
        """Acquire the lock.

        :return: This method does not return anything.
        """

    @abstractmethod
    async def release(self):
        """Release the lock.

        :return: This method does not return anything.
        """

    @cached_property
    def hashed_key(self) -> int:
        """Get the hashed key.

        :return: An integer value.
        """
        if not isinstance(self.key, int):
            return hash(self.key)
        return self.key


class LockPool(Pool[Lock], ABC):
    """Lock Pool class."""
