from __future__ import (
    annotations,
)

from abc import (
    ABC,
)
from collections.abc import (
    Hashable,
)
from contextlib import (
    AbstractAsyncContextManager,
)

from cached_property import (
    cached_property,
)

from .injections import (
    Injectable,
)
from .pools import (
    Pool,
)


class Lock(AbstractAsyncContextManager):
    """Lock base class."""

    key: Hashable

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


@Injectable("lock_pool")
class LockPool(Pool[Lock], ABC):
    """Postgres Locking Pool class."""
