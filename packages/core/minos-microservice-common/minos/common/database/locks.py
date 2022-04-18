import warnings
from collections.abc import (
    Hashable,
)

from ..locks import (
    Lock,
)
from .clients import (
    DatabaseClient,
)


class DatabaseLock(Lock):
    """Database Lock class."""

    def __init__(self, client: DatabaseClient, key: Hashable, *args, **kwargs):
        super().__init__(key, *args, **kwargs)

        self.client = client

    async def acquire(self) -> None:
        """Acquire the lock.

        :return: This method does not return anything.
        """
        await self.client.execute("select pg_advisory_lock(%(hashed_key)s)", {"hashed_key": self.hashed_key})

    async def release(self) -> None:
        """Release the lock.

        :return: This method does not return anything.
        """
        await self.client.execute("select pg_advisory_unlock(%(hashed_key)s)", {"hashed_key": self.hashed_key})


class PostgreSqlLock(DatabaseLock):
    """PostgreSql Lock class."""

    def __init__(self, *args, **kwargs):
        warnings.warn(f"{PostgreSqlLock!r} has been deprecated. Use {DatabaseLock} instead.", DeprecationWarning)
        super().__init__(*args, **kwargs)
