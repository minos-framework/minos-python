from collections.abc import (
    Hashable,
)
from typing import (
    AsyncContextManager,
    Optional,
)

from aiopg import (
    Connection,
    Cursor,
)

from ..locks import (
    Lock,
)


class PostgreSqlLock(Lock):
    """"PostgreSql Lock class."""

    cursor: Optional[Cursor]

    def __init__(self, wrapped_connection: AsyncContextManager[Connection], key: Hashable, *args, **kwargs):
        super().__init__(key, *args, **kwargs)

        self.wrapped_connection = wrapped_connection
        self.cursor = None

        self._args = args
        self._kwargs = kwargs

    async def __aenter__(self):
        connection = await self.wrapped_connection.__aenter__()
        cursor = await connection.cursor(*self._args, **self._kwargs).__aenter__()

        self.cursor = cursor
        await self.cursor.execute("select pg_advisory_lock(%(hashed_key)s)", {"hashed_key": self.hashed_key})
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cursor.execute("select pg_advisory_unlock(%(hashed_key)s)", {"hashed_key": self.hashed_key})
        if not self.cursor.closed:
            self.cursor.close()
        self.cursor = None
        await self.wrapped_connection.__aexit__(exc_type, exc_val, exc_tb)
