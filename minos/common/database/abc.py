"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    ABC,
)
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    NoReturn,
    Optional,
)

from aiopg import (
    Cursor,
)
from dependency_injector.wiring import (
    Provide,
)

from ..setup import (
    MinosSetup,
)
from .pool import (
    PostgreSqlPool,
)


class PostgreSqlMinosDatabase(ABC, MinosSetup):
    """PostgreSql Minos Database base class."""

    _pool: Optional[PostgreSqlPool] = Provide["postgresql_pool"]

    def __init__(self, host: str, port: int, database: str, user: str, password: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        self._owned_pool = False

    async def _destroy(self) -> NoReturn:
        if self._owned_pool:
            await self._pool.destroy()
            self._pool = None
            self._owned_pool = False

    async def submit_query_and_fetchone(self, *args, **kwargs) -> tuple:
        """Submit a SQL query and gets the first response.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        return await self.submit_query_and_iter(*args, **kwargs).__anext__()

    # noinspection PyUnusedLocal
    async def submit_query_and_iter(
        self,
        operation: Any,
        parameters: Any = None,
        *,
        timeout: Optional[float] = None,
        lock: Optional[int] = None,
        streaming_mode: bool = False,
        **kwargs,
    ) -> AsyncIterator[tuple]:
        """Submit a SQL query and return an asynchronous iterator.

        :param operation: Query to be executed.
        :param parameters: Parameters to be projected into the query.
        :param timeout: An optional timeout.
        :param lock: Optional key to perform the query with locking. If not set, the query is performed without any
            lock.
        :param streaming_mode: If ``True`` the data fetching is performed in streaming mode, that is iterating over the
            cursor and yielding once a time (requires an opening connection to do that). Otherwise, all the data is
            fetched and keep in memory before yielding it.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        async with self.cursor() as cursor:
            if lock is not None:
                await cursor.execute("select pg_advisory_lock(%s)", (int(lock),))
            try:
                await cursor.execute(operation=operation, parameters=parameters, timeout=timeout)

                if streaming_mode:
                    async for row in cursor:
                        yield row
                    return

                rows = await cursor.fetchall()
            finally:
                if lock is not None:
                    await cursor.execute("select pg_advisory_unlock(%s)", (int(lock),))

        for row in rows:
            yield row

    # noinspection PyUnusedLocal
    async def submit_query(
        self,
        operation: Any,
        parameters: Any = None,
        *,
        timeout: Optional[float] = None,
        lock: Optional[int] = None,
        **kwargs,
    ) -> NoReturn:
        """Submit a SQL query.

        :param operation: Query to be executed.
        :param parameters: Parameters to be projected into the query.
        :param timeout: An optional timeout.
        :param lock: Optional key to perform the query with locking. If not set, the query is performed without any
            lock.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        async with self.cursor() as cursor:
            if lock is not None:
                await cursor.execute("select pg_advisory_lock(%s)", (int(lock),))
            try:
                await cursor.execute(operation=operation, parameters=parameters, timeout=timeout)
            finally:
                if lock is not None:
                    await cursor.execute("select pg_advisory_unlock(%s)", (int(lock),))

    def cursor(self, *args, **kwargs) -> AsyncContextManager[Cursor]:
        """Get a connection cursor from the pool.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A ``Cursor`` instance wrapped inside a context manager.
        """
        return self.pool.cursor(*args, **kwargs)

    @property
    def pool(self) -> PostgreSqlPool:
        """Get the connections pool.

        :return: A ``Pool`` object.
        """
        if self._pool is None or isinstance(self._pool, Provide):
            self._pool = PostgreSqlPool(
                host=self.host, port=self.port, database=self.database, user=self.user, password=self.password,
            )
            self._owned_pool = True
        return self._pool
