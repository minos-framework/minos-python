from collections.abc import (
    Hashable,
)
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Optional,
)

from aiomisc.pool import (
    ContextManager,
)
from aiopg import (
    Cursor,
)
from dependency_injector.wiring import (
    Provide,
    inject,
)

from ..setup import (
    MinosSetup,
)
from .locks import (
    PostgreSqlLock,
)
from .pools import (
    PostgreSqlPool,
)


class PostgreSqlMinosDatabase(MinosSetup):
    """PostgreSql Minos Database base class."""

    def __init__(self, host: str, port: int, database: str, user: str, password: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        self._pool = None
        self._owned_pool = False

    async def _destroy(self) -> None:
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
        if lock is None:
            context_manager = self.cursor()
        else:
            context_manager = self.locked_cursor(lock)

        async with context_manager as cursor:
            await cursor.execute(operation=operation, parameters=parameters, timeout=timeout)

            if streaming_mode:
                async for row in cursor:
                    yield row
                return

            rows = await cursor.fetchall()

        for row in rows:
            yield row

    # noinspection PyUnusedLocal
    async def submit_query(
        self, operation: Any, parameters: Any = None, *, timeout: Optional[float] = None, lock: Any = None, **kwargs,
    ) -> None:
        """Submit a SQL query.

        :param operation: Query to be executed.
        :param parameters: Parameters to be projected into the query.
        :param timeout: An optional timeout.
        :param lock: Optional key to perform the query with locking. If not set, the query is performed without any
            lock.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        if lock is None:
            context_manager = self.cursor()
        else:
            context_manager = self.locked_cursor(lock)

        async with context_manager as cursor:
            await cursor.execute(operation=operation, parameters=parameters, timeout=timeout)

    def locked_cursor(self, key: Hashable, *args, **kwargs) -> AsyncContextManager[Cursor]:
        """Get a new locked cursor.

        :param key: The key to be used for locking.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A Cursor wrapped into an asynchronous context manager.
        """
        lock = PostgreSqlLock(self.pool.acquire(), key, *args, **kwargs)

        async def _fn_enter():
            await lock.__aenter__()
            return lock.cursor

        async def _fn_exit(_):
            await lock.__aexit__(None, None, None)

        return ContextManager(_fn_enter, _fn_exit)

    def cursor(self, *args, **kwargs) -> AsyncContextManager[Cursor]:
        """Get a new cursor.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A Cursor wrapped into an asynchronous context manager.
        """
        acquired = self.pool.acquire()

        async def _fn_enter():
            connection = await acquired.__aenter__()
            cursor = await connection.cursor(*args, **kwargs).__aenter__()
            return cursor

        async def _fn_exit(cursor: Cursor):
            if not cursor.closed:
                cursor.close()
            await acquired.__aexit__(None, None, None)

        return ContextManager(_fn_enter, _fn_exit)

    @property
    def pool(self) -> PostgreSqlPool:
        """Get the connections pool.

        :return: A ``Pool`` object.
        """
        if self._pool is None:
            self._pool, self._owned_pool = self._build_pool()
        return self._pool

    @inject
    def _build_pool(self, pool: PostgreSqlPool = Provide["postgresql_pool"]) -> tuple[PostgreSqlPool, bool]:
        if not isinstance(pool, Provide):
            return pool, False

        pool = PostgreSqlPool(
            host=self.host, port=self.port, database=self.database, user=self.user, password=self.password,
        )
        return pool, True
