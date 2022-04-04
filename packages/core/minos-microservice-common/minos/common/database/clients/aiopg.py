import logging
from collections.abc import (
    AsyncIterator,
    Hashable,
)
from typing import (
    Any,
    Optional,
)

import aiopg
from aiomisc.pool import (
    AsyncContextManager,
    ContextManager,
)
from aiopg import (
    Connection,
    Cursor,
)
from psycopg2 import (
    OperationalError,
)

from .abc import (
    DatabaseClient,
    UnableToConnectException,
)

logger = logging.getLogger(__name__)


class AiopgDatabaseClient(DatabaseClient):
    """TODO"""

    def __init__(
        self,
        database: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if host is None:
            host = "localhost"
        if port is None:
            port = 5432
        if user is None:
            user = "postgres"
        if password is None:
            password = ""

        self._database = database
        self._host = host
        self._port = port
        self._user = user
        self._password = password

        self._connection = None
        self._cursor = None
        self._cursor_wrapper = None

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_connection()

    async def _destroy(self) -> None:
        await self._close_connection()
        await super()._destroy()

    async def _create_connection(self):
        try:
            self._connection = await aiopg.connect(
                host=self.host, port=self.port, dbname=self.database, user=self.user, password=self.password
            )
        except OperationalError as exc:
            logger.warning(f"There was an {exc!r} while trying to get a database connection.")
            raise UnableToConnectException

        logger.debug(f"Created {self.database!r} database connection identified by {id(self._connection)}!")

    async def _close_connection(self):
        if self._connection is not None and not self._connection.closed:
            await self._connection.close()
        self._connection = None
        logger.debug(f"Destroyed {self.database!r} database connection identified by {id(self._connection)}!")

    async def is_valid(self):
        """TODO"""
        if self._connection is None:
            return False

        try:
            # This operation connects to the database and raises an exception if something goes wrong.
            self._connection.isolation_level
        except OperationalError:
            return False

        return not self._connection.closed

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
        self, operation: Any, parameters: Any = None, *, timeout: Optional[float] = None, lock: Any = None, **kwargs
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
        from ..locks import (
            DatabaseLock,
        )

        lock = DatabaseLock(self, key, *args, **kwargs)
        context_manager = self.cursor()

        async def _fn_enter():
            await lock.__aenter__()
            return await context_manager.__aenter__()

        async def _fn_exit(_):
            await context_manager.__aexit__(None, None, None)
            await lock.__aexit__(None, None, None)

        return ContextManager(_fn_enter, _fn_exit)

    def cursor(self, *args, **kwargs) -> AsyncContextManager[Cursor]:
        """Get a new cursor.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A Cursor wrapped into an asynchronous context manager.
        """
        connection: Connection = self._connection

        async def _fn_enter():
            cursor = await connection.cursor(*args, **kwargs).__aenter__()
            return cursor

        async def _fn_exit(cursor: Cursor):
            if not cursor.closed:
                cursor.close()

        return ContextManager(_fn_enter, _fn_exit)

    @property
    def database(self) -> str:
        """Get the database's database.

        :return: A ``str`` value.
        """
        return self._database

    @property
    def host(self) -> str:
        """Get the database's host.

        :return: A ``str`` value.
        """
        return self._host

    @property
    def port(self) -> int:
        """Get the database's port.

        :return: An ``int`` value.
        """
        return self._port

    @property
    def user(self) -> str:
        """Get the database's user.

        :return: A ``str`` value.
        """
        return self._user

    @property
    def password(self) -> str:
        """Get the database's password.

        :return: A ``str`` value.
        """
        return self._password
