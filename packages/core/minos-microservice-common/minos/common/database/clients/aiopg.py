from __future__ import (
    annotations,
)

import logging
from collections.abc import (
    AsyncIterator,
    Hashable,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

import aiopg
from aiomisc.pool import (
    AsyncContextManager,
)
from aiopg import (
    Connection,
    Cursor,
)
from aiopg.utils import ClosableQueue
from psycopg2 import (
    OperationalError,
)

from .abc import (
    DatabaseClient,
    UnableToConnectException,
)

if TYPE_CHECKING:
    from ..locks import (
        DatabaseLock,
    )

logger = logging.getLogger(__name__)


class AiopgDatabaseClient(DatabaseClient):
    """TODO"""

    _connection: Optional[Connection]
    _cursor: Optional[Cursor]
    _lock: Optional[DatabaseLock]

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

        self._lock = None
        self._cursor = None

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_connection()

    async def _destroy(self) -> None:
        await self.reset()
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

    async def reset(self, **kwargs) -> None:
        """TODO"""
        await self._destroy_cursor(**kwargs)

    @property
    def notifications(self) -> ClosableQueue:
        return self._connection.notifies

    # noinspection PyUnusedLocal
    async def fetch_all(
        self,
        *args,
        timeout: Optional[float] = None,
        lock: Optional[int] = None,
        streaming_mode: bool = False,
        **kwargs,
    ) -> AsyncIterator[tuple]:
        """Submit a SQL query and return an asynchronous iterator.

        :param timeout: An optional timeout.
        :param lock: Optional key to perform the query with locking. If not set, the query is performed without any
            lock.
        :param streaming_mode: If ``True`` the data fetching is performed in streaming mode, that is iterating over the
            cursor and yielding once a time (requires an opening connection to do that). Otherwise, all the data is
            fetched and keep in memory before yielding it.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        await self._create_cursor()

        if streaming_mode:
            async for row in self._cursor:
                yield row
            return

        rows = await self._cursor.fetchall()

        for row in rows:
            yield row

    # noinspection PyUnusedLocal
    async def execute(
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
        await self._create_cursor(lock=lock)
        await self._cursor.execute(operation=operation, parameters=parameters, timeout=timeout)

    async def _create_cursor(self, *args, lock: Optional[Hashable] = None, **kwargs):
        if self._cursor is None:
            self._cursor = await self._connection.cursor(*args, **kwargs)

        if lock is not None and self._lock is None:
            from ..locks import (
                DatabaseLock,
            )

            self._lock = DatabaseLock(self, lock, *args, **kwargs)
            await self._lock.__aenter__()

    async def _destroy_cursor(self, **kwargs):
        if self._lock is not None:
            await self._lock.__aexit__(None, None, None)
            self._lock = None

        if self._cursor is not None:
            if self._cursor.closed:
                self._cursor.close()
            self._cursor = None

    def cursor(self, *args, **kwargs) -> AsyncContextManager[Cursor]:
        """Get a new cursor.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A Cursor wrapped into an asynchronous context manager.
        """
        return self._cursor

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
