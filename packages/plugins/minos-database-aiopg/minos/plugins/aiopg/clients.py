from __future__ import (
    annotations,
)

import logging
from asyncio import (
    TimeoutError,
)
from collections.abc import (
    AsyncIterator,
    Iterable,
)
from functools import (
    partial,
)
from typing import (
    Optional,
)

import aiopg
from aiopg import (
    Connection,
    Cursor,
)
from psycopg2 import (
    IntegrityError,
    OperationalError,
    ProgrammingError,
)

from minos.common import (
    CircuitBreakerMixin,
    ConnectionException,
    DatabaseClient,
    IntegrityException,
    ProgrammingException,
)

from .operations import (
    AiopgDatabaseOperation,
)

logger = logging.getLogger(__name__)


class AiopgDatabaseClient(DatabaseClient, CircuitBreakerMixin):
    """Aiopg Database Client class."""

    _connection: Optional[Connection]
    _cursor: Optional[Cursor]

    def __init__(
        self,
        database: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        circuit_breaker_exceptions: Iterable[type] = tuple(),
        connection_timeout: Optional[float] = None,
        cursor_timeout: Optional[float] = None,
        *args,
        **kwargs,
    ):
        super().__init__(
            *args,
            **kwargs,
            circuit_breaker_exceptions=(ConnectionException, *circuit_breaker_exceptions),
        )

        if host is None:
            host = "localhost"
        if port is None:
            port = 5432
        if user is None:
            user = "postgres"
        if password is None:
            password = ""
        if connection_timeout is None:
            connection_timeout = 1
        if cursor_timeout is None:
            cursor_timeout = 60

        self._database = database
        self._host = host
        self._port = port
        self._user = user
        self._password = password

        self._connection_timeout = connection_timeout
        self._cursor_timeout = cursor_timeout

        self._connection = None
        self._cursor = None

    async def _setup(self) -> None:
        await super()._setup()
        await self.recreate()

    async def _destroy(self) -> None:
        await super()._destroy()
        await self.close()

    async def recreate(self) -> None:
        """Recreate the database connection.

        :return: This method does not return anything.
        """
        await self.close()

        self._connection = await self.with_circuit_breaker(self._connect)
        logger.debug(f"Created {self.database!r} database connection identified by {id(self._connection)}!")

    async def _connect(self) -> Connection:
        try:
            return await aiopg.connect(
                timeout=self._connection_timeout,
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
            )
        except (OperationalError, TimeoutError) as exc:
            raise ConnectionException(f"There was not possible to connect to the database: {exc!r}")

    async def close(self) -> None:
        """Close database connection.

        :return: This method does not return anything.
        """
        if await self.is_connected():
            await self._connection.close()

        if self._connection is not None:
            logger.debug(f"Destroyed {self.database!r} database connection identified by {id(self._connection)}!")
            self._connection = None

    async def is_connected(self) -> bool:
        """Check if the client is connected.

        :return: ``True`` if it is connected or ``False`` otherwise.
        """
        if self._connection is None:
            return False

        try:
            # This operation connects to the database and raises an exception if something goes wrong.
            self._connection.isolation_level
        except OperationalError:
            return False

        return not self._connection.closed

    async def _reset(self, **kwargs) -> None:
        await self._destroy_cursor(**kwargs)

    # noinspection PyUnusedLocal
    async def _fetch_all(self) -> AsyncIterator[tuple]:
        if self._cursor is None:
            raise ProgrammingException("An operation must be executed before fetching any value.")

        try:
            async for row in self._cursor:
                yield row
        except ProgrammingError as exc:
            raise ProgrammingException(str(exc))
        except OperationalError as exc:
            raise ConnectionException(f"There was not possible to connect to the database: {exc!r}")

    # noinspection PyUnusedLocal
    async def _execute(self, operation: AiopgDatabaseOperation) -> None:
        if not isinstance(operation, AiopgDatabaseOperation):
            raise ValueError(f"The operation must be a {AiopgDatabaseOperation!r} instance. Obtained: {operation!r}")

        fn = partial(self._execute_cursor, operation=operation.query, parameters=operation.parameters)
        await self.with_circuit_breaker(fn)

    async def _execute_cursor(self, operation: str, parameters: dict):
        if not await self.is_connected():
            await self.recreate()

        self._cursor = await self._connection.cursor(timeout=self._cursor_timeout)
        try:
            await self._cursor.execute(operation=operation, parameters=parameters)
        except OperationalError as exc:
            raise ConnectionException(f"There was not possible to connect to the database: {exc!r}")
        except IntegrityError as exc:
            raise IntegrityException(f"The requested operation raised a integrity error: {exc!r}")

    async def _destroy_cursor(self, **kwargs):
        if self._cursor is not None:
            if not self._cursor.closed:
                self._cursor.close()
            self._cursor = None

    @property
    def cursor(self) -> Optional[Cursor]:
        """Get the cursor.

        :return: A ``Cursor`` instance.
        """
        return self._cursor

    @property
    def connection(self) -> Optional[Connection]:
        """Get the connection.

        :return: A ``Connection`` instance.
        """
        return self._connection

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
