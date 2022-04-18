from __future__ import (
    annotations,
)

import logging
from collections.abc import (
    AsyncIterator,
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
    ConnectionException,
    DatabaseClient,
    IntegrityException,
    ProgrammingException,
)

from .operations import (
    AiopgDatabaseOperation,
)

logger = logging.getLogger(__name__)


class AiopgDatabaseClient(DatabaseClient):
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

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_connection()

    async def _destroy(self) -> None:
        await super()._destroy()
        await self._close_connection()

    async def _create_connection(self):
        try:
            self._connection = await aiopg.connect(
                host=self.host, port=self.port, dbname=self.database, user=self.user, password=self.password
            )
        except OperationalError as exc:
            msg = f"There was an {exc!r} while trying to get a database connection."
            logger.warning(msg)
            raise ConnectionException(msg)

        logger.debug(f"Created {self.database!r} database connection identified by {id(self._connection)}!")

    async def _close_connection(self):
        if self._connection is not None and not self._connection.closed:
            await self._connection.close()
        self._connection = None
        logger.debug(f"Destroyed {self.database!r} database connection identified by {id(self._connection)}!")

    async def _is_valid(self) -> bool:
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
        await self._create_cursor()
        try:
            async for row in self._cursor:
                yield row
        except ProgrammingError as exc:
            raise ProgrammingException(str(exc))

    # noinspection PyUnusedLocal
    async def _execute(self, operation: AiopgDatabaseOperation) -> None:
        if not isinstance(operation, AiopgDatabaseOperation):
            raise ValueError(f"The operation must be a {AiopgDatabaseOperation!r} instance. Obtained: {operation!r}")

        await self._create_cursor()
        try:
            await self._cursor.execute(operation=operation.query, parameters=operation.parameters)
        except IntegrityError as exc:
            raise IntegrityException(f"The requested operation raised a integrity error: {exc!r}")

    async def _create_cursor(self):
        if self._cursor is None:
            self._cursor = await self._connection.cursor()

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
