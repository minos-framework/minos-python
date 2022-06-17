import logging
from asyncio import (
    TimeoutError,
    wait_for,
)
from collections.abc import (
    Callable,
    Iterable,
)
from functools import (
    partial,
)
from typing import (
    Any,
    AsyncIterator,
    Optional,
)

from sqlalchemy.engine import (
    URL,
)
from sqlalchemy.exc import (
    IntegrityError,
    OperationalError,
    ProgrammingError,
)
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncResult,
    create_async_engine,
)

from minos.common import (
    CircuitBreakerMixin,
    ConnectionException,
    DatabaseClient,
    IntegrityException,
    ProgrammingException,
)

from .operations import (
    SqlAlchemyDatabaseOperation,
)

logger = logging.getLogger(__name__)


class SqlAlchemyDatabaseClient(DatabaseClient, CircuitBreakerMixin):
    """SqlAlchemy Database Client class."""

    _engine: AsyncEngine
    _connection: Optional[AsyncConnection]
    _result: Optional[AsyncResult]

    def __init__(
        self,
        *args,
        circuit_breaker_exceptions: Iterable[type] = tuple(),
        connection_timeout: Optional[float] = None,
        result_timeout: Optional[float] = None,
        **kwargs,
    ):
        super().__init__(
            *args,
            **kwargs,
            circuit_breaker_exceptions=(ConnectionException, *circuit_breaker_exceptions),
        )
        if connection_timeout is None:
            connection_timeout = 1
        if result_timeout is None:
            result_timeout = 60

        url = URL.create(
            drivername=kwargs.get("driver"),
            username=kwargs.get("user"),
            password=kwargs.get("password"),
            host=kwargs.get("host"),
            port=kwargs.get("port"),
            database=kwargs.get("database"),
        )
        self._engine = create_async_engine(url, pool_size=1)
        self._connection_timeout = connection_timeout
        self._result_timeout = result_timeout

        self._connection = None
        self._result = None

    async def _setup(self) -> None:
        await super()._setup()
        await self.recreate()

    async def _destroy(self) -> None:
        await super()._destroy()
        await self.close()

    async def _reset(self, **kwargs) -> None:
        await self._destroy_result()

    async def _execute(self, operation: SqlAlchemyDatabaseOperation) -> None:
        if not isinstance(operation, SqlAlchemyDatabaseOperation):
            raise ValueError(
                f"The operation must be a {SqlAlchemyDatabaseOperation!r} instance. Obtained: {operation!r}"
            )

        fn = partial(self._execute_with_timeout, operation=operation)
        await self.with_circuit_breaker(fn)

    async def _execute_with_timeout(self, operation: SqlAlchemyDatabaseOperation) -> None:
        if not await self.is_connected():
            await self.recreate()
        try:
            self._result = await wait_for(self._execute_operation(operation), self._result_timeout)
        except ProgrammingError as exc:
            raise ProgrammingException(str(exc))
        except (OperationalError, TimeoutError) as exc:
            raise ConnectionException(f"There was not possible to connect to the database: {exc!r}")
        except IntegrityError as exc:
            raise IntegrityException(f"The requested operation raised a integrity error: {exc!r}")

    async def _execute_operation(self, operation: SqlAlchemyDatabaseOperation) -> Optional[AsyncResult]:
        if isinstance(operation.statement, Callable):
            await self._connection.run_sync(operation.statement)
            return None

        if not operation.stream:
            await self._connection.execute(statement=operation.statement, parameters=operation.parameters)
            return None

        return await self._connection.stream(statement=operation.statement, parameters=operation.parameters)

    async def _fetch_all(self, *args, **kwargs) -> AsyncIterator[Any]:
        if self._result is None:
            raise ProgrammingException("An operation must be executed before fetching any value.")

        try:
            async for row in self._result:
                yield row
        except ProgrammingError as exc:
            raise ProgrammingException(str(exc))
        except OperationalError as exc:
            raise ConnectionException(f"There was not possible to connect to the database: {exc!r}")

    async def recreate(self):
        """Recreate the database connection.

        :return: This method does not return anything.
        """
        await self.close()

        self._connection = await self.with_circuit_breaker(self._connect)

        logger.debug(f"Created {self.url!r} connection identified by {id(self._connection)}!")

    async def close(self) -> None:
        """Close database connection.

        :return: This method does not return anything.
        """
        await self._destroy_result()

        if await self.is_connected():
            await self._connection.close()

        if self._connection is not None:
            logger.debug(f"Destroyed {self.url!r} connection identified by {id(self._connection)}!")
            self._connection = None

        await self.engine.dispose()

    async def _destroy_result(self) -> None:
        self._result = None

    async def is_connected(self) -> bool:
        """Check if the client is connected.

        :return: ``True`` if it is connected or ``False`` otherwise.
        """
        if self._connection is None:
            return False

        return not self._connection.closed

    async def _connect(self) -> AsyncConnection:
        try:
            return await wait_for(self._engine.connect(), self._connection_timeout)
        except (OperationalError, TimeoutError) as exc:
            raise ConnectionException(f"There was not possible to connect to the database: {exc!r}")

    @property
    def url(self) -> str:
        """Get the url.

        :return: A ``str``.
        """
        return self._engine.url

    @property
    def engine(self) -> AsyncEngine:
        """Get the engine.

        :return: An ``AsyncEngine``.
        """
        return self._engine

    @property
    def connection(self) -> Optional[AsyncConnection]:
        """Get the connection if available.

        :return: An ``AsyncConnection`` or ``None``.
        """
        return self._connection

    @property
    def result(self) -> Optional[AsyncResult]:
        """Get the result if available.

        :return: An ``AsyncResult`` instance or ``None``.
        """
        return self._result
