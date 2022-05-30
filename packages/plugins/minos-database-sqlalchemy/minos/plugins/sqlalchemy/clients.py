import logging
from collections.abc import (
    Callable,
)
from typing import (
    Any,
    AsyncIterator,
    Optional,
)

from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncResult,
    create_async_engine,
)

from minos.common import (
    DatabaseClient,
    ProgrammingException,
)

from .operations import (
    SqlAlchemyDatabaseOperation,
)

logger = logging.getLogger(__name__)


class SqlAlchemyDatabaseClient(DatabaseClient):
    """TODO"""

    _engine: AsyncEngine
    _connection: Optional[AsyncConnection]
    _result: Optional[AsyncResult]

    def __init__(self, url: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._url = url

        self._engine = create_async_engine(self._url, pool_size=1)
        self._connection = None
        self._result = None

    async def _setup(self) -> None:
        await super()._setup()
        await self.recreate()

    async def _destroy(self) -> None:
        await super()._destroy()
        await self.close()

    async def _reset(self, **kwargs) -> None:
        self._result = None

    async def _execute(self, operation: SqlAlchemyDatabaseOperation) -> None:
        if not isinstance(operation, SqlAlchemyDatabaseOperation):
            raise ValueError(
                f"The operation must be a {SqlAlchemyDatabaseOperation!r} instance. Obtained: {operation!r}"
            )

        if not await self.is_connected():
            await self.recreate()

        if isinstance(operation.expression, Callable):
            await self._connection.run_sync(operation.expression)
        else:
            self._result = await self._connection.stream(operation.expression)

    async def _fetch_all(self, *args, **kwargs) -> AsyncIterator[Any]:
        if self._result is None:
            raise ProgrammingException("An operation must be executed before fetching any value.")

        async for row in self._result:
            yield row

        self._result = None

    async def recreate(self):
        await self.close()
        self._connection = await self._engine.connect()

    async def close(self) -> None:
        if await self.is_connected():
            await self._connection.close()

        if self._connection is not None:
            self._connection = None

    async def is_connected(self) -> bool:
        """Check if the client is connected.

        :return: ``True`` if it is connected or ``False`` otherwise.
        """
        if self._connection is None:
            return False

        return not self._connection.closed
