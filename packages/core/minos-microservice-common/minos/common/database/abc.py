import warnings
from typing import (
    AsyncIterator,
    Optional,
)

from ..exceptions import (
    NotProvidedException,
)
from ..injections import (
    Inject,
)
from ..pools import (
    PoolFactory,
)
from ..setup import (
    SetupMixin,
)
from .operations import (
    DatabaseOperation,
)
from .pools import (
    DatabaseClientPool,
    PostgreSqlPool,
)


class DatabaseMixin(SetupMixin):
    """PostgreSql Minos Database base class."""

    @Inject()
    def __init__(
        self,
        database_pool: Optional[DatabaseClientPool] = None,
        pool_factory: Optional[PoolFactory] = None,
        database_key: Optional[str] = None,
        postgresql_pool: Optional[PostgreSqlPool] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs, pool_factory=pool_factory)
        if database_pool is None and pool_factory is not None:
            database_pool = pool_factory.get_pool(type_="database", identifier=database_key)

        if database_pool is None and postgresql_pool is not None:
            warnings.warn("'postgresql_pool' argument has been deprecated", DeprecationWarning)
            database_pool = postgresql_pool

        if not isinstance(database_pool, DatabaseClientPool):
            raise NotProvidedException(f"A {DatabaseClientPool!r} instance is required. Obtained: {database_pool}")

        self._pool = database_pool

    async def submit_query_and_fetchone(self, operation: DatabaseOperation) -> tuple:
        """Submit a SQL query and gets the first response.

        :param operation: TODO
        :return: This method does not return anything.
        """
        async with self.pool.acquire() as client:
            await client.execute(operation)
            return await client.fetch_one()

    # noinspection PyUnusedLocal
    async def submit_query_and_iter(
        self, operation: DatabaseOperation, streaming_mode: Optional[bool] = None
    ) -> AsyncIterator[tuple]:
        """Submit a SQL query and return an asynchronous iterator.

        :param operation: TODO
        :param streaming_mode: If ``True`` return the values in streaming directly from the database (keep an open
            database connection), otherwise preloads the full set of values on memory and then retrieves them.
        :return: This method does not return anything.
        """
        if streaming_mode is None:
            streaming_mode = False

        async with self.pool.acquire() as client:
            await client.execute(operation)
            async_iterable = client.fetch_all()
            if streaming_mode:
                async for value in async_iterable:
                    yield value
                return

            iterable = [value async for value in async_iterable]

        for value in iterable:
            yield value

    # noinspection PyUnusedLocal
    async def submit_query(self, operation: DatabaseOperation) -> None:
        """Submit a SQL query.

        :param operation: TODO
        :return: This method does not return anything.
        """
        async with self.pool.acquire() as client:
            return await client.execute(operation)

    @property
    def pool(self) -> DatabaseClientPool:
        """Get the connections pool.

        :return: A ``Pool`` object.
        """
        return self._pool


class PostgreSqlMinosDatabase(DatabaseMixin):
    """PostgreSql Minos Database class."""

    def __init__(self, *args, **kwargs):
        warnings.warn(
            f"{PostgreSqlMinosDatabase!r} has been deprecated. Use {DatabaseMixin} instead.", DeprecationWarning
        )
        super().__init__(*args, **kwargs)
