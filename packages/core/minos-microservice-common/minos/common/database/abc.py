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
        postgresql_pool: Optional[PostgreSqlPool] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs, pool_factory=pool_factory)
        if database_pool is None and pool_factory is not None:
            database_pool = pool_factory.get_pool("database")

        if database_pool is None and postgresql_pool is not None:
            warnings.warn("'postgresql_pool' argument has been deprecated", DeprecationWarning)
            database_pool = postgresql_pool

        if not isinstance(database_pool, DatabaseClientPool):
            raise NotProvidedException(f"A {DatabaseClientPool!r} instance is required. Obtained: {database_pool}")

        self._pool = database_pool

    async def submit_query_and_fetchone(self, *args, **kwargs) -> tuple:
        """Submit a SQL query and gets the first response.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        async with self.pool.acquire() as client:
            return await client.submit_query_and_fetchone(*args, **kwargs)

    # noinspection PyUnusedLocal
    async def submit_query_and_iter(self, *args, **kwargs) -> AsyncIterator[tuple]:
        """Submit a SQL query and return an asynchronous iterator.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        async with self.pool.acquire() as client:
            async for value in client.submit_query_and_iter(*args, **kwargs):
                yield value

    # noinspection PyUnusedLocal
    async def submit_query(self, *args, **kwargs) -> None:
        """Submit a SQL query.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        async with self.pool.acquire() as client:
            return await client.submit_query(*args, **kwargs)

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
