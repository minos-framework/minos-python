from typing import (
    AsyncIterator,
    Generic,
    Optional,
    TypeVar,
    get_args, get_origin,
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
    DatabaseOperationFactory,
)
from .pools import (
    DatabaseClient,
    DatabaseClientPool,
)

GenericDatabaseOperationFactory = TypeVar("GenericDatabaseOperationFactory", bound=DatabaseOperationFactory)


class DatabaseMixin(SetupMixin, Generic[GenericDatabaseOperationFactory]):
    """Database Mixin class."""

    @Inject()
    def __init__(
        self,
        database_pool: Optional[DatabaseClientPool] = None,
        pool_factory: Optional[PoolFactory] = None,
        database_key: Optional[str] = None,
        operation_factory: Optional[GenericDatabaseOperationFactory] = None,
        operation_factory_cls: Optional[type[GenericDatabaseOperationFactory]] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs, pool_factory=pool_factory)
        if database_pool is None and pool_factory is not None:
            database_pool = pool_factory.get_pool(type_="database", identifier=database_key)

        if not isinstance(database_pool, DatabaseClientPool):
            raise NotProvidedException(f"A {DatabaseClientPool!r} instance is required. Obtained: {database_pool}")

        self._pool = database_pool

        if operation_factory is None:
            if operation_factory_cls is None:
                operation_factory_cls = self._get_generic_operation_factory()
            if operation_factory_cls is not None:
                operation_factory = self.pool_instance_cls.get_factory(operation_factory_cls)

        self._operation_factory = operation_factory

    @property
    def operation_factory(self) -> Optional[GenericDatabaseOperationFactory]:
        """TODO

        :return: TODO
        """
        return self._operation_factory

    def _get_generic_operation_factory(self) -> Optional[type[GenericDatabaseOperationFactory]]:
        operation_factory_cls = None
        # noinspection PyUnresolvedReferences
        for base in self.__orig_bases__:
            origin = get_origin(base)
            if origin is None or not issubclass(origin, DatabaseMixin):
                continue
            args = get_args(base)
            if not len(args):
                continue
            operation_factory_cls = args[0]
            if not isinstance(operation_factory_cls, type) or not issubclass(
                operation_factory_cls, DatabaseOperationFactory
            ):
                raise TypeError(f"{type(self)!r} must contain a {DatabaseOperationFactory!r} as generic value.")
        return operation_factory_cls

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
    def pool_instance_cls(self) -> type[DatabaseClient]:
        """TODO

        :return: TODO
        """
        return self.pool.instance_cls

    @property
    def pool(self) -> DatabaseClientPool:
        """Get the connections pool.

        :return: A ``Pool`` object.
        """
        return self._pool
