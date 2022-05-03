from collections.abc import (
    AsyncIterator,
)
from contextlib import (
    suppress,
)
from typing import (
    Any,
    Generic,
    Optional,
    TypeVar,
    get_args,
    get_origin,
)

from ..exceptions import (
    NotProvidedException,
)
from ..injections import (
    Inject,
)
from ..pools import (
    PoolException,
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
        database_key: Optional[tuple[str]] = None,
        operation_factory: Optional[GenericDatabaseOperationFactory] = None,
        operation_factory_cls: Optional[type[GenericDatabaseOperationFactory]] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs, pool_factory=pool_factory)

        if database_pool is None and pool_factory is not None:
            database_pool = self._get_pool_from_factory(pool_factory, database_key)

        if not isinstance(database_pool, DatabaseClientPool):
            raise NotProvidedException(f"A {DatabaseClientPool!r} instance is required. Obtained: {database_pool}")

        self._pool = database_pool

        if operation_factory is None:
            if operation_factory_cls is None:
                operation_factory_cls = self._get_generic_operation_factory()
            if operation_factory_cls is not None:
                operation_factory = self.database_client_cls.get_factory(operation_factory_cls)

        self._operation_factory = operation_factory

    @staticmethod
    def _get_pool_from_factory(pool_factory: PoolFactory, database_key: Optional[tuple[str]]):
        if database_key is None:
            database_key = tuple()

        for identifier in database_key:
            with suppress(PoolException):
                return pool_factory.get_pool(type_="database", identifier=identifier)

        return pool_factory.get_pool(type_="database")

    @property
    def database_operation_factory(self) -> Optional[GenericDatabaseOperationFactory]:
        """Get the operation factory if any.

        :return: A ``OperationFactory`` if it has been set or ``None`` otherwise.
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
            operation_factory_cls = args[0]
            if not isinstance(operation_factory_cls, type) or not issubclass(
                operation_factory_cls, DatabaseOperationFactory
            ):
                raise TypeError(f"{type(self)!r} must contain a {DatabaseOperationFactory!r} as generic value.")
        return operation_factory_cls

    async def execute_on_database_and_fetch_one(self, operation: DatabaseOperation) -> Any:
        """Submit an Operation and get the first response.

        :param operation: The operation to be executed.
        :return: This method does not return anything.
        """
        async with self.database_pool.acquire() as client:
            await client.execute(operation)
            return await client.fetch_one()

    # noinspection PyUnusedLocal
    async def execute_on_database_and_fetch_all(
        self, operation: DatabaseOperation, streaming_mode: Optional[bool] = None
    ) -> AsyncIterator[tuple]:
        """Submit an Operation and return an asynchronous iterator.

        :param operation: The operation to be executed.
        :param streaming_mode: If ``True`` return the values in streaming directly from the database (keep an open
            database connection), otherwise preloads the full set of values on memory and then retrieves them.
        :return: This method does not return anything.
        """
        if streaming_mode is None:
            streaming_mode = False

        async with self.database_pool.acquire() as client:
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
    async def execute_on_database(self, operation: DatabaseOperation) -> None:
        """Submit an Operation.

        :param operation: The operation to be executed.
        :return: This method does not return anything.
        """
        async with self.database_pool.acquire() as client:
            return await client.execute(operation)

    @property
    def database_client_cls(self) -> type[DatabaseClient]:
        """Get the client's class.

        :return: A ``type`` instance that is subclass of ``DatabaseClient``.
        """
        return self.database_pool.client_cls

    @property
    def database_pool(self) -> DatabaseClientPool:
        """Get the database pool.

        :return: A ``DatabaseClientPool`` object.
        """
        return self._pool
