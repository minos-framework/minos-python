from __future__ import (
    annotations,
)

from abc import (
    ABC,
)
from collections.abc import (
    AsyncIterator,
    Iterable,
)
from datetime import (
    datetime,
)
from pathlib import (
    Path,
)
from typing import (
    Any,
    Optional,
)
from uuid import (
    UUID,
)

from minos.aggregate import (
    Action,
    Aggregate,
    Entity,
    EntitySet,
    EventDatabaseOperationFactory,
    ExternalEntity,
    InMemoryEventRepository,
    InMemorySnapshotRepository,
    InMemoryTransactionRepository,
    Ref,
    RootEntity,
    SnapshotDatabaseOperationFactory,
    TransactionDatabaseOperationFactory,
    TransactionStatus,
    ValueObject,
    ValueObjectSet,
)
from minos.aggregate.queries import (
    _Condition,
    _Ordering,
)
from minos.common import (
    DatabaseClient,
    DatabaseClientPool,
    DatabaseOperation,
    Lock,
    LockDatabaseOperationFactory,
    LockPool,
    ManagementDatabaseOperationFactory,
    PoolFactory,
)
from minos.common.testing import (
    MinosTestCase,
)
from minos.networks import (
    BrokerClientPool,
    InMemoryBrokerPublisher,
    InMemoryBrokerSubscriberBuilder,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"


class AggregateTestCase(MinosTestCase, ABC):
    def get_config_file_path(self):
        return CONFIG_FILE_PATH

    def get_injections(self):
        pool_factory = PoolFactory.from_config(
            self.config,
            default_classes={"broker": BrokerClientPool, "lock": FakeLockPool, "database": DatabaseClientPool},
        )
        broker_publisher = InMemoryBrokerPublisher()
        broker_subscriber_builder = InMemoryBrokerSubscriberBuilder()
        transaction_repository = InMemoryTransactionRepository(lock_pool=pool_factory.get_pool("lock"))
        event_repository = InMemoryEventRepository(
            broker_publisher=broker_publisher,
            transaction_repository=transaction_repository,
            lock_pool=pool_factory.get_pool("lock"),
        )
        snapshot_repository = InMemorySnapshotRepository(
            event_repository=event_repository, transaction_repository=transaction_repository
        )
        return [
            pool_factory,
            broker_publisher,
            broker_subscriber_builder,
            transaction_repository,
            event_repository,
            snapshot_repository,
        ]


class FakeDatabaseClient(DatabaseClient):
    """For testing purposes"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kwargs = kwargs
        self._response = tuple()

    async def _is_valid(self, **kwargs) -> bool:
        """For testing purposes"""
        return True

    async def _reset(self, **kwargs) -> None:
        """For testing purposes"""
        self._response = tuple()

    async def _execute(self, operation: FakeDatabaseOperation) -> None:
        """For testing purposes"""
        self._response = operation.response

    async def _fetch_all(self, *args, **kwargs) -> AsyncIterator[Any]:
        """For testing purposes"""
        for value in self._response:
            yield value


class FakeDatabaseOperation(DatabaseOperation):
    """For testing purposes"""

    def __init__(self, content: str, response: Optional[Iterable[Any]] = tuple(), *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.content = content
        self.response = tuple(response)


class FakeLockDatabaseOperationFactory(LockDatabaseOperationFactory):
    """For testing purposes"""

    def build_acquire(self, hashed_key: int) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("acquire")

    def build_release(self, hashed_key: int) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("release")


FakeDatabaseClient.register_factory(LockDatabaseOperationFactory, FakeLockDatabaseOperationFactory)


class FakeManagementDatabaseOperationFactory(ManagementDatabaseOperationFactory):
    """For testing purposes"""

    def build_create(self, database: str) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("create")

    def build_delete(self, database: str) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("delete")


FakeDatabaseClient.register_factory(ManagementDatabaseOperationFactory, FakeManagementDatabaseOperationFactory)


class FakeTransactionDatabaseOperationFactory(TransactionDatabaseOperationFactory):
    """For testing purposes."""

    def build_create(self) -> DatabaseOperation:
        """For testing purposes."""
        return FakeDatabaseOperation("create")

    def build_submit(
        self, uuid: UUID, destination_uuid: UUID, status: TransactionStatus, event_offset: int, **kwargs
    ) -> DatabaseOperation:
        """For testing purposes."""
        return FakeDatabaseOperation("submit")

    def build_query(
        self,
        uuid: Optional[UUID] = None,
        uuid_ne: Optional[UUID] = None,
        uuid_in: Optional[tuple[UUID]] = None,
        destination_uuid: Optional[UUID] = None,
        status: Optional[str] = None,
        status_in: Optional[tuple[str]] = None,
        event_offset: Optional[int] = None,
        event_offset_lt: Optional[int] = None,
        event_offset_gt: Optional[int] = None,
        event_offset_le: Optional[int] = None,
        event_offset_ge: Optional[int] = None,
        updated_at: Optional[datetime] = None,
        updated_at_lt: Optional[datetime] = None,
        updated_at_gt: Optional[datetime] = None,
        updated_at_le: Optional[datetime] = None,
        updated_at_ge: Optional[datetime] = None,
        **kwargs,
    ) -> DatabaseOperation:
        """For testing purposes."""
        return FakeDatabaseOperation("select")


FakeDatabaseClient.register_factory(TransactionDatabaseOperationFactory, FakeTransactionDatabaseOperationFactory)


class FakeEventDatabaseOperationFactory(EventDatabaseOperationFactory):
    """For testing purposes."""

    def build_create(self) -> DatabaseOperation:
        """For testing purposes."""
        return FakeDatabaseOperation("create")

    def build_submit(
        self,
        transaction_uuids: tuple[UUID],
        uuid: UUID,
        action: Action,
        name: str,
        version: int,
        data: bytes,
        created_at: datetime,
        transaction_uuid: UUID,
        lock: Optional[int],
        **kwargs,
    ) -> DatabaseOperation:
        """For testing purposes."""
        return FakeDatabaseOperation("submit")

    def build_query(
        self,
        uuid: Optional[UUID] = None,
        name: Optional[str] = None,
        version: Optional[int] = None,
        version_lt: Optional[int] = None,
        version_gt: Optional[int] = None,
        version_le: Optional[int] = None,
        version_ge: Optional[int] = None,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_le: Optional[int] = None,
        id_ge: Optional[int] = None,
        transaction_uuid: Optional[UUID] = None,
        transaction_uuid_ne: Optional[UUID] = None,
        transaction_uuid_in: Optional[tuple[UUID, ...]] = None,
        **kwargs,
    ) -> DatabaseOperation:
        """For testing purposes."""
        return FakeDatabaseOperation("select_rows")

    def build_query_offset(self) -> DatabaseOperation:
        """For testing purposes."""
        return FakeDatabaseOperation("select_max_id")


FakeDatabaseClient.register_factory(EventDatabaseOperationFactory, FakeEventDatabaseOperationFactory)


class FakeSnapshotDatabaseOperationFactory(SnapshotDatabaseOperationFactory):
    """For testing purposes."""

    def build_create(self) -> DatabaseOperation:
        """For testing purposes."""
        return FakeDatabaseOperation("create")

    def build_delete(self, transaction_uuids: Iterable[UUID]) -> DatabaseOperation:
        """For testing purposes."""
        return FakeDatabaseOperation("delete")

    def build_submit(
        self,
        uuid: UUID,
        name: str,
        version: int,
        schema: bytes,
        data: dict[str, Any],
        created_at: datetime,
        updated_at: datetime,
        transaction_uuid: UUID,
    ) -> DatabaseOperation:
        """For testing purposes."""
        return FakeDatabaseOperation("insert")

    def build_query(
        self,
        name: str,
        condition: _Condition,
        ordering: Optional[_Ordering],
        limit: Optional[int],
        transaction_uuids: tuple[UUID, ...],
        exclude_deleted: bool,
    ) -> DatabaseOperation:
        """For testing purposes."""
        return FakeDatabaseOperation("query")

    def build_submit_offset(self, value: int) -> DatabaseOperation:
        """For testing purposes."""
        return FakeDatabaseOperation("store_offset")

    def build_query_offset(self) -> DatabaseOperation:
        """For testing purposes."""
        return FakeDatabaseOperation("get_offset")


FakeDatabaseClient.register_factory(SnapshotDatabaseOperationFactory, FakeSnapshotDatabaseOperationFactory)


class FakeAsyncIterator:
    """For testing purposes."""

    def __init__(self, seq):
        self.iter = iter(seq)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration


class FakeLock(Lock):
    """For testing purposes."""

    def __init__(self, key=None, *args, **kwargs):
        if key is None:
            key = "fake"
        super().__init__(key, *args, **kwargs)

    async def acquire(self) -> None:
        """For testing purposes."""

    async def release(self):
        """For testing purposes."""


class FakeLockPool(LockPool):
    """For testing purposes."""

    async def _create_instance(self):
        return FakeLock()

    async def _destroy_instance(self, instance) -> None:
        """For testing purposes."""


class Owner(RootEntity):
    """For testing purposes"""

    name: str
    surname: str
    age: Optional[int]


class Car(RootEntity):
    """For testing purposes"""

    doors: int
    color: str
    owner: Optional[Ref[Owner]]


class Order(RootEntity):
    """For testing purposes"""

    products: EntitySet[OrderItem]
    reviews: ValueObjectSet[Review]


class OrderItem(Entity):
    """For testing purposes"""

    name: str


class Review(ValueObject):
    """For testing purposes."""

    message: str


class Product(ExternalEntity):
    """For testing purposes."""

    title: str
    quantity: int


class OrderAggregate(Aggregate[Order]):
    """For testing purposes."""

    @staticmethod
    async def create_order() -> UUID:
        """For testing purposes."""

        order = await Order.create(products=EntitySet(), reviews=ValueObjectSet())
        return order.uuid
