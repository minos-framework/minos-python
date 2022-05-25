from __future__ import (
    annotations,
)

from abc import (
    ABC,
)
from pathlib import (
    Path,
)
from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    Entity,
    EntitySet,
    InMemoryDeltaRepository,
    InMemorySnapshotRepository,
    Ref,
    ValueObject,
    ValueObjectSet,
    testing,
)
from minos.common import (
    DatabaseClientPool,
    Lock,
    LockPool,
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
from minos.saga import (
    SagaManager,
)
from minos.saga import testing as saga_testing
from minos.transactions import (
    InMemoryTransactionRepository,
)
from minos.transactions import testing as transactions_testing

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"


class AggregateTestCase(MinosTestCase, ABC):
    testing_module = {
        "aggregate": testing,
        "transactions": transactions_testing,
        "saga": saga_testing,
    }

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
        delta_repository = InMemoryDeltaRepository(
            broker_publisher=broker_publisher,
            transaction_repository=transaction_repository,
            lock_pool=pool_factory.get_pool("lock"),
        )
        snapshot_repository = InMemorySnapshotRepository(
            delta_repository=delta_repository, transaction_repository=transaction_repository
        )
        saga_manager = SagaManager.from_config(
            self.config, database_pool=pool_factory.get_pool("database"), broker_pool=pool_factory.get_pool("broker")
        )
        return [
            pool_factory,
            broker_publisher,
            broker_subscriber_builder,
            transaction_repository,
            delta_repository,
            snapshot_repository,
            saga_manager,
        ]


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


class Owner(Entity):
    """For testing purposes"""

    name: str
    surname: str
    age: Optional[int]


class Car(Entity):
    """For testing purposes"""

    doors: int
    color: str
    owner: Optional[Ref[Owner]]


class Order(Entity):
    """For testing purposes"""

    products: EntitySet[OrderItem]
    reviews: ValueObjectSet[Review]


class OrderItem(Entity):
    """For testing purposes"""

    name: str


class Review(ValueObject):
    """For testing purposes."""

    message: str


class OrderAggregate(Aggregate[Order]):
    """For testing purposes."""

    async def create_order(self) -> UUID:
        """For testing purposes."""

        order, _ = await self.repository.create(Order, products=EntitySet(), reviews=ValueObjectSet())
        return order.uuid
