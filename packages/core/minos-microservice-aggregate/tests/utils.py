from __future__ import (
    annotations,
)

import unittest
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
    ExternalEntity,
    InMemoryEventRepository,
    InMemorySnapshotRepository,
    InMemoryTransactionRepository,
    Ref,
    RootEntity,
    ValueObject,
    ValueObjectSet,
)
from minos.common import (
    DependencyInjector,
    Lock,
    LockPool,
)
from minos.networks import (
    BrokerClientPool,
    InMemoryBrokerPublisher,
    InMemoryBrokerSubscriberBuilder,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"


class MinosTestCase(unittest.IsolatedAsyncioTestCase, ABC):
    def setUp(self) -> None:
        super().setUp()
        self.broker_pool = BrokerClientPool.from_config(CONFIG_FILE_PATH)
        self.broker_publisher = InMemoryBrokerPublisher()
        self.broker_subscriber_builder = InMemoryBrokerSubscriberBuilder()
        self.lock_pool = FakeLockPool()
        self.transaction_repository = InMemoryTransactionRepository(lock_pool=self.lock_pool)
        self.event_repository = InMemoryEventRepository(
            broker_publisher=self.broker_publisher,
            transaction_repository=self.transaction_repository,
            lock_pool=self.lock_pool,
        )
        self.snapshot_repository = InMemorySnapshotRepository(
            event_repository=self.event_repository, transaction_repository=self.transaction_repository
        )

        self.injector = DependencyInjector(
            None,
            [
                self.broker_pool,
                self.broker_publisher,
                self.broker_subscriber_builder,
                self.lock_pool,
                self.transaction_repository,
                self.event_repository,
                self.snapshot_repository,
            ],
        )
        self.injector.wire()

    async def asyncSetUp(self):
        await super().asyncSetUp()

        await self.injector.setup()

    async def asyncTearDown(self):
        await self.injector.destroy()

        await super().asyncTearDown()

    def tearDown(self) -> None:
        self.injector.unwire()
        super().tearDown()


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

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return


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
