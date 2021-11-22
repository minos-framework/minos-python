from __future__ import (
    annotations,
)

import sys
import unittest
from datetime import (
    timedelta,
)
from pathlib import (
    Path,
)
from typing import (
    Optional,
)
from unittest import (
    TestCase,
)
from uuid import (
    UUID,
    uuid4,
)

from cached_property import (
    cached_property,
)
from dependency_injector import (
    containers,
    providers,
)

from minos.aggregate import (
    Aggregate,
    Entity,
    EntitySet,
    EventEntry,
    InMemoryEventRepository,
    InMemorySnapshotRepository,
    InMemoryTransactionRepository,
    ModelRef,
    ValueObject,
    ValueObjectSet,
)
from minos.common import (
    Lock,
    MinosPool,
    MinosSetup,
    current_datetime,
)
from minos.networks import (
    Request,
)

BASE_PATH = Path(__file__).parent


class MinosTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.broker_publisher = FakeBroker()
        self.broker_pool = FakeBrokerPool()
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

        self.container = containers.DynamicContainer()
        self.container.broker_publisher = providers.Object(self.broker_publisher)
        self.container.broker_pool = providers.Object(self.broker_pool)
        self.container.transaction_repository = providers.Object(self.transaction_repository)
        self.container.lock_pool = providers.Object(self.lock_pool)
        self.container.event_repository = providers.Object(self.event_repository)
        self.container.snapshot_repository = providers.Object(self.snapshot_repository)
        self.container.wire(
            modules=[sys.modules["minos.aggregate"], sys.modules["minos.networks"], sys.modules["minos.common"]]
        )

    async def asyncSetUp(self):
        await super().asyncSetUp()

        await self.broker_publisher.setup()
        await self.transaction_repository.setup()
        await self.lock_pool.setup()
        await self.event_repository.setup()
        await self.snapshot_repository.setup()

    async def asyncTearDown(self):
        await self.snapshot_repository.destroy()
        await self.event_repository.destroy()
        await self.lock_pool.destroy()
        await self.transaction_repository.destroy()
        await self.broker_publisher.destroy()

        await super().asyncTearDown()

    def tearDown(self) -> None:
        self.container.unwire()
        super().tearDown()


class TestRepositorySelect(unittest.IsolatedAsyncioTestCase):
    def assert_equal_repository_entries(self: TestCase, expected: list[EventEntry], observed: list[EventEntry]) -> None:
        """For testing purposes."""

        self.assertEqual(len(expected), len(observed))

        for e, o in zip(expected, observed):
            self.assertEqual(type(e), type(o))
            self.assertEqual(e.aggregate_uuid, o.aggregate_uuid)
            self.assertEqual(e.aggregate_name, o.aggregate_name)
            self.assertEqual(e.version, o.version)
            self.assertEqual(e.data, o.data)
            self.assertEqual(e.id, o.id)
            self.assertEqual(e.action, o.action)
            self.assertAlmostEqual(e.created_at or current_datetime(), o.created_at, delta=timedelta(seconds=5))


class FakeBroker(MinosSetup):
    """For testing purposes."""

    async def send(self, *args, **kwargs) -> None:
        """For testing purposes."""

    async def get_one(self, *args, **kwargs):
        """For testing purposes."""

    async def get_many(self, *args, **kwargs):
        """For testing purposes."""


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


class FakeLockPool(MinosPool):
    """For testing purposes."""

    async def _create_instance(self):
        return FakeLock()

    async def _destroy_instance(self, instance) -> None:
        """For testing purposes."""


class FakeBrokerPool(MinosPool):
    """For testing purposes."""

    async def _create_instance(self):
        return FakeBroker()

    async def _destroy_instance(self, instance) -> None:
        """For testing purposes."""


class FakeRequest(Request):
    """For testing purposes"""

    def __init__(self, content):
        super().__init__()
        self._content = content

    @cached_property
    def user(self) -> Optional[UUID]:
        """For testing purposes"""
        return uuid4()

    async def content(self, **kwargs):
        """For testing purposes"""
        return self._content

    def __eq__(self, other) -> bool:
        return isinstance(other, type(self)) and self._content == other._content

    def __repr__(self) -> str:
        return f"FakeRequest({self._content!r})"


class Owner(Aggregate):
    """Aggregate ``Owner`` class for testing purposes."""

    name: str
    surname: str
    age: Optional[int]


class Car(Aggregate):
    """Aggregate ``Car`` class for testing purposes."""

    doors: int
    color: str
    owner: Optional[list[ModelRef[Owner]]]


class Order(Aggregate):
    """For testing purposes"""

    products: EntitySet[OrderItem]
    reviews: ValueObjectSet[Review]


class OrderItem(Entity):
    """For testing purposes"""

    amount: int


class Review(ValueObject):
    """For testing purposes."""

    message: str
