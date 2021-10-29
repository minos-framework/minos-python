import sys
import unittest
from datetime import (
    timedelta,
)
from pathlib import (
    Path,
)
from typing import (
    Any,
    AsyncIterator,
    Optional,
)
from unittest import (
    TestCase,
)
from uuid import (
    UUID,
)

from dependency_injector import (
    containers,
    providers,
)

from minos.common import (
    Aggregate,
    CommandReply,
    Entity,
    InMemoryRepository,
    InMemorySnapshot,
    InMemoryTransactionRepository,
    Lock,
    MinosBroker,
    MinosHandler,
    MinosPool,
    MinosRepository,
    MinosSagaManager,
    MinosSnapshot,
    RepositoryEntry,
    TransactionEntry,
    TransactionRepository,
    current_datetime,
)

BASE_PATH = Path(__file__).parent


class MinosTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.event_broker = FakeBroker()
        self.lock_pool = FakeLockPool()
        self.transaction_repository = InMemoryTransactionRepository(lock_pool=self.lock_pool)
        self.repository = InMemoryRepository(
            event_broker=self.event_broker, transaction_repository=self.transaction_repository, lock_pool=self.lock_pool
        )
        self.snapshot = InMemorySnapshot(repository=self.repository, transaction_repository=self.transaction_repository)

        self.container = containers.DynamicContainer()
        self.container.event_broker = providers.Object(self.event_broker)
        self.container.transaction_repository = providers.Object(self.transaction_repository)
        self.container.lock_pool = providers.Object(self.lock_pool)
        self.container.repository = providers.Object(self.repository)
        self.container.snapshot = providers.Object(self.snapshot)
        self.container.wire(modules=[sys.modules[__name__]])

    async def asyncSetUp(self):
        await super().asyncSetUp()

        await self.event_broker.setup()
        await self.transaction_repository.setup()
        await self.lock_pool.setup()
        await self.repository.setup()
        await self.snapshot.setup()

    async def asyncTearDown(self):
        await self.snapshot.destroy()
        await self.repository.destroy()
        await self.lock_pool.destroy()
        await self.transaction_repository.destroy()
        await self.event_broker.destroy()

        await super().asyncTearDown()

    def tearDown(self) -> None:
        self.container.unwire()
        super().tearDown()


class TestRepositorySelect(unittest.IsolatedAsyncioTestCase):
    def assert_equal_repository_entries(
        self: TestCase, expected: list[RepositoryEntry], observed: list[RepositoryEntry]
    ) -> None:
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


class FakeRepository(MinosRepository):
    """For testing purposes."""

    async def _submit(self, entry: RepositoryEntry, **kwargs) -> RepositoryEntry:
        """For testing purposes."""

    def _select(self, *args, **kwargs) -> AsyncIterator[RepositoryEntry]:
        """For testing purposes."""

    @property
    async def _offset(self) -> int:
        """For testing purposes."""
        return 0


class FakeBroker(MinosBroker):
    """For testing purposes."""

    def __init__(self, **kwargs):
        super().__init__()
        self.call_count = 0
        self.calls_kwargs = list()

    async def send(self, data: Any, **kwargs) -> None:
        """For testing purposes."""
        self.call_count += 1
        self.calls_kwargs.append({"data": data} | kwargs)

    @property
    def call_kwargs(self) -> Optional[dict[str, Any]]:
        """For testing purposes."""
        if len(self.calls_kwargs) == 0:
            return None
        return self.calls_kwargs[-1]

    def reset_mock(self):
        self.call_count = 0
        self.calls_kwargs = list()


class FakeHandler(MinosHandler):
    """For testing purposes."""

    async def get_one(self, *args, **kwargs) -> Any:
        """For testing purposes."""

    async def get_many(self, *args, **kwargs) -> list[Any]:
        """For testing purposes."""


class FakeSagaManager(MinosSagaManager):
    """For testing purposes."""

    async def _run_new(self, name: str, **kwargs) -> UUID:
        """For testing purposes."""

    async def _load_and_run(self, reply: CommandReply, **kwargs) -> UUID:
        """For testing purposes."""


class FakeEntrypoint:
    """For testing purposes."""

    def __init__(self, *args, **kwargs):
        """For testing purposes."""

    async def __aenter__(self):
        """For testing purposes."""

    async def graceful_shutdown(*args, **kwargs):
        """For testing purposes."""


class FakeLoop:
    """For testing purposes."""

    def __init__(self):
        """For testing purposes."""

    def run_forever(self):
        """For testing purposes."""

    def run_until_complete(self, *args, **kwargs):
        """For testing purposes."""


class FakeSnapshot(MinosSnapshot):
    """For testing purposes."""

    async def _get(self, *args, **kwargs) -> Aggregate:
        """For testing purposes."""

    def _find(self, *args, **kwargs) -> AsyncIterator[Aggregate]:
        """For testing purposes."""

    async def _synchronize(self, **kwargs) -> None:
        """For testing purposes."""


class FakeEntity(Entity):
    """For testing purposes."""

    name: str


class FakeTransactionRepository(TransactionRepository):
    """For testing purposes."""

    async def _submit(self, transaction: TransactionEntry) -> None:
        """For testing purposes."""

    def _select(self, **kwargs) -> AsyncIterator[TransactionEntry]:
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
