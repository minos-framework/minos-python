from pathlib import (
    Path,
)

from minos.aggregate import (
    InMemoryDeltaRepository,
    InMemorySnapshotRepository,
)
from minos.common import (
    DatabaseClientPool,
    Lock,
    LockPool,
    PoolFactory,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
)
from minos.networks import (
    BrokerClientPool,
    InMemoryBrokerPublisher,
    InMemoryBrokerSubscriberBuilder,
)
from minos.transactions import (
    InMemoryTransactionRepository,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"


class AiopgTestCase(DatabaseMinosTestCase):
    def get_config_file_path(self) -> Path:
        return CONFIG_FILE_PATH

    def get_injections(self):
        pool_factory = PoolFactory.from_config(
            self.config,
            default_classes={
                "broker": BrokerClientPool,
                "lock": FakeLockPool,
                "database": DatabaseClientPool,
            },
        )
        broker_publisher = InMemoryBrokerPublisher()
        broker_subscriber_builder = InMemoryBrokerSubscriberBuilder()
        transaction_repository = InMemoryTransactionRepository(
            lock_pool=pool_factory.get_pool("lock"),
        )
        delta_repository = InMemoryDeltaRepository(
            broker_publisher=broker_publisher,
            transaction_repository=transaction_repository,
            lock_pool=pool_factory.get_pool("lock"),
        )
        snapshot_repository = InMemorySnapshotRepository(
            delta_repository=delta_repository,
            transaction_repository=transaction_repository,
        )
        return [
            pool_factory,
            broker_publisher,
            broker_subscriber_builder,
            transaction_repository,
            delta_repository,
            snapshot_repository,
        ]


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
