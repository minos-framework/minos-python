from pathlib import (
    Path,
)

from minos.aggregate import (
    InMemoryDeltaRepository,
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
)
from minos.transactions import (
    InMemoryTransactionRepository,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"


class SqlAlchemyTestCase(DatabaseMinosTestCase):
    """For testing purposes."""

    def get_config_file_path(self) -> Path:
        """For testing purposes."""
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
        transaction_repository = InMemoryTransactionRepository(
            pool_factory=pool_factory,
        )
        delta_repository = InMemoryDeltaRepository(
            transaction_repository=transaction_repository,
            pool_factory=pool_factory,
        )
        return [
            pool_factory,
            transaction_repository,
            delta_repository,
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
