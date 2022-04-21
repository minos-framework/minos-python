from __future__ import (
    annotations,
)

from abc import (
    ABC,
)
from pathlib import (
    Path,
)
from typing import Union

from minos.common import (
    Lock,
    LockPool, DatabaseClientPool, PoolFactory, InjectableMixin,
)
from minos.common.testing import (
    MinosTestCase,
)
from minos.networks import BrokerClientPool
from minos.transactions import InMemoryTransactionRepository

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"


class TransactionsTestCase(MinosTestCase, ABC):
    def get_config_file_path(self):
        return CONFIG_FILE_PATH

    def get_injections(self) -> list[Union[InjectableMixin, type[InjectableMixin], str]]:
        pool_factory = PoolFactory.from_config(
            self.config,
            default_classes={"broker": BrokerClientPool, "lock": FakeLockPool, "database": DatabaseClientPool},
        )
        transaction_repository = InMemoryTransactionRepository(lock_pool=pool_factory.get_pool("lock"))

        return [
            pool_factory,
            transaction_repository,
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
