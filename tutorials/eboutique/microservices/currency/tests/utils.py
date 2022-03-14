from __future__ import (
    annotations,
)

from pathlib import (
    Path,
)
from uuid import (
    UUID,
)

from minos.aggregate import (
    InMemoryEventRepository,
    InMemorySnapshotRepository,
    InMemoryTransactionRepository,
)
from minos.common import (
    DependencyInjector,
    Lock,
    MinosConfig,
    MinosPool,
    MinosSetup,
)
from minos.saga import (
    SagaContext,
    SagaStatus,
)


class _FakeBroker(MinosSetup):
    """For testing purposes."""

    async def send(self, *args, **kwargs) -> None:
        """For testing purposes."""


class _FakeSagaManager(MinosSetup):
    """For testing purposes."""

    async def run(self, *args, **kwargs) -> UUID:
        """For testing purposes."""


class _FakeSagaExecution:
    def __init__(self, context: SagaContext, status: SagaStatus = SagaStatus.Finished):
        self.context = context
        self.status = status


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


def build_dependency_injector() -> DependencyInjector:
    """For testing purposes"""

    return DependencyInjector(
        build_config(),
        saga_manager=_FakeSagaManager,
        broker_publisher=_FakeBroker,
        lock_pool=FakeLockPool,
        transaction_repository=InMemoryTransactionRepository,
        event_repository=InMemoryEventRepository,
        snapshot_repository=InMemorySnapshotRepository,
    )


def build_config() -> MinosConfig:
    """For testing purposes"""

    return MinosConfig(DEFAULT_CONFIG_FILE_PATH)


DEFAULT_CONFIG_FILE_PATH = Path(__file__).parents[1] / "config.yml"
